# from graph import Graph
import networkx as nx
from networkx.drawing.nx_agraph import to_agraph 
import psycopg2
import pandas as pd
import logging
import time
import numpy as np
"""
Given a database connection info, 
generate the join graph from it in order
to be used to generate join trees using 
tree generator
"""

logger = logging.getLogger(__name__)

class Schema_Graph_Generator:

	def __init__(self,conn):
		self.conn = conn
		self.cur = self.conn.cursor()


	def generate_edge_info(self):

		q_get_constraint_view = """
		CREATE OR REPLACE VIEW con_info as
		SELECT f.table as f_table, p.table as p_table, f.col as f_key, p.col as p_key
		FROM
		(SELECT
		(SELECT r.relname FROM pg_class r WHERE r.oid = c.conrelid) AS table, 
		  (SELECT array_to_string(array_agg(attname order by attname),',') FROM pg_attribute 
		   WHERE attrelid = c.conrelid and ARRAY[attnum] <@ c.conkey) AS col, 
		  (SELECT r.relname FROM pg_class r WHERE r.oid = c.confrelid) AS ref 
		FROM pg_constraint c WHERE (SELECT r.relname FROM pg_class r WHERE r.oid = c.conrelid) IS NOT NULL
		AND (SELECT r.relname FROM pg_class r WHERE r.oid = c.confrelid) IS NOT NULL
		) as f,
		(
		SELECT
		(SELECT r.relname FROM pg_class r WHERE r.oid = c.conrelid) AS table, 
		  (SELECT array_to_string(array_agg(attname order by attname),',') FROM pg_attribute 
		   WHERE attrelid = c.conrelid and ARRAY[attnum] <@ c.conkey) AS col, 
		  (SELECT r.relname FROM pg_class r WHERE r.oid = c.confrelid) AS ref 
		FROM pg_constraint c WHERE (SELECT r.relname FROM pg_class r WHERE r.oid = c.conrelid) IS NOT NULL
		AND (SELECT r.relname FROM pg_class r WHERE r.oid = c.confrelid) IS NULL
		) as p
		WHERE f.ref = p.table;
		"""

		q_get_all_keys = """
		CREATE OR REPLACE VIEW rel_keys AS
		SELECT distinct p_table as rel, string_agg(distinct p_key, ', ' ORDER BY p_key) as keys from con_info group by p_table
		UNION 
		SELECT distinct f_table as rel, string_agg(distinct f_key, ', ' ORDER BY f_key) as keys from con_info group by f_table
		"""


		self.cur.execute(q_get_constraint_view)
		self.conn.commit()


		self.cur.execute(q_get_all_keys)
		self.conn.commit()


		conf_query = """
		SELECT * FROM con_info;
		"""
		con_info = pd.read_sql(conf_query, self.conn)

		con_info['condition_str'] = con_info.apply(
			lambda row : "({})=({})".format(
				sorted([
			",".join((row["f_table"]+".") + x for x in row['f_key'].split(',')), 
			",".join((row["p_table"]+".") + x for x in row['p_key'].split(','))
				])[0],
				sorted([
			",".join((row["f_table"]+".") + x for x in row['f_key'].split(',')), 
			",".join((row["p_table"]+".") + x for x in row['p_key'].split(','))
				])[1]
		),
			axis=1
		)

		con_info['key_dict'] = con_info.apply(
			lambda row: {row['f_table']:list(row['f_key'].split(',')), row['p_table']:row['p_key'].split(',')},
			axis=1
			)
		# sort it so that we may compare this with user query where clause to find
		# out if a condition has been specified in query

		return con_info


	def get_tables(self):

		all_tables_q = """
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema='public'
		AND table_type='BASE TABLE'
		AND table_name NOT LIKE '%_categorized%';
		"""

		tables_list = pd.read_sql(all_tables_q,self.conn)['table_name'].to_list()

		return tables_list


	def get_attr_dict(self):

		q_attr_viw = """
		CREATE OR REPLACE VIEW table_attr AS 
		SELECT ic.table_name, string_agg(ic.column_name::character varying || ':' || ic.data_type::character varying, ',') AS attributes 
		FROM information_schema.columns ic, information_schema.tables it
		WHERE ic.table_name =  it.table_name 
		AND ic.table_schema='public' 
		AND it.table_type = 'BASE TABLE'
		GROUP BY ic.table_name;

		"""

		self.cur.execute(q_attr_viw)

		attr_query = """
		SELECT ta.table_name, ta.attributes, string_agg(rk.keys, ',') FROM table_attr ta, rel_keys rk
		WHERE  ta.table_name = rk.rel
		GROUP BY ta.table_name, ta.attributes
		"""

		attr_dict = pd.read_sql(attr_query, self.conn).set_index('table_name').T.to_dict('list')

		for k,v in attr_dict.items():
			attrs_list = [tuple(x.split(':')) for x in v[0].split(',')]
			attrs_list.sort(key = lambda x:x[0])
			keys_list = list(set(x.strip() for x in v[1].split(',')))
			keys_list.sort()

			attr_dict[k] = {'attributes': attrs_list,
			'keys': keys_list
			}

		pk_query = """
		SELECT
		(SELECT r.relname FROM pg_class r WHERE r.oid = c.conrelid) AS table, 
		(SELECT array_to_string(array_agg(attname order by attname),',') FROM pg_attribute WHERE attrelid = c.conrelid and ARRAY[attnum] <@ c.conkey) AS col
		FROM pg_constraint c WHERE (SELECT r.relname FROM pg_class r WHERE r.oid = c.conrelid) IS NOT NULL AND (SELECT r.relname FROM pg_class r WHERE r.oid = c.confrelid) IS NULL
		AND (SELECT r.relname FROM pg_class r WHERE r.oid = c.conrelid) in 
		(
		SELECT it.table_name  
		FROM information_schema.columns ic, information_schema.tables it
		WHERE ic.table_name =  it.table_name 
		AND ic.table_schema='public'
		AND it.table_type = 'BASE TABLE'
		)
		"""

		pk_dict = pd.read_sql(pk_query, self.conn).set_index('table').T.to_dict('list')
		# logger.debug(pk_dict)
		for k,v in pk_dict.items():
			attr_dict[k]['p_key'] = [x.strip() for x in v[0].split(',')]

		# logger.debug(attr_dict)

		return attr_dict




	def generate_graph(self, graph): 

		"""
		input: an empty graph
		output: the graph with the information provided by conn inserted, 
				and also a dictionary with keys as the relation label and values as 
				lists of the attributes names
		"""

		edge_info = self.generate_edge_info()

		table_names = self.get_tables()

		attr_dict = self.get_attr_dict()

		for t in table_names:
			graph.add_node(t)

		# logger.debug(edge_info)
		edge_info_dict = edge_info.to_dict(orient='records')



		for d in edge_info_dict:
			graph.add_edge(d['f_table'], d['p_table'], condition=d['condition_str'], key_dict=d['key_dict'], 
				p_table=d['p_table'],f_table=d['f_table'])


		return graph, attr_dict



