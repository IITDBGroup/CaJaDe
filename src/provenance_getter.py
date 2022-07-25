from src.gprom_wrapper import *
from src.sg_generator import Schema_Graph_Generator
import sys
import psycopg2
import pandas as pd
import re
import logging
import math
from networkx import MultiGraph



logger = logging.getLogger(__name__)



rel_name = re.compile("PROV_(([^_]|[_][_])+)", flags=re.IGNORECASE)

re_PROV = re.compile("PROV", flags=re.IGNORECASE)
re_CAT = re.compile("cat__")
# single_underscore = re.compile("?[_]")

# given a query return a df of the provenance for 
# now and the relations accessed by the query

class provenance_getter:
	
	def __init__(self, conn, gprom_wrapper, db_dict):
		self.conn = conn
		self.gprom_wrapper = gprom_wrapper
		self.cur = self.conn.cursor()
		self.db_dict = db_dict

	def get_all_keys(self):

	    keys_q = """
	    select distinct column_name from information_schema.key_column_usage;
	    """

	    self.cur.execute(keys_q)
	    keys = pd.read_sql(keys_q,self.conn)['column_name'].to_list()

	    return keys

	def create_original_pt(self, query):
		code, output = self.gprom_wrapper.runQuery(query)
		drop_pt_full = "DROP TABLE IF EXISTS pt_full CASCADE;"
		gen_pt_full_query = output.decode("utf-8")
		# logger.debug(f'gen_original_pt_query:\n {gen_pt_full_query}')
		pt_full_table = f"CREATE TABLE pt_full AS {gen_pt_full_query};"
		self.cur.execute(drop_pt_full)
		self.cur.execute(pt_full_table)
		self.conn.commit()


	def gen_provenance_table(self, user_questions, user_specified_attrs, sample_rate=1):

		# need to do a replacement here to categorized tables in query
		# but to get the tables accessed by query, first need to get original provenance

		keys = self.get_all_keys()

		rel_list = []
		
		prov_df = pd.read_sql("select * from pt_full", self.conn)

		for n in list(prov_df):
			result = rel_name.search(n)
			if(result):
				rel_list.append(result.group(1))

		rel_set = set(rel_list)
		rels = [n.replace('__','_') for n in rel_set]
		rels = list(rels)
		rels.sort()

		pt_attrs = []

		pt_remamed_attr = list(prov_df)
		pt_full_dict = dict.fromkeys(pt_remamed_attr, None)
		pt_dict = {'attributes':{}, 
		'keys':[],
		'user_text_attrs':[],
		'user_numerical_attrs':[]}

		for k,v in pt_full_dict.items():
			if(re_PROV.search(k)):
				str_items = re.split('(?<!_)_(?!_)', k)
				# logger.debug(str_items)
				if(len(str_items)==3):
					origin_rel = str_items[1]
					origin_rel = origin_rel.replace('__','_')
					origin_attr = str_items[2]
					origin_attr = origin_attr.replace('__','_')
				elif(len(str_items)==4):
					origin_rel = ''.join([str_items[1],str_items[2]])
					origin_rel = origin_rel.replace('__','_')
					origin_attr = str_items[3]
					origin_attr = origin_attr.replace('__','_')

				for kk, vv in self.db_dict.items():
					if(origin_rel==kk):
						for attr in vv['attributes']:
							if(origin_attr==attr[0]):
								pt_dict['attributes'][':'.join([k,attr[1]])] = (origin_rel, origin_attr)
								if((origin_rel, origin_attr) in user_specified_attrs):
									if(attr[1]=='nominal'):
										pt_dict['user_text_attrs'].append(k)
									else:
										pt_dict['user_numerical_attrs'].append(k)		
								if(origin_attr in self.db_dict[origin_rel]['keys'] or 
									origin_attr in self.db_dict[origin_rel]['p_key']):
									pt_dict['keys'].append(k)
				# logger.debug((origin_rel, origin_attr))
						
			else:
				pt_dict['attributes'][':'.join([k,'nominal'])] = ('PT', k)

			pt_attrs.append(f'"{k}"')

		# logger.debug(f'pt_dict:{pt_dict}')
		pt_dict['attributes']['pnumber:nominal'] = ('PT','pnumber')
		pt_dict['attributes']['is_user:nominal'] = ('PT','is_user')

		# logger.debug(pt_attrs)
		drop_PT_view_query = "DROP TABLE IF EXISTS pt CASCADE;"

		user_prov_part = "ROW_NUMBER() OVER () AS pnumber, 'yes' as is_user"
		non_user_prov_part = "ROW_NUMBER() OVER () AS pnumber, 'no' as is_user"

		add_col = "ROW_NUMBER() OVER (PARTITION BY season_name, month) AS pnumber, DENSE_RANK() OVER (ORDER BY season_name, month) as is_user"

		logger.debug(f'ATTRIBUTES:{pt_attrs}')

		user_questions = []    # added so we can test 0-point question
		
		if(len(user_questions)==1):
			APT_view = f"""
			CREATE TABLE pt AS 
			(
				(
				SELECT {','.join(pt_attrs)}, {user_prov_part} FROM pt_full
				WHERE {user_questions[0]}
				)
				UNION ALL
				(
				SELECT {','.join(pt_attrs)}, {non_user_prov_part} FROM pt_full
				WHERE NOT {user_questions[0]}
				)
			)
			"""
		elif (len(user_questions) == 2):
			APT_view = f"""
			CREATE TABLE pt AS 
			(
				(
				SELECT {','.join(pt_attrs)}, {user_prov_part} FROM pt_full
				WHERE {user_questions[0]}
				)
				UNION ALL
				(
				SELECT {','.join(pt_attrs)}, {non_user_prov_part} FROM pt_full
				WHERE {user_questions[1]}
				)
			)
			"""
		else:
			APT_view = f"""
			CREATE TABLE pt AS 
			(
				SELECT {','.join(pt_attrs)}, {add_col} FROM pt_full
			)
			"""

		q_pt_size = "SELECT COUNT(*) FROM pt;"

		# logger.debug(APT_view)
		self.cur.execute(drop_PT_view_query)
		self.cur.execute(APT_view)
		self.conn.commit()

		self.cur.execute(q_pt_size)
		user_pt_size = int(self.cur.fetchone()[0])

		return user_pt_size, pt_dict, rels

