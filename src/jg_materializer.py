from sg_generator import Schema_Graph_Generator
from networkx import MultiGraph
import networkx as nx
import psycopg2
from provenance_getter import provenance_getter
from gprom_wrapper import  GProMWrapper
from jg_generator import Join_Graph_Generator
import logging 
from renaming import encode
import re
from instrumentation import ExecStats



logger = logging.getLogger(__name__)



pt_attr_re = re.compile("(?<=PT\.)(\w+)(?=[),=])")

class QueryGeneratorStats(ExecStats):
    """
    Statistics gathered during mining
    """

    TIMERS = {'renaming',
              'compose_query',
              'materialize_jg'
              }

class Join_Graph_Materializer:

    def __init__(self, conn, db_dict):
        """
        db_dict: a dictionary storing relation name and its attributes
        Note: db_dict['PT'] also needs to be added 
        """
        self.conn = conn
        self.db_dict = db_dict
        self.stats = QueryGeneratorStats()
        self.cur = self.conn.cursor()

    def init_cost_estimator(self):

        cost_estimation_fuction_q = """
        create or replace function COST_ESTIMATION (in qry text, out r jsonb) returns setof jsonb as $$ 
        declare  
        explcmd text;  
        begin  
        explcmd := ('EXPLAIN (FORMAT JSON) ' || qry);  
        for r in execute explcmd loop  
        return next;  
        end loop;  
        return;  
        end; $$ language plpgsql;
        """
        self.cur.execute(cost_estimation_fuction_q)


    def materialize_jg(self, join_graph, cost_estimate=False):

        renaming_dict = {}
        # dictionary storing the renamings from renaming.encode function

        # renaming nodes and their attributes
        self.stats.startTimer('renaming')
        for node in join_graph.graph_core:
            if(node.key==1):
                rename_input_dict = {"key": node.key,
                "label": "PT",
                "columns": list(self.db_dict["PT"]['attributes'])
                }
                renaming_dict = encode(rel=rename_input_dict, map_dict = renaming_dict, is_pt=True) 

            else:
                rename_input_dict = {"key": node.key,
                "label": node.label,
                "columns": list(self.db_dict[node.label]['attributes'])
                }
                renaming_dict = encode(rel=rename_input_dict, map_dict = renaming_dict, is_pt=False)
        self.stats.stopTimer('renaming')

        # logger.debug(renaming_dict)
        # logger.debug(self.db_dict)
        # now generate the query to output the augmented provenance table

        # first, we need to rename the join conditions 

        select_clause_tokens = []
        where_clause_tokens = []
        from_clause_tokens = []

        # logger.debug(len(self.condition_list))


        # if jg has only one node, then dont need to worry about join conditions, 
        # just need to take  care of ignored attributes
        self.stats.startTimer('compose_query')
        if(len(join_graph.graph_core)==1):
            if(self.db_dict['PT']['user_attrs']):
                for ua in self.db_dict['PT']['user_attrs']:
                    join_graph.ignored_attrs.append(renaming_dict[1]['columns'][ua])
            PT_key_attributes = self.db_dict['PT']['keys']

            for ia in PT_key_attributes:
                for k,v in renaming_dict.items():
                    if(k=='max_rel_index' or k=='max_attr_index' or k=='dtypes'):
                        continue
                    else:
                        if(v['label']=='PT'):
                            join_graph.ignored_attrs.append(v['columns'][ia])

        for node1, node2, cond in join_graph.graph_core.edges.data('condition'):
            # logger.debug(f"node1:{node1}, node2:{node2}, cond:{cond}")
            node1_renamed = renaming_dict[node1.key]['renamed_rel']
            # logger.debug(f'node1_renamed:{node1_renamed}')
            node2_renamed = renaming_dict[node2.key]['renamed_rel']
            # logger.debug(f'node2_renamed:{node2_renamed}')
            node1_original = renaming_dict[node1.key]['label']
            node2_original = renaming_dict[node2.key]['label']

            # if the condition contains the attributes from pt,
            # need to map the condition attribute name to the attribute name in pt
            renamed_attr_tuple_list = []

            if(node1_original=='PT' or node2_original=='PT'):
                pt_rel_name = cond[2]
                pt_rel_attr = pt_attr_re.findall(cond[0])

                for a in pt_rel_attr:
                    for k,v in self.db_dict['PT']['attributes'].items():
                        if(v==(pt_rel_name,a)):
                            renamed_tuple = (k.split(':')[0],a)
                            renamed_attr_tuple_list.append(renamed_tuple)
                            break
                        else:
                            continue

                for t in renamed_attr_tuple_list:
                    cond[0] = cond[0].replace('PT.{}'.format(t[1]),'PT."{}"'.format(t[0]))

                if(self.db_dict['PT']['user_attrs']):
                    for ua in self.db_dict['PT']['user_attrs']:
                        join_graph.ignored_attrs.append(renaming_dict[1]['columns'][ua])

            cond[0] = re.sub(r'\b{}[.]'.format(node1_original), 
                                               f'{node1_renamed}.', 
                                               cond[0])

            cond[0] = re.sub(r'\b{}[.]'.format(node2_original), 
                                               f'{node2_renamed}.', 
                                               cond[0])


            # node1_key_attributes = re.findall(r"\b{}[.]\"?(\w+)\"?".format(node1_renamed), cond[0])
            # ignore all the key columns from relations
            
            node1_key_attributes = self.db_dict[node1.label]['keys']
            # logger.debug(node1_key_attributes)
            # logger.debug(renaming_dict)
            # logger.debug(cond[0])
            for n1_a in node1_key_attributes:
                for k,v in renaming_dict.items():
                    if(k=='max_rel_index' or k=='max_attr_index' or k=='dtypes'):
                        continue
                    else:
                        if(v['renamed_rel']==node1_renamed):
                            join_graph.ignored_attrs.append(v['columns'][n1_a])

            # node2_key_attributes = re.findall(r"\b{}[.](\w+)".format(node2_renamed), cond[0])
            node2_key_attributes = self.db_dict[node2.label]['keys']
            # logger.debug(node2_key_attributes)

            for n2_a in node2_key_attributes:
                for k,v in renaming_dict.items():
                    if(k=='max_rel_index' or k=='max_attr_index' or k=='dtypes'):
                        continue
                    else:
                        if(v['renamed_rel']==node2_renamed):
                            join_graph.ignored_attrs.append(v['columns'][n2_a])

            # logger.debug("@@@@ node1.ignored_attrs @@@@")
            # logger.debug(node1.ignored_attrs)
            # logger.debug("@@@@ node2.ignored_attrs @@@@")
            # logger.debug(node2.ignored_attrs)

            where_clause_tokens.append(cond[0])

        for k,v in renaming_dict.items():
            if(k=='max_rel_index' or k=='max_attr_index' or k=='dtypes'):
                continue
            else:
                a_from_token = "{} AS {}".format(v['label'],v['renamed_rel'])
                from_clause_tokens.append(a_from_token)
                for k1,v1 in v['columns'].items():
                    if(v['label']=='PT'):
                        a_select_token = '{}."{}" AS {}'.format(v['renamed_rel'],k1,v1)
                    else:
                        a_select_token = '{}.{} AS {}'.format(v['renamed_rel'],k1,v1)
                    select_clause_tokens.append(a_select_token)


        if(not where_clause_tokens):
            query = "SELECT {} \n FROM {}\n".format(", ".join(select_clause_tokens),
                                                    ", ".join(from_clause_tokens)
                                                        )
        else:
            query = "SELECT {} \n FROM {} \n WHERE {} \n".format(", ".join(select_clause_tokens),
                                                                 ", ".join(from_clause_tokens),
                                                                 " AND ".join(where_clause_tokens)
                                                                 ) 
        self.stats.stopTimer('compose_query')

        if(cost_estimate):
            cost_estimate_q = f"""
            SELECT (((plan->0->'Plan'->>'Total Cost')::float8) * 100.0)::int          
             FROM  COST_ESTIMATION('{query}') AS p(plan);
            """
            self.cur.execute(cost_estimate_q)
            estimated_cost = int(self.cur.fetchone()[0])
        else:
            estimated_cost = None

        return estimated_cost, renaming_dict, query
  


