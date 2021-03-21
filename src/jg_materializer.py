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
import copy
from collections import Counter
import pandas as pd


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

    def __init__(self, conn, db_dict, gwrapper, user_query):
        """
        db_dict: a dictionary storing relation name and its attributes
        Note: db_dict['PT'] also needs to be added 
        gwrapper: gprom wrapper used to do the "redudancy check"
        """
        self.conn = conn
        self.db_dict = db_dict
        self.stats = QueryGeneratorStats()
        self.cur = self.conn.cursor()
        self.pt_ec = None # equivalent class from the original pt
        self.pt_rename_dict = None 
        self.gwrapper = gwrapper
        self.user_query = user_query # used to get the initial EC (since GProM wouldnt work properly)


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


    def get_col_info(self, renamed_attribute, db_dict, jg_rename_dict, ec_id):
        """
        return which base table the renamed_attribute belongs to 
        and if it is a part of the keys and the pkeys of that base table
        """
        re_a_index = int(re.findall(r'([0-9]+)', renamed_attribute)[0])

        ispk=True
        for k,v in jg_rename_dict.items():
            if(k=='max_rel_index' or k=='max_attr_index' or k=='dtypes'):
                continue
            else:
                if(re_a_index>=v['rel_min_attr_index'] and re_a_index<=v['rel_max_attr_index']):
                    tid = k
                    t_a_name = v['columns'][renamed_attribute]
                    t_name = v['label']
                    if(t_name=='PT'):
                        atype = jg_rename_dict['dtypes'][renamed_attribute]
                        (t_name, t_a_name) = db_dict['PT']['attributes'][':'.join([t_a_name,atype])]
                        t_pk = db_dict[t_name]['p_key']
                    else:
                        t_pk = db_dict[t_name]['p_key']
                    if(t_a_name in t_pk):
                        ispk = True
                    break
                else:
                    continue

        res = {'table':t_name, 'table_identity':tid, 
        'original_attr_name':t_a_name, 
        'is_part_of_pk':ispk, 'table_pk':t_pk, 'ec_id':ec_id}
        
        # logger.debug(res)

        return res


    def gen_ec(self, query):
        """
        generate original equivalent class of PT

        jg_pt_only: jg that only has pt as its node,
        whose renaming results will be universal renaming 
        scheme for the other jgs  
        """
        # logger.debug(query)
        errcode, output_raw = self.gwrapper.runQuery(query, ec_options=True)
        logger.debug(query)
        output = output_raw.decode("utf-8")
        logger.debug(output)
        reg_rc_line = re.compile(r'EC T_ProjectionOperator.*\nList size [0-9]+\n({.*})')
        rc_line = reg_rc_line.search(output).group(1)
        res = re.findall(r'(\{.*?\})', rc_line)
        # logger.debug(res)

        return [x.strip('{}').split(' ') for x in res]


    def process_pt_ec(self, pt_raw_ec, jg_rename_dict):
        processed_ec = []
        for one_ec in pt_raw_ec:
            processed_one_ec = []
            for col in one_ec:
                for k,v in jg_rename_dict[1]['columns'].items():
                    if(col==v):
                        processed_one_ec.append(k)
                        break
            processed_ec.append(processed_one_ec)

        return processed_ec


    def lists_overlap(self, a, b):
        return bool(set(a) & set(b))


    def modifiy_jg_ec(self, jg_ec, pt_ec):
        res = []
        modifier_from_pt_ec = [pe for pe in pt_ec if len(pe)>1]
        
        # modify: adding relationships from x 
        for m in modifier_from_pt_ec:
            for i in range(len(jg_ec)):
                if(self.lists_overlap(m,jg_ec[i])):
                    jg_ec[i] = m+jg_ec[i]

        # combine elements from modified jg_ec 
        # then do a set operation

        while(jg_ec):
            i = 0
            index_to_del = [i]
            re = jg_ec[i]
            while(i<len(jg_ec)-1):
                i+=1
                if(self.lists_overlap(re,jg_ec[i])):
                    re = re+jg_ec[i]
                    index_to_del.append(i)
            res.append(re)
            if(index_to_del):
                for index in sorted(index_to_del, reverse=True):
                    del jg_ec[index]   
        ret = [list(set(r)) for r in res]
        # logger.debug(ret)
        return ret


    def non_redundant_check(self, jg_target_query, jg_rename_dict):
        """
        return bool to indentify if the jg candidate fulfills
        the requirement of the augmented provenance have indeed
        brought in new informations

        checking equivalent classes and compare it with the original
        pt's equivalent classes pt_ec
        """

        # logger.debug(self.db_dict)
        # logger.debug(jg_rename_dict)

        if(self.pt_ec is None):
            pt_raw_ec = self.gen_ec(self.user_query)
            self.pt_ec = self.process_pt_ec(pt_raw_ec, jg_rename_dict)
            # map the raw EC returned by GProM to renamed attributes
            # only need to do this once since every jg will have the same 
            # mapping scheme for PT node 

        # logger.debug(self.pt_ec)
        
        jg_target_ec = [x for x in self.gen_ec(jg_target_query)
            if x!="pnumber" and x!="is_user"]
        
        # logger.debug(jg_target_ec)

        jg_target_ec = self.modifiy_jg_ec(jg_target_ec, self.pt_ec)

        # logger.debug(jg_target_ec)

        # rules to check if a jg_target is bringing new info using ECs
        # 1) if 2 ECs have the same number of entries, then it is guaranteed to be redundant
        #    if not go to rule 2
        # 2) for 2 ECs having the different number of entries, check to see if ECs having more than
        #    1 attribute are equivalent because of they are the same relation and are keys, if yes 
        #    then this is also redundant

        if(len(self.pt_ec) == len(jg_target_ec)): # rule 1)
            return False
        else:
            ecs_more_than_one = [ec for ec in jg_target_ec if len(ec)>1]
            if(not ecs_more_than_one): # meaning no need to check rule 2, and it is non redundant
                return True
            else:
                ec_info_dicts = []
                ec_id = 1
                for ec in ecs_more_than_one:
                    for renamed_a in ec:
                        col_info_dict = self.get_col_info(renamed_a, self.db_dict, jg_rename_dict, ec_id)
                        if(col_info_dict['is_part_of_pk']):
                            ec_info_dicts.append(col_info_dict)
                    ec_id+=1

                if(not ec_info_dicts):
                    return True
                else:
                    ec_df = pd.DataFrame(ec_info_dicts)
                    # logger.debug('~~~~~~original ec_df~~~~~~~')
                    # logger.debug(ec_df)
                    ec_df = ec_df.groupby(['table','ec_id','original_attr_name'])['table_identity'].apply(list).reset_index()
                    # logger.debug('!!!after group by ec_df !!!!!')
                    # logger.debug(ec_df)
                    ec_df['table_identity'] = ec_df['table_identity'].apply(lambda x : ','.join(map(str,sorted(x))))
                    # logger.debug('$$$$$$ second ec_df $$$$$$$')
                    # logger.debug(ec_df)
                    ec_df = ec_df.groupby(['table_identity','table'])['original_attr_name'].apply(list).apply(sorted).reset_index()
                    ec_df = ec_df[ec_df['table_identity'].str.contains(",")]
                    # logger.debug('^^^^^^^final ec_df^^^^^^^^')
                    # logger.debug(ec_df)
                    if(not ec_df.empty):
                        for index, row in ec_df.iterrows():
                            if(','.join(self.db_dict[row['table']]['p_key'])==','.join(row['original_attr_name'])):
                                return False
                    return True


    def materialize_jg(self, join_graph, cost_estimate=False):

        renaming_dict = {}
        # dictionary storing the renamings from renaming.encode function

        # renaming nodes and their attributes
        self.stats.startTimer('renaming')
        for node in join_graph.graph_core:
            if(node.key==1):
                if(self.pt_rename_dict is None):
                    rename_input_dict = {"key": node.key,
                    "label": "PT",
                    "columns": list(self.db_dict["PT"]['attributes'])
                    }
                    self.pt_rename_dict = encode(rel=rename_input_dict, map_dict = renaming_dict, is_pt=True)
                renaming_dict = copy.deepcopy(self.pt_rename_dict) 
            else:
                rename_input_dict = {"key": node.key,
                "label": node.label,
                "columns": list(self.db_dict[node.label]['attributes'])
                }
                renaming_dict = encode(rel=rename_input_dict, map_dict = renaming_dict, is_pt=False)

        self.stats.stopTimer('renaming')

        # logger.debug(self.db_dict)
        # now generate the query to output the augmented provenance table

        # first, we need to rename the join conditions 

        select_clause_tokens = []
        where_clause_tokens = []
        from_clause_tokens = []

        # if jg has only one node, then don't need to worry about join conditions, 
        # just need to take  care of ignored attributes

        self.stats.startTimer('compose_query')
        PT_key_attributes = self.db_dict['PT']['keys']

        if(len(join_graph.graph_core)==1):

            join_graph.ignored_attrs.extend([kk for kk,vk in renaming_dict[1]['columns'].items() 
                if vk in PT_key_attributes])

            join_graph.ignored_attrs.extend([ku for ku,vu in renaming_dict[1]['columns'].items() 
                if vu in self.db_dict['PT']['user_text_attrs']])

            join_graph.user_attrs.extend([ku for ku,vu in renaming_dict[1]['columns'].items() 
                if vu in self.db_dict['PT']['user_numerical_attrs']])


            # for k,v in renaming_dict.items():
            #     if(k=='max_rel_index' or k=='max_attr_index' or k=='dtypes'):
            #         continue
            #     else:
            #         if(v['label']=='PT'):
            #             join_graph.ignored_attrs.extend([kk for kk,vk in v['columns'].items() 
            #                 if vk in PT_key_attributes])
            #             join_graph.ignored_attrs.extend([ku for ku,vu in v['columns'].items() 
            #                 if vu in self.db_dict['PT']['user_attrs']])
            #             break
            
        else:
            # based on the renaming result, change the join conditions accordingly          
            for node1, node2, cond in join_graph.graph_core.edges.data('condition'):
                logger.debug(cond)
                node1_renamed = renaming_dict[node1.key]['renamed_rel']
                node2_renamed = renaming_dict[node2.key]['renamed_rel']
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

                    # if(self.db_dict['PT']['user_attrs']):
                    join_graph.ignored_attrs.extend([kk for kk,vk in renaming_dict[1]['columns'].items() 
                        if vk in PT_key_attributes])

                    join_graph.ignored_attrs.extend([kr for kr,ko in renaming_dict[1]['columns'].items()
                        if ko in self.db_dict['PT']['user_text_attrs']])
                    
                    join_graph.user_attrs.extend([kr for kr,ko in renaming_dict[1]['columns'].items()
                        if ko in self.db_dict['PT']['user_numerical_attrs']])
                else:
                    node1_key_attributes = self.db_dict[node1.label]['p_key']

                    for k,v in renaming_dict.items():
                        if(k=='max_rel_index' or k=='max_attr_index' or k=='dtypes'):
                            continue
                        else:
                            if(v['renamed_rel']==node1_renamed):
                                join_graph.ignored_attrs.extend([k1 for k1,v1 in v['columns'].items() 
                                    if v1 in node1_key_attributes])
                                break

                node2_key_attributes = self.db_dict[node2.label]['p_key']

                for k,v in renaming_dict.items():
                    if(k=='max_rel_index' or k=='max_attr_index' or k=='dtypes'):
                        continue
                    else:
                        if(v['renamed_rel']==node2_renamed):
                            join_graph.ignored_attrs.extend([k2 for k2,v2 in v['columns'].items() 
                                if v2 in node2_key_attributes])
                            break

                cond[0] = re.sub(r'\b{}[.]'.format(node1_original), 
                                                   f'{node1_renamed}.', 
                                                   cond[0])

                cond[0] = re.sub(r'\b{}[.]'.format(node2_original), 
                                                   f'{node2_renamed}.', 
                                                   cond[0])



                where_clause_tokens.append(cond[0])

        for k,v in renaming_dict.items():
            if(k=='max_rel_index' or k=='max_attr_index' or k=='dtypes'):
                continue
            else:
                a_from_token = "{} AS {}".format(v['label'],v['renamed_rel'])
                from_clause_tokens.append(a_from_token)
                for k1,v1 in v['columns'].items():
                    if(v['label']=='PT'):
                        a_select_token = '{}."{}" AS {}'.format(v['renamed_rel'],v1,k1)
                    else:
                        a_select_token = '{}.{} AS {}'.format(v['renamed_rel'],v1,k1)
                    select_clause_tokens.append(a_select_token)


        if(not where_clause_tokens):
            query = "SELECT {} \n FROM {};\n".format(", ".join(select_clause_tokens),
                                                    ", ".join(from_clause_tokens)
                                                        )
        else:
            query = "SELECT {} \n FROM {} \n WHERE {}; \n".format(", ".join(select_clause_tokens),
                                                                 ", ".join(from_clause_tokens),
                                                                 " AND ".join(where_clause_tokens)
                                                                 )
        if(not self.non_redundant_check(query, renaming_dict)):
            # logger.debug("@@@@ a redundant jg @@@@@@@@@")
            # logger.debug(query)
            query = None

        self.stats.stopTimer('compose_query')

        if(cost_estimate and query is not None):
            cost_estimate_q = f"""
            SELECT (((plan->0->'Plan'->>'Total Cost')::float8) * 100.0)::int          
             FROM  COST_ESTIMATION('{query}') AS p(plan);
            """
            self.cur.execute(cost_estimate_q)
            estimated_cost = int(self.cur.fetchone()[0])
        else:
            estimated_cost = None

        return estimated_cost, renaming_dict, query
  


