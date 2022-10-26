from src.sg_generator import Schema_Graph_Generator
from src.pattern_generator import Pattern_Generator
from src.jg_materializer import Join_Graph_Materializer
from demo.app import getUserSelection as appUselection
from src.provenance_getter import provenance_getter
from src.gprom_wrapper import  GProMWrapper
from src.hashes import fnv1a_init, fnv1a_update_str
from src.instrumentation import ExecStats
from networkx import MultiGraph
import networkx as nx
import psycopg2
import logging 
from statistics import mean 
from copy import deepcopy
import re
import time
import numpy as np
import random

class JGGeneratorStats(ExecStats):
    """
    Statistics gathered during mining
    """
    TIMERS = {'jg_enumeration',
              'jg_validation',
              'jg_hashing',
              'jg_simulation'
              }

    PARAMS = {'number_of_jgs',
              'valid_jgs',
              'valid_jgs_cost_high',
              'jg_e_cum',
              'jg_h_cum',
              'jg_v_cum',
              'jg_s_cum',
              'jg_utime_cum'              
             }


logger = logging.getLogger(__name__)

class testingStats(ExecStats):
    TIMERS = {'jg_simulation'}


class Node:

    def __init__(self,label,cond_keys):
        self.key = None
        self.label = label  # join condition etc goes in here
        self.cond_keys = cond_keys # primary key attributes for this relation (PT node does not have this, initialize as None)
        self.ignored_attrs = [] # attrs user chose to ignore when create customized tree
        self.cond_keys_used = [] # used to track what primary key attributes from this relation has been used as join condition in the current join graph

    def __repr__(self):
        # return f"(key: {self.key}, label: {self.label})"
        return f"{self.label}"

    def __eq__(self, other):
        return isinstance(other, Join_Graph) and self.__hash__() == other.__hash__()

    def __hash__(self):
        return hash(self.label)        


class Join_Graph:
    """
    This class barely serves as a wrapper for multigraph from networkx package
    The reason to do this is to make a customized hashable join graph object
    """
    def __init__(self, graph_core, jg_number, spec_node_key=1, intermediate=False, num_edges=0):
        """
        graph_core: A multigraph from networkx

        spec_node_key: The key of the node of which during pattern generation 
        a pattern has to include at least one constant value from the attributes 

        ignored_attrs: those are keys or attributes appeared in user query(such 
        as group by, A='a' etc)

        intermediate: A boolean to indicate if this join graph needs to be used 
        to generate patterns or not (if this join graph is added a mapping table,
        then there are no eligible attributes to be considered, this should not be 
        considered)

        cost: query estimation cost as a measure to determine if this jg will
        be materialized

        renaming_dict: renaming dictionary consisting relations and attributes mappings

        apt_create_q: query to create augmented provenance table
        """
        
        self.jg_number = jg_number
        self.graph_core = graph_core
        self.ignored_attrs = []
        self.user_attrs = []
        self.spec_node_key = spec_node_key
        self.intermediate = intermediate
        self.cost = 0
        self.num_edges = num_edges
        self.renaming_dict = None
        self.apt_create_q = None
        self.redundant = False


    def __repr__(self):

        ret_list = []
        for u, v, cond in self.graph_core.edges.data('condition'):
            cond_print = cond[0].replace("'","''")
            ret_list.append(f"{u.key}: {u.label}, {v.key}: {v.label}, cond: {cond_print}")


        return f"{self.jg_number} : {'| '.join(sorted(ret_list))}"

    def __str__(self):

        ret_list = []
        for u, v, cond in self.graph_core.edges.data('condition'):
            ret_list.append(f"{u.key}: {u.label}, {v.key}: {v.label}")

        return f"{'| '.join(sorted(ret_list))}"

    def __eq__(self, other):
        return isinstance(other, Join_Graph) and self.__hash__() == other.__hash__()

    def __hash__(self):
        return hash(self._gen_sorted_info())

    def _gen_sorted_info(self):
        nodes_list = []
        cond_list = []
        for n in self.graph_core:
            nodes_list.append(n.label)
        if(self.graph_core.edges.data('condition')):
            for u, v, cond in self.graph_core.edges.data('condition'):
                cond_list.append(cond[0])

            nodes_list.sort()
            cond_list.sort()
            return ','.join(nodes_list+cond_list)
        else:
            nodes_list.sort()
            return ','.join(nodes_list)
            

class Join_Graph_Generator:

    def __init__(self, schema_graph, attr_dict, gwrapper, uquery, uq1, uq2, db, conn, uquery0, exclude_high_cost_jg_0, 
                sample_rate_for_s_tmp, lca_s_max_size, lca_s_min_size, lca_eval_mode, min_recall_threshold,
                numercial_attr_filter_method, user_pt_size, user_questions_map, f1_sample_type, f1_min_sample_size_threshold,
                user_assigned_max_num_pred, simul_u, simul_r, simul_s): #, jg_e_cum, jg_h_cum, jg_v_cum, jg_s_cum, jg_utime_cum):
        self.schema_graph = schema_graph
        self.hash_jg_table = {} # a hash dictionary that used to check duplicates
        self.attr_dict = attr_dict
        self.stats = JGGeneratorStats()
        self.gwrapper = gwrapper
        self.uquery = uquery
        self.uq1 = uq1
        self.uq2 = uq2
        self.db = db
        self.conn = conn
        self.uquery0 = uquery0
        self.exclude_high_cost_jg_0 = exclude_high_cost_jg_0
        self.sample_rate_for_s_tmp = sample_rate_for_s_tmp
        self.lca_s_max_size = lca_s_max_size
        self.lca_s_min_size = lca_s_min_size
        self.lca_eval_mode = lca_eval_mode
        self.min_recall_threshold = min_recall_threshold
        self.numercial_attr_filter_method = numercial_attr_filter_method
        self.user_pt_size = user_pt_size
        self.user_questions_map = user_questions_map
        self.f1_sample_type = f1_sample_type
        self.f1_min_sample_size_threshold = f1_min_sample_size_threshold
        self.user_assigned_max_num_pred = user_assigned_max_num_pred
        logger.debug('initialize simulation type, rate, stop')
        self.simul_u=simul_u #simulation type
        self.simul_r=simul_r #simulation rate
        self.simul_s=simul_s
        logger.debug('initialize jg_e_cum and etc')
        # self.jg_e_cum = 0 #jg_enumeration cumulation
        # self.jg_h_cum = 0 #jg_hashing cumulation
        # self.jg_v_cum = 0 #jg_validation cumulation
        # self.jg_s_cum = 0 #jg_simulation cumulation
        # self.jg_utime_cum = 0 #jg_user_time cumulation
        # self.jg_testing = {}
        # self.t_stats = testingStats()


    def valid_check(self, jg_candidate, pt_rels):
        """
        used to confirm if a jg_candidate is valid by checking if 
        each node other than PT has its PK constraint fulfilled
        """

        remaining_conditions = {}

        # logger.debug(jg_candidate)
        
        for n1, n2, cond in jg_candidate.graph_core.edges.data('condition'):
            if(n1.label=='PT'):
                if(n2 not in remaining_conditions):
                    remaining_conditions[n2] = []
                    for n in pt_rels:
                        if(self.schema_graph.has_edge(n2.label, n)):
                            for nn1, nn2, key_dict in self.schema_graph.edges.data('key_dict'):
                                if((nn1==n2.label and nn2==n) or (nn1==n and nn2==n2.label)):
                                    remaining_conditions[n2].append(key_dict[n2.label])
                remaining_conditions[n2] = [rc for rc in remaining_conditions[n2] if len(set(rc).intersection(cond[1][n2.label]))==0]

            if(n1.label!='PT'):
                n1_pkey_attrs_involved = [pk for pk in re.findall(r'{}\.(\w+)?'.format(n1.label),cond[0]) if pk in n1.cond_keys]
                if(n1_pkey_attrs_involved):
                    n1.cond_keys_used.extend(n1_pkey_attrs_involved)
            if(n2.label!='PT'):
                n2_pkey_attrs_involved = [pk for pk in re.findall(r'{}\.(\w+)?'.format(n2.label),cond[0]) if pk in n2.cond_keys]
                if(n2_pkey_attrs_involved):
                    n2.cond_keys_used.extend(n2_pkey_attrs_involved)

        # logger.debug(remaining_conditions)
        for k,v in remaining_conditions.items():
            if(len(v)!=0):
                return False

        for n in jg_candidate.graph_core:
            if(n.label!='PT'):
                n.cond_keys.sort()
                # logger.debug(n.label)
                # logger.debug(n.cond_keys)
                n.cond_keys_used.sort()
                # logger.debug(n.cond_keys_used)
                # logger.debug('\n')
                if(set(n.cond_keys)!=set(n.cond_keys_used)):
                    # logger.debug('False!')
                    # logger.debug('\n')
                    return False
        return True


    def add_one_edge(self, j_graph_target, node1_label, node2_label, condition):
        """
        adding an edge to j_graph_target given node1 and node2 and condition with the edge

        return plans as a list of tuples with 4 elements 
        """
        plans_just_edge = []
        plans_edge_w_new_node = []

        for n in j_graph_target.graph_core:
            logger.debug(n.label)
            logger.debug(node1_label)
            logger.debug(node2_label)
            ########################
            if self.checkFiltering(node2_label) == -1:
                break
            ########################
            if(n.label == node1_label):
                for nn in j_graph_target.graph_core:
                    FoundNode2 = False
                    if(nn.label == node2_label):
                        FoundNode2 = True
                        CondExists = False
                        if(j_graph_target.graph_core.has_edge(n,nn)):
                            existing_edges_dict = j_graph_target.graph_core.get_edge_data(n,nn)
                            if(existing_edges_dict):
                                for e,v in existing_edges_dict.items():
                                    if(v['condition'][0] == condition[0]): 
                                        CondExists = True
                                        break
                                    else:
                                        continue
                            if(CondExists==False):
                                plans_just_edge.append((j_graph_target,n.key, nn.key, condition))
                if(not FoundNode2):
                    plans_edge_w_new_node.append((j_graph_target, n.key, node2_label, condition))
        # logger.debug(j_graph_target) ####
        # logger.debug(n.key) ####
        # logger.debug(node2_label) ####
        # logger.debug(condition) ####
        logger.debug(plans_edge_w_new_node) ####
        return plans_just_edge, plans_edge_w_new_node
    
    def gen_new_jg(self, j_graph_target, pt_rels, jg_cur_number):
        """
        j_graph_target: j_graph to add an edge to

        return: a list of tuples
        where  each tuple has 4 elements
        0: j_graph being added to
        1: key of the node being checked, 
        2: new node(adding node option) or a key value(adding edge option)) 
        3: condition
        """

        new_jgs = []
        j_graph_creation_plans = {'edges_only':[], 'edges_w_node':[]}

        logger.debug(j_graph_target) ####
        for jn in j_graph_target.graph_core:
            logger.debug(jn) ####
            if(jn.label=='PT'):
                for pt_n in pt_rels:
                    # for n in self.schema_graph:
                    #     logger.debug(n)
                    #     print("n: ",n)
                    # logger.debug(self.schema_graph) ###############
                    #self.setFilteringList(self.schema_graph, pt_n)
                    #print("////////////////////////////////////print filteringlist: ", filteringList)
                    #########################################
                    for n in self.schema_graph:
                        if(self.schema_graph.has_edge(pt_n, n)):
                            for cond in [x for x in self.schema_graph.get_edge_data(pt_n, n).values()]:
                                # replace pt_n.label with "PT"
                                # cond_str = cond['condition'].replace(f'{pt_n}.', 'PT.')
                                cond_str = re.sub(f'(?<!_){pt_n}\.', 'PT.', cond['condition'])
                                #logger.debug(j_graph_target) ####
                                logger.debug(n) ####
                                edge_only_plans, edges_w_node_plans = self.add_one_edge(j_graph_target, 'PT', n, [cond_str, cond['key_dict'], pt_n])
                                j_graph_creation_plans['edges_only'].extend(edge_only_plans)
                                j_graph_creation_plans['edges_w_node'].extend(edges_w_node_plans)
            else:
                for n in self.schema_graph:
                    if(self.schema_graph.has_edge(jn.label,n)):
                        for cond in [ x for x in self.schema_graph.get_edge_data(jn.label,n).values()]:
                            logger.debug(jn.label) ####
                            logger.debug(n) ####
                            edge_only_plans, edges_w_node_plans  = self.add_one_edge(j_graph_target, jn.label, n, [cond['condition'], cond['key_dict'], None])
                            j_graph_creation_plans['edges_only'].extend(edge_only_plans)
                            j_graph_creation_plans['edges_w_node'].extend(edges_w_node_plans)
        logger.debug(j_graph_creation_plans) ################
        if(j_graph_creation_plans['edges_only']):
            for n in j_graph_creation_plans['edges_only']:
                new_jg = deepcopy(n[0])
                new_jg.num_edges = n[0].num_edges+1
                cond_to_add = deepcopy(n[3])
                new_jg.jg_number = jg_cur_number
                jg_cur_number+=1
                for m in new_jg.graph_core:
                    if(m.key == n[1]):
                        node1 = m
                    if(m.key == n[2]):
                        node2 = m
                new_jg.graph_core.add_edge(node1, node2, condition=cond_to_add)
                new_jgs.append(new_jg)
        logger.debug(j_graph_creation_plans) ################
        if(j_graph_creation_plans['edges_w_node']):
            for n in j_graph_creation_plans['edges_w_node']:
                new_jg = deepcopy(n[0])
                new_jg.num_edges = n[0].num_edges+1
                cond_to_add = deepcopy(n[3])
                new_jg.jg_number = jg_cur_number
                jg_cur_number+=1
                for m in new_jg.graph_core:
                    if(m.key == n[1]):
                        node1 = m
                node2 = Node(label=n[2], cond_keys=self.attr_dict[n[2]]['edge_keys'])
                node2.key = new_jg.graph_core.graph['max_node_key']
                attrs_from_node2 = [x[0] for x in self.attr_dict[n[2]]['attributes']]
                keys_from_node2 = self.attr_dict[n[2]]['keys']
                if(attrs_from_node2==keys_from_node2):
                    new_jg.intermediate = True
                else:
                    new_jg.intermediate = False
                    new_jg.spec_node_key = node2.key

                new_jg.graph_core.graph['max_node_key']+=1
                new_jg.graph_core.add_edge(node1, node2, condition=cond_to_add)
                new_jgs.append(new_jg)
        
        logger.debug(new_jgs) ####
        return jg_cur_number, new_jgs
    ###########################################################################################################################
    ###########################################################################################################################
    ###########################################################################################################################
    global jgs_selection
    jgs_selection = {}
    global jgNum
    #jgNum = 0
    def setJGselection(self, cur_edge_tmp, valid_jg_tmp, jgNum):
        tmp = []
        tmp.append(jgNum)
        tmp.append(valid_jg_tmp)
        jgs_selection[cur_edge_tmp] = tmp
        #jgs_selection[cur_edge_tmp] = valid_jg_tmp
    def getJGselection(self, cur_edge_tmp):
        return jgs_selection[cur_edge_tmp]
    def setJGnum(self, jgCurNum):
        global jgNum
        jgNum = jgCurNum

    global filteringList
    filteringList = [] #{} #[]
    # def setFilteringList(self): #, schema_graph, pt_n):
    #     for n in self.schema_graph:
    #         ###if(schema_graph.has_edge(pt_n, n)):
    #         if n not in filteringList:
    #             tmp = []
    #             tmp.append(n)
    #             tmp.append(0)
    #             filteringList.append(tmp)
    #             #filteringList[n] = 0
    #             #filteringList.append(n)
    def setFiltering(self, filtering_tmp):
        for n in filtering_tmp:
            if n not in filteringList:
                filteringList.append(n)
        print(filteringList)
        # print("<<Filter/Unfilter>> Enter number (Skip:-1): ")
        # for i in range(0, len(filteringList)):
        #     print('[',i,']',filteringList[i][0], end=' ')
        # print()

        # filterSelection = int(input())
        # if filterSelection!=-1:
        #     if filteringList[filterSelection][1]==0:
        #         filteringList[filterSelection][1] = -1
        #     else:
        #         filteringList[filterSelection][1] = 0
        # self.displayFiltering()

    # def displayFiltering(self):
    #     print("[Filtered] ", end="")
    #     for x in filteringList:
    #         if x[1]==-1:
    #             print(x[0], end=" ")
    #     print()
    def checkFiltering(self, check):
        for x in filteringList:
            if check in filteringList:
                return -1
        # for x in filteringList:
        #     if x[0]==check and x[1]==-1:
        #         return -1
        return 0

    global pt_cond_list
    global node_cond_list
    pt_cond_list = []
    node_cond_list = []
    def clean_text(self, inputStr):
        #new_text = re.sub(':)(', '', inputStr)
        new_text = inputStr.replace(':', '').replace('(', '').replace(')', '')
        return new_text.replace(' ', '')
    def findPT(self, tmp):
        if 'PT' in tmp:
            return 1
        # if 'PT' in tmp:
        #     return tmp.split('.')[1]
        # else:
        #     return tmp.split('.')[1]
    def parsingCond(self, usel, r):
        if 'cond' not in usel:
            node_cond=""
            pt_cond=""
            node=""
        else:
            if '|' not in usel:
                getCond = usel.split('cond')
                cond = getCond[1].split('=')
                cond1 = cond[0] # : (team.team_id)
                cond2 = cond[1] # (PT.home_id)
                tmp1 = self.clean_text(cond1) # team.team_id
                tmp2 = self.clean_text(cond2) # PT.home_id
                if self.findPT(tmp1):
                    pt_cond = tmp1.split('.')[1]
                    node_cond = tmp2.split('.')[1]
                    node = tmp2.split('.')[0]
                else:
                    pt_cond = tmp2.split('.')[1] # pt_cond = home_id
                    node_cond = tmp1.split('.')[1] # node_cond = team_id
                    node = tmp1.split('.')[0] # node = team
                # if r==1:
                #     pt_cond_list.append(pt_cond)
                #     node_cond_list.append(node_cond)
            else:
                #17 : 1: PT, 2: team, cond: (team.team_id)=(PT.home_id)| 1: PT, 2: team, cond: (team.team_id)=(PT.winner_id)
                splitPipe = usel.split('|')
                for i in range(0, len(splitPipe)):
                    getCond = splitPipe[i].split('cond')
                    cond = getCond[1].split('=')
                    cond1 = cond[0] # : (team.team_id)
                    cond2 = cond[1] # (PT.home_id)
                    tmp1 = self.clean_text(cond1) # team.team_id
                    tmp2 = self.clean_text(cond2) # PT.home_id
                    if self.findPT(tmp1):
                        pt_cond = tmp1.split('.')[1]
                        node_cond = tmp2.split('.')[1]
                        node = tmp2.split('.')[0]
                    else:
                        pt_cond = tmp2.split('.')[1] # pt_cond = home_id
                        node_cond = tmp1.split('.')[1] # node_cond = team_id
                        node = tmp1.split('.')[0] # node = team
                    print("pt_cond: ", pt_cond, " node_cond: ", node_cond)
                    print("pt_cond_list: ", pt_cond_list, " node_cond_list: ", node_cond_list)
                    if pt_cond not in pt_cond_list or node_cond not in node_cond_list:
                        # pt_cond_list.append(pt_cond)
                        # node_cond_list.append(node_cond)
                        return pt_cond, node_cond, node
        return pt_cond, node_cond, node
    def ratingDBavg(self, v_jgs): #pt_cond, node_cond, node):
        return_avg = []

        for i in range(0, len(v_jgs)):
            print(">>>>>>>>>",repr(v_jgs[i]))
            pt_cond, node_cond, node = self.parsingCond(repr(v_jgs[i]), 0)
            print("check::::::", pt_cond, node_cond, node)
        
            rconn = psycopg2.connect(database='rating', 
                                        user='postgres', #juseung 
                                        password='1234', 
                                        port='5433', #5432 
                                        host='127.0.0.1')
            cur = rconn.cursor()

            getAVGquery = F"""
                        SELECT ROUND(AVG(urating)::numeric,2)::TEXT
                        FROM (SELECT * FROM myrating.mytable 
                        WHERE node='{node}' AND node_cond='{node_cond}' AND pt_cond='{pt_cond}')
                        AS new;
                        """

            print('getAVGquery: ',getAVGquery)

            cur.execute(getAVGquery)
            getAVG = cur.fetchall()
            print("check2::::: ", getAVG[0][0])
            if getAVG[0][0]:
                return_avg.append(getAVG[0][0])
            else:
                return_avg.append(-1)
                
        # SELECT AVG(urating)
        # FROM (SELECT * FROM myrating.mytable 
        # WHERE node='{node}' and node_cond='{node_cond}' and pt_cond='{pt_cond}')
        # AS new;
        # """
        # SELECT AVG(urating)
        # FROM myrating.mytable
        # WHERE node='{node}' and node_cond='{node_cond}' and pt_cond='{pt_cond}';
        # """

        # cur.execute(getAVGquery)
        # getAVG = cur.fetchall()
        #print("check2::::: ", getAVG[0][0])
        print("return_avg::::: ", return_avg)
        return return_avg

    def ratingDB(self,usel, getUserRating): ###################
        print("******userselection: ", usel)
        #print("<<Rating>> Enter Rate(0 to 5) (Skip:-1): ")

        pt_cond, node_cond, node = self.parsingCond(usel, 1)
        pt_cond_list.append(pt_cond)
        node_cond_list.append(node_cond)
        print("pt_cond_list: ", pt_cond_list, " node_cond_list: ", node_cond_list)

        #getUserRating = int(input())

        # if getUserRating == -1:
        #     print("<<Rating>> Skip Rating")
        # else:
            #connect DB - myrating.rating(id, usel)
            #connect DB - myrating.table(uquery, uq1, uq2, usel, urating)
        rconn = psycopg2.connect(database='rating', 
                                user='postgres', #juseung 
                                password='1234', 
                                port='5433', #5432 
                                host='127.0.0.1')
        cur = rconn.cursor()

        #SELECT * FROM myrating.table;
        #SELECT * FROM myrating.rating;
        getRatingQuery = "SELECT * FROM myrating.mytable;"
        cur.execute(getRatingQuery)
        getRating = cur.fetchall()

        print("*****rating ex:")
        print(getRating)

        # # global pt_cond_list, node_cond_list
        # pt_cond, node_cond, node = self.parsingCond(usel, 1)
        # pt_cond_list.append(pt_cond)
        # node_cond_list.append(node_cond)
        # print("pt_cond_list: ", pt_cond_list, " node_cond_list: ", node_cond_list)

        insertRatingQ = F"""
        INSERT INTO myrating.mytable(node, node_cond, pt_cond, urating)
        VALUES('{node}', '{node_cond}', '{pt_cond}', '{getUserRating}');
        """
        ###[x.replace("'", '') for x in self.uq1]
        # INSERT INTO myrating.table(uquery, uq1, uq2, usel, urating)
        # VALUES('{format(self.uquery.replace("'",''))}', '{format(self.uq1.replace("'",''))}', 
        # '{format(self.uq2.replace("'",''))}', '{usel}', '{getUserRating}');
        # insertRatingQ = F"""
        # INSERT INTO myrating.rating(id, usel)
        # VALUES('{getUserRating}', '{usel}');
        # """
        
        cur.execute(insertRatingQ)

        #cur = rconn.cursor()

        #SELECT * FROM myrating.table;
        #SELECT * FROM myrating.rating;
        getRatingQuery = "SELECT * FROM myrating.mytable;"

        cur.execute(getRatingQuery)
        getRating = cur.fetchall()
        print("*****rating ex:")
        print(getRating)

        rconn.commit()
        
        cur.close()
        rconn.close()

    def getRecomm(self, valid_result, cur_edge, f1_sample_rate, f1c_type): #, connInfo, statstrackerInfo): 
        recomm_result = []
        #####Recommendation
        ###pattern generate
        #result = sample_pgen(vjs)
        #connInfo, statstrackerInfo, 
        #sample_rate_for_s = sample_pgen()

        # pgen_tmp = Pattern_Generator(connInfo)
        # for vr in vjs:
        #     pgen_tmp.gen_patterns(jg = vr,
        #                             jg_name = f"jg_{vr.jg_number}", 
        #                             renaming_dict=, 
        #                             skip_cols=,
        #                             s_rate_for_s = sample_rate_for_s,
        #                             lca_s_max_size=lca_s_max_size,
        #                             lca_s_min_size=lca_s_min_size,
        #                             just_lca=,
        #                             lca_recall_thresh=,
        #                             numercial_attr_filter_method=,
        #                             user_pt_size=,
        #                             original_pt_size=,
        #                             user_questions_map=,
        #                             f1_calculation_type = 'o',
        #                             f1_sample_type=,
        #                             f1_calculation_sample_rate = 0.1,
        #                             f1_calculation_min_size=,
        #                             user_assigned_num_pred_cap=
        #                         )

        jgm = Join_Graph_Materializer(conn=self.conn, db_dict=self.attr_dict, gwrapper=self.gwrapper, user_query=self.uquery0)
        jgm.init_cost_estimator()

        pgen_tmp = Pattern_Generator(self.conn)


        # pattern_ranked_within_jg = {}
        # jg_individual_times_dict = {}

        # cost_friendly_jgs = []
        # not_cost_friendly_jgs = []
        fconn = psycopg2.connect(database='f1rate', 
                                    user='postgres', #juseung 
                                    password='1234', 
                                    port='5433', #5432 
                                    host='127.0.0.1')
        fcur = fconn.cursor()
        logger.debug(self.exclude_high_cost_jg_0)
        if(self.exclude_high_cost_jg_0==False):
            for n in valid_result:
            # for i in range(0, len(valid_result)):
            #     n = repr(valid_result[i])

                cost_estimate, renaming_dict, apt_q = jgm.materialize_jg(n)
                # logger.debug(n.ignored_attrs)
                if(apt_q is not None):
                    n.cost = cost_estimate
                    n.apt_create_q = apt_q
                    n.renaming_dict = renaming_dict
                else:
                    n.redundant = True
                    continue
            jg_cnt=1
            for vr in valid_result:
            # for i in range(0, len(valid_result)):
            #     vr = repr(valid_result[i])

                jg_cnt+=1
                drop_if_exist_jg_view = "DROP MATERIALIZED VIEW IF EXISTS {} CASCADE;".format('jg_{}'.format(vr.jg_number))
                jg_query_view = "CREATE MATERIALIZED VIEW {} AS {}".format('jg_{}'.format(vr.jg_number), vr.apt_create_q)
                jgm.cur.execute(drop_if_exist_jg_view)
                jgm.cur.execute(jg_query_view)
                apt_size_query = f"SELECT count(*) FROM jg_{vr.jg_number}"
                jgm.cur.execute(apt_size_query)
                apt_size = int(jgm.cur.fetchone()[0])
                pgen_tmp.gen_patterns(jg = vr,
                                    jg_name = f"jg_{vr.jg_number}", 
                                    renaming_dict = vr.renaming_dict, 
                                    skip_cols = vr.ignored_attrs,
                                    s_rate_for_s = self.sample_rate_for_s_tmp,
                                    lca_s_max_size = self.lca_s_max_size,
                                    lca_s_min_size = self.lca_s_min_size,
                                    just_lca = self.lca_eval_mode,
                                    lca_recall_thresh = self.min_recall_threshold,
                                    numercial_attr_filter_method = self.numercial_attr_filter_method,
                                    user_pt_size = self.user_pt_size,
                                    original_pt_size = apt_size,
                                    user_questions_map = self.user_questions_map,
                                    f1_calculation_type = f1c_type, #'o',#'s',#'o',
                                    f1_sample_type = self.f1_sample_type,
                                    f1_calculation_sample_rate = f1_sample_rate, #0.1,
                                    f1_calculation_min_size = self.f1_min_sample_size_threshold,
                                    user_assigned_num_pred_cap = self.user_assigned_max_num_pred
                                    )
                patterns_to_insert_tmp = pgen_tmp.top_pattern_from_one_jg(vr)
                ###############
                print("##############Pattern 1 ##############", patterns_to_insert_tmp)
                if patterns_to_insert_tmp != None: #len(patterns_to_insert_tmp) != 0:
                    tmp = 0
                    for i in range(0, len(patterns_to_insert_tmp)):
                        tmp += patterns_to_insert_tmp[i]['F1']
                    tmp_avg = tmp/len(patterns_to_insert_tmp)
                    recomm_val = round(tmp_avg, 3)
                    recomm_result.append(recomm_val) #round(tmp_avg, 3))
                else:
                    recomm_val = 0
                    recomm_result.append(recomm_val) #0)
                
                insertF1rateQ = F"""
                    INSERT INTO f1sample(f1calrate, f1avg, jg)
                    VALUES('{f1_sample_rate}', '{recomm_val}', '{vr}');
                """

                fcur.execute(insertF1rateQ)
                fconn.commit()
        else:
            valid_result = [v for v in valid_result if not v.intermediate]
            logger.debug(valid_result)
            cost_estimate_dict = {i:[] for i in range(0,cur_edge+1)}
            for vr in valid_result:
            #for i in range(0, len(valid_result)):
                #vr = repr(valid_result[i])

                cost_estimate, renaming_dict, apt_q = jgm.materialize_jg(vr,cost_estimate=True) #vr
                if(apt_q is not None):
                    vr.cost = cost_estimate
                    vr.apt_create_q = apt_q
                    vr.renaming_dict = renaming_dict
                else:
                    vr.redundant=True
                    continue
            valid_result = [v for v in valid_result if not v.redundant]
            logger.debug(valid_result)
            avg_cost_estimate_by_num_edges = {k:mean(v) for k,v in cost_estimate_dict.items() if v}
            jg_cnt=1
            for n in valid_result:
                logger.debug(n.cost)
                logger.debug(avg_cost_estimate_by_num_edges)
                logger.debug(n.num_edges)
                #logger.debug(avg_cost_estimate_by_num_edges[n.num_edges])
            if len(avg_cost_estimate_by_num_edges)!=0:
                valid_result = [n for n in valid_result if n.cost<=avg_cost_estimate_by_num_edges[n.num_edges]]
            logger.debug(valid_result)
            for n in valid_result:
            #for i in range(0, len(valid_result)):
                #n = repr(valid_result[i])

                jg_cnt+=1
                drop_if_exist_jg_view = "DROP MATERIALIZED VIEW IF EXISTS {} CASCADE;".format('jg_{}'.format(n.jg_number))
                jg_query_view = "CREATE MATERIALIZED VIEW {} AS {}".format('jg_{}'.format(n.jg_number), n.apt_create_q)
                jgm.cur.execute(drop_if_exist_jg_view)
                jgm.cur.execute(jg_query_view)
                apt_size_query = f"SELECT count(*) FROM jg_{n.jg_number}"
                jgm.cur.execute(apt_size_query)
                apt_size = int(jgm.cur.fetchone()[0])
                pgen_tmp.gen_patterns(jg = n,
                                    jg_name = f"jg_{n.jg_number}", 
                                    renaming_dict = n.renaming_dict,  
                                    skip_cols = n.ignored_attrs,
                                    s_rate_for_s = self.sample_rate_for_s_tmp,
                                    lca_s_max_size = self.lca_s_max_size,
                                    lca_s_min_size = self.lca_s_min_size,
                                    just_lca = self.lca_eval_mode,
                                    lca_recall_thresh = self.min_recall_threshold,
                                    numercial_attr_filter_method = self.numercial_attr_filter_method,
                                    user_pt_size = self.user_pt_size,
                                    original_pt_size = apt_size,
                                    user_questions_map = self.user_questions_map,
                                    f1_calculation_type = f1c_type, #'o',#'s',#'o',
                                    f1_sample_type = self.f1_sample_type,
                                    f1_calculation_sample_rate = f1_sample_rate, #0.1,
                                    f1_calculation_min_size = self.f1_min_sample_size_threshold,
                                    user_assigned_num_pred_cap = self.user_assigned_max_num_pred
                                    )
                patterns_to_insert_tmp = pgen_tmp.top_pattern_from_one_jg(n)
                ###############
                print("##############Pattern 2##############", patterns_to_insert_tmp)
                if patterns_to_insert_tmp == None or len(patterns_to_insert_tmp) == 0:
                    recomm_val = 0
                    recomm_result.append(recomm_val) #0)
                else:
                    tmp = 0
                    for i in range(0, len(patterns_to_insert_tmp)):
                        tmp+= patterns_to_insert_tmp[i]['F1']
                    tmp_avg = tmp/len(patterns_to_insert_tmp)
                    recomm_val = round(tmp_avg, 3)
                    recomm_result.append(recomm_val) #round(tmp_avg, 3))
                
                insertF1rateQ = F"""
                    INSERT INTO f1sample(f1calrate, f1avg, jg)
                    VALUES('{f1_sample_rate}', '{recomm_val}', '{n}');
                """
                fcur.execute(insertF1rateQ)
                fconn.commit()
        
        fcur.close()
        fconn.close()

        return recomm_result

    global test_insertion_complete
    test_insertion_complete = False

    def f1rate_test(self, valid_jgs, cur_edge):
        rc01 = self.getRecomm(valid_jgs, cur_edge, 0.1, 's')
        rc015 = self.getRecomm(valid_jgs, cur_edge, 0.15, 's')
        rc02 = self.getRecomm(valid_jgs, cur_edge, 0.2, 's')
        rc025 = self.getRecomm(valid_jgs, cur_edge, 0.25, 's')
        rc03 = self.getRecomm(valid_jgs, cur_edge, 0.3, 's')
        rc03_o = self.getRecomm(valid_jgs, cur_edge, 0.3, 'o')

        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>recommendation1>>>>> ", rc01)
        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>recommendation2>>>>> ", rc015)
        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>recommendation3>>>>> ", rc02)
        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>recommendation4>>>>> ", rc025)
        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>recommendation5>>>>> ", rc03)
        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>recommendation5-'o'>>>>> ", rc03_o)

        for i in range(0, len(rc01)):
            ftconn = psycopg2.connect(database='f1rate_test',
                                user='postgres', #juseung 
                                password='1234', 
                                port='5433', #5432 
                                host='127.0.0.1')
            ftcur = ftconn.cursor()

        #for i in range(0, len(recomm1)):
            # insertF1testQ = F"""
            #     INSERT INTO f1test(dataset, rate01, rate015, rate02, rate025, rate03)
            #     VALUES('{self.db}', '{recomm1[i]}', '{recomm2[i]}', '{recomm3[i]}', '{recomm4[i]}', '{recomm5[i]}');
            # """
            insertF1testQ = F"""
                INSERT INTO testing(db, jg, edge, s_01, s_015, s_02, s_025, s_03, o_03)
                VALUES('{self.db}', '{valid_jgs[i]}', '{cur_edge}', '{rc01[i]}', '{rc015[i]}', '{rc02[i]}', '{rc025[i]}', '{rc03[i]}', '{rc03_o[i]}');
            """
            ftcur.execute(insertF1testQ)
            ftconn.commit()

            ftcur.close()
            ftconn.close()
        global test_insertion_complete
        test_insertion_complete = True

    def simulated_user_responses(self, recomm, getRavg, cur_step):

        # do I need to consider back steps?
        # when to stop?

        # if simul_u is True, run this function to generate user responses based on the simulation rate

        # check the simul_s: whether stop or not
        if cur_step >= self.simul_s:
            logger.debug("@@@@@@@@@@")
            logger.debug(f"Current Step: {cur_step} // Simulation Stop: {self.simul_s}")
            return 0, -1

        r_max = max(recomm)
        r_max_idx = [i for i,v in enumerate(recomm) if v==r_max]
        r_max_cnt = len(r_max_idx)

        # only one max
        if r_max_cnt == 1:
            # generate weights
            weights = []
            for i in range(0,len(recomm)):
                if i==r_max_idx[0]:
                    weights.append(self.simul_r)
                else:
                    non_simul_r = (1-self.simul_r)/(len(recomm)-1)
                    weights.append(non_simul_r)

            # get random choices based on the weights
            time.sleep(5)
            uSelect_value = random.choices(recomm, weights,k=1)[0]
            simul_result = recomm.index(uSelect_value)+1

        # more than one max, then consider the averages of user ratings
        else:
            # find max from getRavg
            ravg_max_lst = [getRavg[j] for j in range(0,len(getRavg)) if j in r_max_idx]
            ravg_max = max(ravg_max_lst)
            ravg_max_idx = [i for i,v in enumerate(getRavg) if v==ravg_max]
            ravg_max_cnt = len(ravg_max_idx)

            time.sleep(5)
            # only one max
            if ravg_max_cnt == 1:
                # find max's index, then go with this max value for uSelect_value
                simul_result = getRavg.index(ravg_max)+1

            # also more than on max, get random choice among them
            else:
                simul_result = random.choice(ravg_max_idx)+1          

        # have to return uer selection and user rating
        # no rating in this simulated user response version

        #self.jg_utime_cum = self.jg_utime_cum + 5
        self.stats.params['jg_utime_cum']+=5
        logger.debug(f"jg usertime cum: {self.stats.params['jg_utime_cum']}")
        return simul_result, -1

    def Generate_JGs(self, pt_rels, num_edges, customize=False, filtering_tmp=[]): #, connInfo, statstracker):
        """
        num_edges: this defines the size of a join graph
        pt_rels: relations coming from PT
        customize: whether you want to customize a jg or not (TODO)
        """
        # logger.debug(self.attr_dict)
        jg_cur_number = 1

        if(customize==False):
            generated_jg_set = []
            prev_jg_set = []
            cur_jg_size = 0
            #################################################################################       
            valid_jgs = []
            uSelection=0
            cur_edge=0
            uSelection_jg = ''

            #self.setFilteringList()
            print("///////////////////////////////////////////filtering list: ", filtering_tmp)
            

            while True:
                # self.displayFiltering()
                if filtering_tmp:
                  self.setFiltering(filtering_tmp)


                # print("[*******]cur_edge:",cur_edge)
                # print('[@@@@@@@@@@]jgs selection dic: ', jgs_selection)
                # print()
                # if cur_edge==0 and cur_edge>=2:
                self.stats.startTimer('jg_enumeration')
                print("//////////jg cur num: ", jg_cur_number)
                # self.setJGnum(jg_cur_number)
                if(not prev_jg_set):
                    first_jg_core = MultiGraph(jg_id = '1', max_node_key = 1, sg = self.schema_graph, db_dict=self.attr_dict)
                    first_jg = Join_Graph(graph_core=first_jg_core, jg_number=jg_cur_number, num_edges=0)
                    jg_cur_number+=1
                    first_node = Node(label='PT', cond_keys=None)
                    first_node.key = first_jg.graph_core.graph['max_node_key']
                    first_jg.graph_core.graph['max_node_key']+=1
                    first_jg.graph_core.add_node(first_node)
                    prev_jg_set.append(first_jg)
                    generated_jg_set.extend([first_jg])
                else:
                    if cur_edge>=1:
                        new_jgs=[]
                        jg_cur_number, new_generated_jgs = self.gen_new_jg(uSelection_jg, pt_rels, jg_cur_number) #(valid_jgs[uSelection-1], pt_rels, jg_cur_number)
                        new_jgs.extend(new_generated_jgs)
                    else:
                        new_jgs = []
                        for t in prev_jg_set:
                            #logger.debug(t) ####
                            jg_cur_number, new_generated_jgs = self.gen_new_jg(t, pt_rels, jg_cur_number)
                            new_jgs.extend(new_generated_jgs)
                    prev_jg_set = new_jgs
                    ###if cur_edge != 1:
                    generated_jg_set.clear() ####
                    generated_jg_set.extend(new_jgs)
                
                logger.debug(generated_jg_set) ####
                #if (cur_edge>=1):
                self.stats.stopTimer('jg_enumeration')
                self.stats.params['number_of_jgs']+=len(generated_jg_set)
                self.stats.startTimer('jg_hashing')
                
                jg_hash_table = self.hash_jgs(generated_jg_set)
                self.stats.stopTimer('jg_hashing')
                valid_jgs = []

                self.stats.startTimer('jg_validation')
                for n in jg_hash_table:
                    logger.debug(n) ####
                    if(self.valid_check(n, pt_rels)==True):
                        valid_jgs.append(n)
                self.stats.stopTimer('jg_validation')

                logger.debug(valid_jgs) ####

                valid_jgs.sort(key=lambda j: j.jg_number)
#######################################################################################################
                self.stats.params['jg_e_cum']+=self.stats.time['jg_enumeration']
                self.stats.params['jg_h_cum']+=self.stats.time['jg_hashing']
                self.stats.params['jg_v_cum']+=self.stats.time['jg_validation']
                logger.debug(f"jg enumeration cum: {self.stats.params['jg_e_cum']}")
                logger.debug(f"jg hashing cum: {self.stats.params['jg_h_cum']}")
                logger.debug(f"jg validation cum: {self.stats.params['jg_v_cum']}")
                # self.jg_e_cum = self.jg_e_cum + round(self.stats.time['jg_enumeration'],2)
                # self.jg_h_cum = self.jg_h_cum + round(self.stats.time['jg_hashing'],2)
                # self.jg_v_cum = self.jg_v_cum + round(self.stats.time['jg_validation'],2)
#######################################################################################################

############################################################################################################
############################################################################################################
                self.setJGnum(jg_cur_number)
                print("<<Enter number (Stop:0|Previous step:-1)>>: ")
                # print('valid jg:', valid_jgs)

                self.stats.startTimer('jg_simulation') ################
                while True:                   
                    # print("///////////////////////")
                    if len(valid_jgs)==0 and cur_edge>=1:
                        print("<<<<<empty valid_jgs>>>>>")
                        dict_tmp = self.getJGselection(cur_edge-1)
                        jg_cur_number = dict_tmp[0]
                        valid_jgs = dict_tmp[1]
                        self.setJGnum(jg_cur_number)

                        cur_edge-=1

                        generated_jg_set.clear()
                        print("<<Go back to Previous Step>>")
                    else:
                        print("*************************************************")
                        print(valid_jgs)

                        global test_insertion_complete
                        test_insertion_complete = False
                        ##### Get rate avg #####
                        getRavg = self.ratingDBavg(valid_jgs)
                        

                        ############################################################
                        ########## f1_rate_testing ##########
                        # self.f1rate_test(valid_jgs, cur_edge)
                        # while(True):
                        #     if test_insertion_complete:
                        #         print("COMPLETED>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
                        #         break
                        ############################################################

                        ##### Recommendation #####
                        recomm = self.getRecomm(valid_jgs, cur_edge, 0.25, 's') #self.getRecomm(valid_jgs, cur_edge, 0.3, 'o')
                        #print("recommendation>>>>> ", recomm)
                        
                        ########## get user selection and rating ##########
                        # print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
                        # for i in range(0, len(valid_jgs)):
                        #     print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>[",i+1,"]",repr(valid_jgs[i]))
                        # print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

############################################################################################################################
############################################################################################################################
############################################################################################################################                       
                        # check the simul_u flag if it is True -> run the simulated version for testing
                        # if simul_u flag is True then check simul_r -> the probablity for choosing the highest score
                        # add 5 seconds for the simulated usres for their decision making
                        
                        if self.simul_u:
                            # run the simulated version
                            uSelection, uRating = self.simulated_user_responses(recomm, getRavg, cur_edge)
                            # add 5 seconds to the OR count as a user time

                        else:
                            # run the user interactive version
                            uSelection, uRating = appUselection(valid_jgs,cur_edge, recomm, getRavg)

############################################################################################################################
############################################################################################################################
############################################################################################################################


                        # uSelection, uRating = appUselection(valid_jgs,cur_edge, recomm, getRavg)
                        print("uselection, uRating:******: ", uSelection,"*****",uRating)

                        for i in range(0, len(valid_jgs)):
                            print("[",i+1,"]",repr(valid_jgs[i]))                            
                            # pt_cond, node_cond, node = self.parsingCond(repr(valid_jgs[i]), 0) # parsing                            
                            # print("rate avg: ", self.ratingDBavg(pt_cond, node_cond, node)[0][0])# get avg and print avg

                        # iteration = np.arange(0.1, 0.35, 0.05)
                        # for j in iteration:
                        #     f1_sample_rate = round(j,5)
                        #     recomm = self.getRecomm(valid_jgs, cur_edge,f1_sample_rate)
                        #     print("recommendation>>>>> ", recomm)
                        
                        # recomm = self.getRecomm(valid_jgs, cur_edge,0.1)
                        # print("recommendation>>>>> ", recomm)

                        ################uSelection = int(input())
                        if uSelection==0:
                            break
                        elif uSelection==-1 and cur_edge>=1:
                            ######################valid_jgs = jgs_selection[cur_edge-2]
                            #valid_jgs = self.getJGselection(cur_edge-1) #self.getJGselection(cur_edge-2)
                            dict_tmp = self.getJGselection(cur_edge-1)
                            jg_cur_number = dict_tmp[0]
                            valid_jgs = dict_tmp[1]
                            self.setJGnum(jg_cur_number)

                            cur_edge-=1

                            generated_jg_set.clear()
                            print("<<Go back to Previous Step>>")
                            #print('******jgs selection dic: ', jgs_selection)
                            #print("*******cur_edge:",cur_edge,"***a:",a)
                        elif uSelection==-1 and cur_edge==0:
                            print("<<Can't Go back to Previous Step>>")
                        else:
                            print("<<SELECTED JG>>: ", repr(valid_jgs[uSelection-1]))
                            ####Rating>>>>>
                            ###if uRating exists, if uRating value is not -1, execute ratingDB
                            if uRating!=-1:
                                self.ratingDB(repr(valid_jgs[uSelection-1]), uRating)
                            ##self.ratingDB(repr(valid_jgs[uSelection-1])) #########################################################################
                            #################################################################################################################

                            #print('@@@@@@@@@@jgs selection dic: ', jgs_selection)
                            #############jgs_selection[cur_edge] = valid_jgs #valid_jgs[a-1]
                            self.setJGselection(cur_edge, valid_jgs, jgNum)
                            cur_edge+=1
                            uSelection_jg = valid_jgs[uSelection-1]
                            #print('$$$$$$$$$$jgs selection dic: ', jgs_selection)
                            break

                jg_hash_table.clear() #####
############################################################################################################
############################################################################################################
                #********************** cur_edge+=1
                #print('[######]jgs selection dic: ', jgs_selection)
                if uSelection==0:
                    return valid_jgs
                
                self.stats.stopTimer('jg_simulation') ################
                self.stats.params['jg_s_cum']+=self.stats.time['jg_simulation']
                logger.debug(f"jg simulation cum: {self.stats.params['jg_s_cum']}")
                #self.jg_s_cum = self.jg_s_cum + round(self.stats.time['jg_simulation'],2)
            #return valid_jgs
        else:
            pass

        self.stats.params['jg_e_cum'] = round(self.stats.params['jg_e_cum'],2)
        self.stats.params['jg_h_cum'] = round(self.stats.params['jg_h_cum'],2)
        self.stats.params['jg_v_cum'] = round(self.stats.params['jg_v_cum'],2)
        self.stats.params['jg_s_cum'] = round(self.stats.params['jg_s_cum'],2)

            #################################################################################

            # self.stats.startTimer('jg_enumeration')
            # while(cur_jg_size <= num_edges):
            #     if(not prev_jg_set):
            #         first_jg_core = MultiGraph(jg_id = '1', max_node_key = 1, sg = self.schema_graph, db_dict=self.attr_dict)
            #         first_jg = Join_Graph(graph_core=first_jg_core, jg_number=jg_cur_number, num_edges=0)
            #         jg_cur_number+=1
            #         first_node = Node(label='PT', cond_keys=None)
            #         first_node.key = first_jg.graph_core.graph['max_node_key']
            #         first_jg.graph_core.graph['max_node_key']+=1
            #         first_jg.graph_core.add_node(first_node)
            #         prev_jg_set.append(first_jg)
            #         generated_jg_set.extend([first_jg])
            #     else:
            #         new_jgs = []
            #         for t in prev_jg_set:
            #             logger.debug(t) ####
            #             logger.debug(repr(t)) ####
            #             ######
            #             print("check return: ", self.selection_checker(t, uSelection)) ################
            #             checkValue = self.selection_checker(t, uSelection)
            #             if checkValue == 0:
            #                 jg_cur_number, new_generated_jgs = self.gen_new_jg(t, pt_rels, jg_cur_number, 0)
            #                 new_jgs.extend(new_generated_jgs)
            #             elif checkValue == 1:
            #                 jg_cur_number, new_generated_jgs = self.gen_new_jg(t, pt_rels, jg_cur_number, checkValue)
            #                 new_jgs.extend(new_generated_jgs)
            #             # elif checkValue == -1:
            #             #     break
            #             # else:
            #             #     jg_cur_number, new_generated_jgs = self.gen_new_jg(t, pt_rels, jg_cur_number, checkValue)
            #             #     new_jgs.extend(new_generated_jgs)
            #             #####
            #             # jg_cur_number, new_generated_jgs = self.gen_new_jg(t, pt_rels, jg_cur_number)
            #             # new_jgs.extend(new_generated_jgs)
            #         prev_jg_set = new_jgs
            #         logger.debug(prev_jg_set) ############
            #         generated_jg_set.extend(new_jgs)
            #     cur_jg_size+=1
            # self.stats.stopTimer('jg_enumeration')
            
            # self.stats.params['number_of_jgs']+=len(generated_jg_set)
            # self.stats.startTimer('jg_hashing')
            # # logger.debug(generated_jg_set)
            # jg_hash_table = self.hash_jgs(generated_jg_set)
            # self.stats.stopTimer('jg_hashing')
            # valid_jgs = []

            # self.stats.startTimer('jg_validation')
            # for n in jg_hash_table:
            #     if(self.valid_check(n, pt_rels)==True):
            #         valid_jgs.append(n)
            # self.stats.stopTimer('jg_validation')

            # valid_jgs.sort(key=lambda j: j.jg_number)
            # ########################################
            # print("<<Enter number>>: ")
            # for i in range(0, len(valid_jgs)):
            #     print("[",i+1,"]",repr(valid_jgs[i]))
            # a = int(input())
            # print("<<SELECTED JG>>: ", repr(valid_jgs[a-1]))
            # #########################################
            # # sort it to make sure jg materializer will see PT only first
            # # for v in valid_jgs:
            # #     logger.debug(v)
            # #     logger.debug(f"intermediate? {v.intermediate}")
        #     return valid_jgs
        # else:
        #     pass


    def hash_jgs(self, jg_set):
        if(not jg_set):
            return None
        else:
            for n in jg_set:
                if n in self.hash_jg_table:
                    self.hash_jg_table[n].append(n.jg_number)
                else:
                    self.hash_jg_table[n] = []

            # logger.debug(self.hash_jg_table)
            return self.hash_jg_table
