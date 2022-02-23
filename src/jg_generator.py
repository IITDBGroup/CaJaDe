from src.sg_generator import Schema_Graph_Generator
from src.provenance_getter import provenance_getter
from src.gprom_wrapper import  GProMWrapper
from src.hashes import fnv1a_init, fnv1a_update_str
from src.instrumentation import ExecStats
from networkx import MultiGraph
import networkx as nx
import psycopg2
import logging 
from copy import deepcopy
import re
import time


class JGGeneratorStats(ExecStats):
    """
    Statistics gathered during mining
    """
    TIMERS = {'jg_enumeration',
              'jg_validtaion',
              'jg_hashing'
              }

    PARAMS = {'number_of_jgs',
              'valid_jgs',
              'valid_jgs_cost_high'
             }


logger = logging.getLogger(__name__)



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

    def __init__(self, schema_graph, attr_dict, gwrapper):
        self.schema_graph = schema_graph
        self.hash_jg_table = {} # a hash dictionary that used to check duplicates
        self.attr_dict = attr_dict
        self.stats = JGGeneratorStats()
        self.gwrapper = gwrapper

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
            #logger.debug(n.label) ####
            #logger.debug(node1_label) ####
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
        # logger.debug(plans_edge_w_new_node) ####
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

        for jn in j_graph_target.graph_core:
            #logger.debug(jn) ####
            if(jn.label=='PT'):
                for pt_n in pt_rels:
                    for n in self.schema_graph:
                        if(self.schema_graph.has_edge(pt_n, n)):
                            for cond in [x for x in self.schema_graph.get_edge_data(pt_n, n).values()]:
                                # replace pt_n.label with "PT"
                                # cond_str = cond['condition'].replace(f'{pt_n}.', 'PT.')
                                cond_str = re.sub(f'(?<!_){pt_n}\.', 'PT.', cond['condition'])
                                #logger.debug(j_graph_target) ####
                                edge_only_plans, edges_w_node_plans = self.add_one_edge(j_graph_target, 'PT', n, [cond_str, cond['key_dict'], pt_n])
                                j_graph_creation_plans['edges_only'].extend(edge_only_plans)
                                j_graph_creation_plans['edges_w_node'].extend(edges_w_node_plans)
            else:
                for n in self.schema_graph:
                    if(self.schema_graph.has_edge(jn.label,n)):
                        for cond in [ x for x in self.schema_graph.get_edge_data(jn.label,n).values()]:
                            edge_only_plans, edges_w_node_plans  = self.add_one_edge(j_graph_target, jn.label, n, [cond['condition'], cond['key_dict'], None])
                            j_graph_creation_plans['edges_only'].extend(edge_only_plans)
                            j_graph_creation_plans['edges_w_node'].extend(edges_w_node_plans)

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

        #logger.debug(new_jgs) ####
        return jg_cur_number, new_jgs
    
    def genprev(self, pt_rels, generated_jg_set, cur_edge, prev_jg_set, jg_cur_number, valid_jgs,a):
        return self.gen(pt_rels, generated_jg_set, cur_edge, prev_jg_set, jg_cur_number, valid_jgs,a)
    
    global count_edge

    def gen(self, pt_rels, generated_jg_set, cur_edge, prev_jg_set, jg_cur_number, valid_jgs,a):
        print("*******cur_edge:",cur_edge)
        if cur_edge==0 and cur_edge>=2:
            self.stats.startTimer('jg_enumeration')
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
                print("*******cur_edge:",cur_edge,"***a:",a)
                new_jgs=[]
                print('#####valid jg:', valid_jgs)
                #logger.debug(repr(valid_jgs[a-1]))
                jg_cur_number, new_generated_jgs = self.gen_new_jg(valid_jgs[a-1], pt_rels, jg_cur_number)
                new_jgs.extend(new_generated_jgs)
            else:
                new_jgs = []
                for t in prev_jg_set:
                    logger.debug(t) ####
                    jg_cur_number, new_generated_jgs = self.gen_new_jg(t, pt_rels, jg_cur_number)
                    new_jgs.extend(new_generated_jgs)
            prev_jg_set = new_jgs
            #if cur_edge != 1:
            generated_jg_set.clear() ####
            generated_jg_set.extend(new_jgs)
        
        #if (cur_edge>1):
        self.stats.stopTimer('jg_enumeration')            
        self.stats.params['number_of_jgs']+=len(generated_jg_set)
        self.stats.startTimer('jg_hashing')
        
        jg_hash_table = self.hash_jgs(generated_jg_set)
        self.stats.stopTimer('jg_hashing')
        #valid_jgs = []
        valid_jgs.clear()

        self.stats.startTimer('jg_validtaion')
        for n in jg_hash_table:
            if(self.valid_check(n, pt_rels)==True):
                valid_jgs.append(n)
        self.stats.stopTimer('jg_validtaion')

        valid_jgs.sort(key=lambda j: j.jg_number)
        ###@@@
        return_val=0
            
        print("<<Enter number (Stop:0|Previous step:-1)>>: ")
        print('valid jg:', valid_jgs)
        for i in range(0, len(valid_jgs)):
            print("[",i+1,"]",repr(valid_jgs[i]))
        a = int(input())
        if a==0:
            #break
            return valid_jgs
        elif a==-1 and cur_edge>1:
            ##cur_edge-=1
            #generated_jg_set.clear()
            global count_edge
            count_edge=cur_edge-2
            print("^^^^-1*****count_edge: ", count_edge,"******cur_edge: ", cur_edge)
            return -1
        else:
            print("<<SELECTED JG>>: ", repr(valid_jgs[a-1]))
            print('******************valid jg******************:', valid_jgs)
            cur_edge+=1
            jg_hash_table.clear()

            #while True:
            return_val = self.gen(pt_rels, generated_jg_set, cur_edge, prev_jg_set, jg_cur_number, valid_jgs,a)
            if return_val != -1:
                valid_jgs=return_val
                print('1@@@@@@@@@@@@@@@@@@@@valid jg:', valid_jgs)
                #break
            elif return_val == -1 and count_edge!=cur_edge:
                print("****-1")
                print("****-1*****count_edge: ", count_edge,"******cur_edge: ", cur_edge)
                print('2@@@@@@@@@@@@@@@@@@@@valid jg:', valid_jgs)
                return -1
                # print("<<Enter number (Stop:0|Previous step:-1)>>: ")
                # print('valid jg:', valid_jgs)
                # for i in range(0, len(valid_jgs)):
                #     print("[",i+1,"]",repr(valid_jgs[i]))
                # a = int(input())
            else:
                generated_jg_set.clear() 
                print("####-1*****count_edge: ", count_edge,"******cur_edge: ", cur_edge)
                print('@@@@@@@@@@@@@@@@@@@@valid jg:', valid_jgs)

                # else:
                #     valid_jgs = return_val
        return valid_jgs
    global jgs_selection
    jgs_selection = {}
    def setJGselection(self, cur_edge_tmp, valid_jg_tmp):
        jgs_selection[cur_edge_tmp] = valid_jg_tmp
    def getJGselection(self, cur_edge_tmp):
        return jgs_selection[cur_edge_tmp]
    def Generate_JGs(self, pt_rels, num_edges, customize=False):
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
            # valid_jgs_prev=[]
            # global jgs_selection
            # jgs_selection = {}
            uSelection=0
            cur_edge=0
            #for cur_edge in range(0, num_edges+1):
            # while (cur_edge<=num_edges): #(cur_edge<=3):
            while True:
                #cur_edge, generated_jg_set, prev_jg_set, jg_cur_number = self.gen(pt_rels, generated_jg_set, cur_edge, prev_jg_set, jg_cur_number, valid_jgs,a)
            ##valid_jgs = self.genprev(pt_rels, generated_jg_set, cur_edge, prev_jg_set, jg_cur_number, valid_jgs,a)

                # print("[*******]cur_edge:",cur_edge)
                # print('[@@@@@@@@@@]jgs selection dic: ', jgs_selection)
                # print()
                # if cur_edge==0 and cur_edge>=2:
                #     self.stats.startTimer('jg_enumeration')
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
                    if cur_edge>1:
                        # print("*******cur_edge:",cur_edge,"***a:",a)
                        new_jgs=[]
                        #print('#####valid jg:', valid_jgs)
                        #logger.debug(repr(valid_jgs[a-1]))
                        jg_cur_number, new_generated_jgs = self.gen_new_jg(valid_jgs[uSelection-1], pt_rels, jg_cur_number)
                        new_jgs.extend(new_generated_jgs)
                    else:
                        new_jgs = []
                        for t in prev_jg_set:
                            logger.debug(t) ####
                            jg_cur_number, new_generated_jgs = self.gen_new_jg(t, pt_rels, jg_cur_number)
                            new_jgs.extend(new_generated_jgs)
                    prev_jg_set = new_jgs
                    if cur_edge != 1:
                        generated_jg_set.clear() ####
                    generated_jg_set.extend(new_jgs)
                
                if (cur_edge>=1):
                    self.stats.stopTimer('jg_enumeration')            
                    self.stats.params['number_of_jgs']+=len(generated_jg_set)
                    self.stats.startTimer('jg_hashing')
                    
                    jg_hash_table = self.hash_jgs(generated_jg_set)
                    self.stats.stopTimer('jg_hashing')
                    valid_jgs = []
                    #valid_jgs.clear()

                    self.stats.startTimer('jg_validtaion')
                    for n in jg_hash_table:
                        if(self.valid_check(n, pt_rels)==True):
                            valid_jgs.append(n)
                    self.stats.stopTimer('jg_validtaion')

                    valid_jgs.sort(key=lambda j: j.jg_number)
                    ######################################################################################
                    print("<<Enter number (Stop:0|Previous step:-1)>>: ")
                    # print('valid jg:', valid_jgs)
                    #valid_jgs_prev=valid_jgs
                    
                    #print('valid jg prev:', valid_jgs_prev)
                    
                    uSelection_prev=uSelection
                    for i in range(0, len(valid_jgs)):
                        print("[",i+1,"]",repr(valid_jgs[i]))
                    uSelection = int(input())
                    if uSelection==0:
                        break
                    elif uSelection==-1 and cur_edge>1:
                        #valid_jgs.clear()
                        
                        ######################valid_jgs = jgs_selection[cur_edge-2]
                        valid_jgs = self.getJGselection(cur_edge-2)
                        cur_edge-=3
                        uSelection=uSelection_prev
                        
                        #valid_jgs=valid_jgs_prev.copy()
                        # print('\n@@@cur_edge -2:', cur_edge-2)
                        # print('\n@@@valid jg:', valid_jgs)
                        #print('@@@valid jg prev:', valid_jgs_prev)
                        generated_jg_set.clear()
                        print("<<Go back to Previous Step>>")
                        #print("*******cur_edge:",cur_edge,"***a:",a)
                    else:
                        print("<<SELECTED JG>>: ", repr(valid_jgs[uSelection-1]))
                        # print("<<cur edge>>: ", cur_edge)
                       
                        # print('@@@@@@@@@@jgs selection dic: ', jgs_selection)
                        #############jgs_selection[cur_edge] = valid_jgs #valid_jgs[a-1]
                        self.setJGselection(cur_edge, valid_jgs)
                        # print('jgs selection dic: ', jgs_selection)
                    
                        # print('&&valid jg:', valid_jgs)
                        #valid_jgs_prev.clear()
                        #valid_jgs_prev.append(repr(valid_jgs[a-1]))
                        #valid_jgs_prev=valid_jgs.copy()                        
                        #print('&&valid jg prev:', valid_jgs_prev)
                    jg_hash_table.clear() #####
                    ##########################################################################################
                    # prev_jg_set.clear()
                    # prev_jg_set.append(repr(valid_jgs[a-1]))
                cur_edge+=1
                #print('[######]jgs selection dic: ', jgs_selection)
            return valid_jgs
        else:
            pass

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

            # self.stats.startTimer('jg_validtaion')
            # for n in jg_hash_table:
            #     if(self.valid_check(n, pt_rels)==True):
            #         valid_jgs.append(n)
            # self.stats.stopTimer('jg_validtaion')

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