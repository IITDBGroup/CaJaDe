import pandas as pd
import logging
import math
import re
import itertools
import random
import time
from src.instrumentation import ExecStats
from statistics import mean
from varclushi import VarClusHi
import numpy as np
from scipy.stats import entropy
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder
from copy import deepcopy
import datetime
import heapq
from operator import itemgetter


logger = logging.getLogger(__name__)

class PatternGeneratorStats(ExecStats):
    """
    Statistics gathered during mining
    """

    TIMERS = {'LCA',
              'refinment',
              'check_recall',
              'run_F1_query',
              'feature_reduct',
              'pattern_recover',
              'rank_patterns',
              'f1_sample',
              'per_jg_timer'
              }

    PARAMS = {'n_p_pass_node_rule_and_recall_thresh',
              'eval_pat_cnt',
              'pat_misclass_cnt',
              # use this only when you want to measure effectiveness of sampling on fscore
              'error_sum_of_squares'
              # use this only when you want to measure effectiveness of sampling on fscore
              }



class Pattern_Generator:

    """
    Pattern enumerator & Quality measure once we have a apt query result/ sampled query result
    """
    def __init__(self, conn):
        self.conn = conn
        self.cur = self.conn.cursor()
        self.stats = PatternGeneratorStats()
        self.pattern_pool = []
        self.pattern_by_jg = {}
        self.weighted_sample_views = {} # key as the pattern structure, value as the name of the view

    def pattern_recover(self, renaming_dict, pending_pattern, user_questions_map):

        pending_pattern['desc'] = []
        pending_pattern['tokens'] = {}

        for nt in pending_pattern['nominal_values']:
            re_a_index = int(re.findall(r'([0-9]+)', nt[0])[0])
            for k_n, v_n in renaming_dict.items():
                if(k_n=='max_rel_index' or k_n=='max_attr_index' or k_n=='dtypes'):
                    continue
                else:
                    if(re_a_index>=v_n['rel_min_attr_index'] and re_a_index<=v_n['rel_max_attr_index']):
                        pending_pattern['desc'].append(f"{v_n['label']}_{k_n}.{renaming_dict[k_n]['columns'][nt[0]]}={nt[1]}")
                        pending_pattern['tokens'][f"{v_n['label']}_{k_n}.{renaming_dict[k_n]['columns'][nt[0]]}"]=f"{nt[1]}"
                        break

        if('ordinal_values' in pending_pattern):
            for ot in pending_pattern['ordinal_values']:
                re_a_index = int(re.findall(r'([0-9]+)', ot[0])[0])
                for k_o, v_o in renaming_dict.items():
                    if(k_o=='max_rel_index' or k_o=='max_attr_index' or k_o=='dtypes'):
                        continue
                    else:
                        if(re_a_index>=v_o['rel_min_attr_index'] and re_a_index<=v_o['rel_max_attr_index']):
                            pending_pattern['desc'].append(f"{v_o['label']}_{k_o}.{renaming_dict[k_o]['columns'][ot[0]]}{ot[1]}{ot[2]}")
                            pending_pattern['tokens'][f"{v_o['label']}_{k_o}.{renaming_dict[k_o]['columns'][ot[0]]}"]=f"{ot[2]}"
                            break

        if('correlated_attrs' in pending_pattern and bool(pending_pattern['correlated_attrs'])):
            correlated_attrs_recover_dict = {}
            for k,v in pending_pattern['correlated_attrs'].items():
                re_a_index = int(re.findall(r'([0-9]+)', k)[0])
                for k_r, v_r in renaming_dict.items():
                    if(k_r=='max_rel_index' or k_r=='max_attr_index' or k_r=='dtypes'):
                        continue
                    else:
                        if(re_a_index>=v_r['rel_min_attr_index'] and re_a_index<=v_r['rel_max_attr_index']):
                            correlated_attrs_recover_dict[renaming_dict[k_r]['columns'][k]] = v
                            break
            # logger.debug(correlated_attrs_recover_dict)

            for used_col, cor_cols in correlated_attrs_recover_dict.items():
                for i in range(len(cor_cols)):
                    re_a_index = int(re.findall(r'([0-9]+)', cor_cols[i])[0])
                    for k_rr, v_rr in renaming_dict.items():
                        if(k_rr=='max_rel_index' or k_rr=='max_attr_index' or k_rr=='dtypes'):
                            continue
                        else:
                            if(re_a_index>=v_rr['rel_min_attr_index'] and re_a_index<=v_rr['rel_max_attr_index']):
                                correlated_attrs_recover_dict[used_col][i] = renaming_dict[k_rr]['columns'][cor_cols[i]]
                                break

            pending_pattern['correlated_attrs'] = correlated_attrs_recover_dict

        pending_pattern['desc'] = ','.join(sorted(pending_pattern['desc']))
        pending_pattern['num_edges'] = pending_pattern['join_graph'].num_edges
        pending_pattern['is_user'] = user_questions_map[pending_pattern['is_user']]
        # logger.debug(pending_pattern)


        return pending_pattern

    def topk_jg_patterns(self, num_jg=3, k_p=5, sortby='avg'):
        """
        return patterns from top num_jg jgs with 
        top k patterns from each jg

        sortby options: 'avg', 'entropy', 'stddv'
        """
        res = []

        if(sortby=='avg'):
            avg_list = []
            for k,v in self.pattern_by_jg.items():
                if(bool(self.pattern_by_jg[k])):
                    avg_list.append((k,mean([x['F1'] for x in self.pattern_by_jg[k]])))

            avg_list.sort(key = lambda x: x[1], reverse = True)

            first_num_jgs = [j[0] for j in avg_list[0:num_jg]]


        elif(sortby=='entropy'):

            jg_entropy_list = []

            for j in self.pattern_by_jg:
                value,counts = np.unique([f['F1'] for f in self.pattern_by_jg[j]], return_counts=True)
                jg_entropy_list.append((j,entropy(counts)))
            
            jg_entropy_list.sort(key = lambda p: p[1], reverse = True)
            first_num_jgs = [j[0] for j in jg_entropy_list[0:num_jg]]


        for jg in first_num_jgs:
            self.pattern_by_jg[jg].sort(key = lambda p: p['F1'], reverse = True)
            res.extend(self.pattern_by_jg[jg][0:k_p])

        return res

    def pattern_diversification(self, 
                            pattern_pool,
                            k=5, 
                            same_attr_weight=-0.3, # punish patterns sharing same attribute
                            same_val_weight=-2, # punish same attr and same value,
                            ):
        # record patterns' info as new patterns being added and everytime a new pattern 
        # comes in, we iteratively compute its similarities between itself and the previously
        # added patterns.

        res = []

        res.append(pattern_pool[0])
        # logger.debug(f"diversification : {pattern_pool[0]['join_graph']}")

        num_patterns = 1
        # add the highest ranked pattern in terms
        # of fscore as the starting point 

        while(num_patterns<k and len(pattern_pool)>=k):
            for i in range(len(pattern_pool)):
                pat = pattern_pool[i]
                scores = [] 
                for r in res:
                    repeated_attrs = []
                    diff_attrs = []

                    for pk,pv in pat['tokens'].items():
                        if(pk in r['tokens']):
                            if(pv == r['tokens'][pk]):
                                repeated_attrs.append(same_val_weight)
                            else:
                                repeated_attrs.append(same_attr_weight)
                        else:
                            diff_attrs.append(1)

                    scores.append(pat['F1']+(sum(repeated_attrs)+sum(diff_attrs))/len(pat['tokens']))

                pat['diverse_score'] = min(scores)
                # logger.debug(f"min_sim: {min_sim}")

            pat_toadd = sorted(pattern_pool, key=itemgetter('diverse_score'),reverse = True)[0]
            res.append(pat_toadd)
            num_patterns+=1

        return res 

    def rank_patterns(self, ranking_type='global'):
        # organize patterns: either 'global' or 'by_jg'
        # return the ranked pattern (List if global, dict if by_jg)
        if(ranking_type=='global'):
            self.stats.startTimer('rank_patterns') 
            pattern_global_sorted = sorted(self.pattern_pool, key=lambda p: p['F1'], reverse=True)
            self.stats.stopTimer('rank_patterns')
            return pattern_global_sorted
        else:
            for k,v in self.pattern_by_jg.items():
                self.stats.startTimer('rank_patterns') 
                self.pattern_by_jg[k] = sorted(v, key = lambda p: p['F1'], reverse=True)
                self.stats.stopTimer('rank_patterns')
            return self.pattern_by_jg


    def avg_patterns(self, avg_on='recall'):
        if(avg_on=='recall'):
            return round(mean([x['recall'] for x in self.pattern_pool]),2)
        else:
            return round(mean([x['F1'] for x in self.pattern_pool]),2)


    def extend_valid_candidates(self, 
                                patterns_to_extend, 
                                numerical_variable_candidates,
                                numerical_quartiles,
                                correlation_dict):

        candidates_to_return = []

        # logger.debug(patterns_to_extend)
        # logger.debug(numerical_variable_candidates)
        # logger.debug(numerical_quartiles)
        for pte in patterns_to_extend:
            for n in numerical_variable_candidates:
                if(n[0]>pte['max_cluster_rank']):
                    for val in numerical_quartiles[n[1]]:
                        for one_dir in ['>','<']:
                            new_ordinal_values = deepcopy(pte['ordinal_values'])
                            new_ordinal_values.append((n[1],one_dir,val))
                            new_correlated_attrs = deepcopy(pte['correlated_attrs'])
                            new_correlated_attrs[n[1]] = deepcopy(correlation_dict[n[1]])  
                            is_user=deepcopy(pte['is_user'])
                            # one_patt_with_ordi_dir_yes 
                            candidates_to_return.append({'join_graph':pte['join_graph'], 'recall':0, 'precision':0, 
                                'nominal_values': deepcopy(pte['nominal_values']), 
                                'ordinal_values': new_ordinal_values,
                                'correlated_attrs': new_correlated_attrs,
                                'max_cluster_rank': n[0],                                            
                                'is_user': is_user})
        # logger.debug(candidates_to_return)
        return candidates_to_return

    def match_fit_weighted_sample(self, pattern, jg_name, stddev_ranks_dict, w_sample_attr, sample_size):
        """
        only used when we want to do weighted sampling

        rule: 1: if found out pattern only have nominal values
                 return {jg_name}_ws_nom only
              2: else rank numerical/ordinal attributes based on 
                 stddev_ranks and return sample name based on highest 
                 stddev from self.weighted_sample_views[jg_name]
              3: if no appropriate jg sample can be found, then create
                 a view and then return
        """
        # logger.debug(pattern)
        if(not pattern['ordinal_values']):
            # if no numeric attribute is involved
            return self.weighted_sample_views[jg_name]['nominal_only_recall'], f"{jg_name}_ws_nom"
        else:
            pat_ordi_vals = [ov[0] for ov in pattern['ordinal_values']]
            # logger.debug(pat_ordi_vals)
            # logger.debug(stddev_ranks_dict)
            highest_rank_val_info = sorted([[stddev_ranks_dict[p], p] for p in pat_ordi_vals], key = lambda x: x[0])[0]

            highest_rank_num = highest_rank_val_info[0]
            highest_rank_attr = highest_rank_val_info[1]

            if(self.weighted_sample_views[jg_name]['jg_samples']):
                if(self.weighted_sample_views[jg_name]['jg_samples'][0][0]<=highest_rank_num):
                    ret_sample_jg = self.weighted_sample_views[jg_name]['jg_samples'][0]
                    return ret_sample_jg[3], ret_sample_jg[2]

            view_number = self.weighted_sample_views[jg_name]['ws_index']

            self.stats.startTimer('f1_sample')

            new_ws = f"{jg_name}_ws_{view_number}"
            drop_jg_ws_q = f"""
            DROP MATERIALIZED VIEW IF EXISTS {new_ws} CASCADE
            """
            self.cur.execute(drop_jg_ws_q)

            jg_new_ws_q = f"""
            CREATE MATERIALIZED VIEW {new_ws} AS
            WITH weights AS
            (SELECT {w_sample_attr}, COUNT(*) AS cnt, 
            stddev({highest_rank_attr})::numeric/avg({highest_rank_attr}) AS sdv
            FROM {jg_name}
            GROUP BY {w_sample_attr}
            )
            SELECT j.* 
            FROM {jg_name} j, weights w
            WHERE j.{w_sample_attr} = w.{w_sample_attr}
            ORDER BY RANDOM() * w.cnt * w.sdv DESC
            LIMIT {sample_size}; 
            """
            # logger.debug(jg_new_ws_q)

            self.cur.execute(jg_new_ws_q)

            self.stats.stopTimer('f1_sample')

            self.weighted_sample_views[jg_name]['ws_index']+=1

            sample_recall_dict = {}

            q_tp_yes = f"""
            SELECT COUNT(DISTINCT pnumber)
            FROM {new_ws}
            WHERE is_user='yes'
            """
            self.cur.execute(q_tp_yes)
            sample_recall_dict['yes'] = int(self.cur.fetchone()[0])

            q_tp_no = f"""
            SELECT COUNT(DISTINCT pnumber)
            FROM {new_ws}
            WHERE is_user='no'
            """

            self.cur.execute(q_tp_no)
            sample_recall_dict['no'] = int(self.cur.fetchone()[0])

            highest_rank_val_info.append(new_ws)
            highest_rank_val_info.append(sample_recall_dict)

            heapq.heappush(self.weighted_sample_views[jg_name]['jg_samples'], highest_rank_val_info)


            return sample_recall_dict, new_ws



    
    def get_fscore(self,
                   prov_version,
                   f1_calculation_type,
                   pattern,
                   pattern_recall_threshold,
                   jg_name,
                   recall_dicts,
                   need_weighted_sampling,
                   w_sample_attr=None,
                   stddv_ranks_dict=None,
                   sample_size=0):

        # use to keep the true positive value here since 
        # true postive for recall will always be the same
        pattern['jg_name'] = jg_name

        if('sample' not in recall_dicts and not need_weighted_sampling):
            f1_calculation_type='o'
        # logger.debug(pattern)

        where_cond_list = []
        if('ordinal_values' in pattern):
            for ot in pattern['ordinal_values']:
                where_cond_list.append(f"{ot[0]}{ot[1]}{ot[2]}")
            for nt in pattern['nominal_values']:
                if(isinstance(nt[1], datetime.date)):
                    nt[1] = nt[1].strftime('%Y-%m-%d')
                n_value = nt[1].replace("'","''")
                where_cond_list.append(f"{nt[0]}='{n_value}'")
        else:
            for nt in pattern['nominal_values']:
                n_value = nt[1].replace("'","''")
                where_cond_list.append(f"{nt[0]}='{n_value}'")

        pattern_conditions = " AND ".join(where_cond_list)
        where_cond_list.append(f"is_user='{pattern['is_user']}'")
        true_positive_conditions = " AND ".join(where_cond_list)

        if(prov_version=='fractional'):
            F1_q = f"""
            SELECT 
            2*
            ROUND
            (
                (
                    (
                        (
                        SELECT SUM(prov_value) 
                        FROM {jg_name}_d
                        WHERE {true_positive_conditions}
                        )::NUMERIC
                        /
                        (
                        SELECT SUM(prov_value) 
                        FROM {jg_name}_d
                        WHERE {pattern_conditions}
                        )::NUMERIC
                    )
                    *
                    (
                        (
                        SELECT SUM(prov_value) 
                        FROM {jg_name}_d
                        WHERE {true_positive_conditions}
                        )::NUMERIC
                        /
                        (
                        SELECT SUM(prov_value) 
                        FROM {jg_name}_d
                        WHERE is_user='{pattern['is_user']}'
                        )::NUMERIC
                    )
                )
                /
                (
                    (
                        (
                        SELECT SUM(prov_value) 
                        FROM {jg_name}_d
                        WHERE {true_positive_conditions}
                        )::NUMERIC
                        /
                        (
                        SELECT SUM(prov_value) 
                        FROM {jg_name}_d
                        WHERE {pattern_conditions}
                        )::NUMERIC
                    )
                    +
                    (
                        (
                        SELECT SUM(prov_value) 
                        FROM {jg_name}_d
                        WHERE {true_positive_conditions}
                        )::NUMERIC
                        /
                        (
                        SELECT SUM(prov_value) 
                        FROM {jg_name}_d
                        WHERE is_user='{pattern['is_user']}'
                        )::NUMERIC
                    )
                )
            ,5
            )
            """          
        else:
            if(f1_calculation_type!='s'):
                check_original_recall_threshold_q = f"""
                SELECT ROUND
                (
                  (
                  SELECT COUNT(DISTINCT pnumber) 
                  FROM {jg_name}
                  WHERE {true_positive_conditions}
                  )::NUMERIC
                  /
                  NULLIF(
                  {recall_dicts['original']['no'] if pattern['is_user']=='no' else recall_dicts['original']['yes']}
                  ,0)
                ,5
                );
                """
            # if(need_weighted_sampling == True):

            if(f1_calculation_type=='e' or f1_calculation_type=='s'):
                if(need_weighted_sampling==True):
                    weighted_s_dict, jg_sample_name = self.match_fit_weighted_sample(pattern, jg_name, stddv_ranks_dict, w_sample_attr, sample_size)
                    all_p = weighted_s_dict['no'] if pattern['is_user']=='no' else weighted_s_dict['yes']
                else:
                    all_p = recall_dicts['sample']['no'] if pattern['is_user']=='no' else recall_dicts['sample']['yes']
                    jg_sample_name = f"{jg_name}_fs"

                check_sample_recall_threshold_q = f"""
                SELECT ROUND
                (
                  (
                  SELECT COUNT(DISTINCT pnumber) 
                  FROM {jg_sample_name}
                  WHERE {true_positive_conditions}
                  )::NUMERIC
                  /
                  NULLIF(
                  {all_p}
                  ,0)
                ,5
                );
                """

            if(f1_calculation_type=='o' or f1_calculation_type=='e'):
                self.stats.startTimer('check_recall')
                self.cur.execute(check_original_recall_threshold_q)
                original_recall_result = self.cur.fetchone()[0]
                if(original_recall_result is None):
                    original_recall_result = 0
                else:
                    original_recall_result = float(original_recall_result)
                self.stats.stopTimer('check_recall')

            if(f1_calculation_type=='e' or f1_calculation_type=='s'):
                self.stats.startTimer('check_recall')
                self.cur.execute(check_sample_recall_threshold_q)
                sample_recall_result = self.cur.fetchone()[0]
                if(sample_recall_result is None):
                    sample_recall_result = 0
                else:
                    sample_recall_result = float(sample_recall_result)
                pattern['sample_recall'] = sample_recall_result
                self.stats.stopTimer('check_recall')

            if(f1_calculation_type=='e'):
                # check if missclassify
                self.stats.params['eval_pat_cnt']+=1
                if((original_recall_result<pattern_recall_threshold and sample_recall_result>pattern_recall_threshold)\
                    or (original_recall_result>pattern_recall_threshold and sample_recall_result<pattern_recall_threshold)):
                    self.stats.params['eval_pat_cnt']+=1
                self.stats.params['error_sum_of_squares'] += (original_recall_result-sample_recall_result)**2
            
            if(f1_calculation_type=='o' or f1_calculation_type=='e'):
                recall_result=original_recall_result
            else:
                recall_result=sample_recall_result

            if(recall_result<=pattern_recall_threshold and f1_calculation_type!='e'):
                return pattern, False
            else:
                pattern['recall'] = recall_result
                
                if(f1_calculation_type!='o'):
                    sample_F1_q = f"""
                    WITH precision AS 
                    (SELECT 
                        (
                        SELECT COUNT(DISTINCT pnumber)
                        FROM {jg_sample_name}
                        WHERE {true_positive_conditions}
                        )::NUMERIC
                        /
                        NULLIF(
                        (
                        SELECT SUM(pnumber_sum) FROM 
                        (
                        SELECT COUNT(DISTINCT pnumber) AS pnumber_sum
                        FROM {jg_sample_name}
                        WHERE {pattern_conditions}
                        GROUP BY is_user
                        ) AS FP_AND_TR
                        )::NUMERIC,0) AS prec
                    )
                    SELECT  
                    p.prec,
                    2*
                    ROUND
                    (
                        (
                            (p.prec)
                            * {sample_recall_result}
                        )
                        /
                        NULLIF(
                        ( 
                            (p.prec) + {sample_recall_result}
                        ),0),5
                    ) AS f1
                    from precision p
                    """


                F1_q = f"""
                WITH precision AS 
                (SELECT 
                    (
                    SELECT COUNT(DISTINCT pnumber)
                    FROM {jg_name}
                    WHERE {true_positive_conditions}
                    )::NUMERIC
                    /
                    NULLIF(
                    (
                    SELECT SUM(pnumber_sum) FROM 
                    (
                    SELECT COUNT(DISTINCT pnumber) AS pnumber_sum
                    FROM {jg_name}
                    WHERE {pattern_conditions}
                    GROUP BY is_user
                    ) AS FP_AND_TR
                    )::NUMERIC,0) AS prec
                )
                SELECT  
                p.prec,
                2*
                ROUND
                (
                    (
                        (p.prec)
                        * {recall_result}
                    )
                    /
                    NULLIF(
                    ( 
                        (p.prec) + {recall_result}
                    ),0),5
                ) AS f1
                from precision p
                """

            self.stats.startTimer('run_F1_query')
            
            # logger.debug(F1_q)
            if(f1_calculation_type == 's'):
                F1_q = sample_F1_q

            self.cur.execute(F1_q)
            result = self.cur.fetchone()
            self.stats.stopTimer('run_F1_query')
            prec,f1 = result[0], result[1]

            if(f1 is None):
                pattern['F1'] = 0
            else:
                pattern['F1'] = float(f1)

            if(prec is None):
                pattern['precision'] = 0
            else:
                pattern['precision'] = float(prec)

            if(f1_calculation_type!='e'):
                return pattern, True
            else:
                self.stats.startTimer('run_F1_query')
                # logger.debug(F1_q)
                self.cur.execute(sample_F1_q)
                sample_result = self.cur.fetchone()
                self.stats.stopTimer('run_F1_query')

                sample_prec,sample_f1 = sample_result[0], sample_result[1]

                if(sample_f1 is None):
                    pattern['sample_F1'] = 0
                else:
                    pattern['sample_F1'] = float(sample_f1)

                if(sample_prec is None):
                    pattern['sample_precision'] = 0
                else:
                    pattern['sample_precision'] = float(sample_prec)

                return pattern, True


    def gen_patterns(self, 
                      jg, 
                      jg_name, 
                      renaming_dict, 
                      skip_cols,
                      user_questions_map,
                      user_pt_size,
                      original_pt_size,
                      attr_alias='a',
                      prov_version='existential',
                      s_rate_for_s=0.1,
                      lca_s_max_size = 1000,
                      lca_s_min_size = 100,
                      lca_recall_thresh = 0.3,
                      just_lca = False,
                      pattern_recall_threshold=0, 
                      numercial_attr_filter_method = 'y',
                      sample_repeatable = False,
                      f1_sample_type = 'weighted',
                      seed = 0.5,
                      f1_calculation_type = 'o',
                      f1_calculation_sample_rate = 0.3,
                      f1_calculation_min_size = 100,
                      user_assigned_num_pred_cap = 3,
                      num_numerical_attr_rate = 1.5,
                      ):

        """
        jg_name: view created from jg_generator
        
        skip_cols: renamed keys will be ignored
        
        attr_alias: used to further ignore colmns from user query in provenance
        
        s_rate_for_s: sample rate for s: this is used for LCA, usually very small compared to d size 
        
        prov_version: choose from ['existential', 'fractional']
        
        numercial_attr_filter_method: 'varclus', 'rf', 'none'

        user_assigned_num_pred_cap: maximum number of numerical attributes allowed.
        doesnt need to meet this number: (could be filtered by recall thresh or invalidated by
        max number of clusters)

        num_numerical_attr_rate: if filter method is 'varclus', after random forest, how 
        many important features will be considered? it is equal to user_assigned_num_pred_cap*num_numerical_attr_rate
        """

        self.pattern_by_jg[jg] = []

        need_weighted_sampling  = False

        recall_dicts = {}

        sorted_stddv_dct = None
        # a dictionary of dictionary which could
        # store up to 2 dictionaries, 1 for "original" f1 version
        # the other one for "sample" f1 version
        # this could happen if f1_calculation_type='e'

        jg_size_q = f"SELECT count(*) as size FROM {jg_name}";
        self.cur.execute(jg_size_q)
        jg_apt_size = int(self.cur.fetchone()[0])

        # logger.debug(renaming_dict)

        sample_f1_jg_size=0
        if(jg_apt_size!=0):
            # based on f1 calcuation type set up recall dict and materialized apt to evaluate f1
            if(f1_calculation_type=='s' or f1_calculation_type=='e'):
                if(jg_apt_size>f1_calculation_min_size): # this decides that we need sampling
                    # logger.debug(f"jg_apt_size: {jg_apt_size}")
                    sample_f1_jg_size = math.ceil(jg_apt_size * f1_calculation_sample_rate)
                    if(jg_apt_size <= user_pt_size or f1_sample_type!='weighted'): # this means if we need weighted sampling
                        # logger.debug('uniform sampling!')
                        sample_recall_dict = {}
                        
                        self.stats.startTimer('f1_sample')

                        # logger.debug(f"jg_apt_size : {jg_apt_size}")
                        # logger.debug(f"sample_f1_jg_size")

                        drop_f1_jg = f"""
                        DROP MATERIALIZED VIEW IF EXISTS {jg_name}_fs CASCADE
                        """
                        self.cur.execute(drop_f1_jg)
                        create_f1_jg_size = f"""
                        CREATE MATERIALIZED VIEW {jg_name}_fs AS 
                        (
                        SELECT * FROM {jg_name}
                        ORDER BY RANDOM()
                        LIMIT {sample_f1_jg_size}
                        )
                        """
                        self.cur.execute(create_f1_jg_size)
                    
                        q_tp_yes = f"""
                        SELECT COUNT(DISTINCT pnumber)
                        FROM {jg_name}_fs
                        WHERE is_user='yes'
                        """
                        self.cur.execute(q_tp_yes)
                        sample_recall_dict['yes'] = int(self.cur.fetchone()[0])

                        q_tp_no = f"""
                        SELECT COUNT(DISTINCT pnumber)
                        FROM {jg_name}_fs
                        WHERE is_user='no'
                        """
                        self.cur.execute(q_tp_no)
                        sample_recall_dict['no'] = int(self.cur.fetchone()[0])

                        self.stats.stopTimer('f1_sample')

                        recall_dicts['sample']=sample_recall_dict
                        # logger.debug(recall_dicts)
                    
                    else:
                        # logger.debug('weighted sampling!')
                        recall_dicts['sample'] = {}
                        need_weighted_sampling = True

                else:
                    sample_recall_dict = {}
                    
                    self.stats.startTimer('f1_sample')

                    sample_f1_jg_size = math.ceil(jg_apt_size * f1_calculation_sample_rate)

                    drop_f1_jg = f"""
                    DROP MATERIALIZED VIEW IF EXISTS {jg_name}_fs CASCADE
                    """
                    self.cur.execute(drop_f1_jg)
                    create_f1_jg_size = f"""
                    CREATE MATERIALIZED VIEW {jg_name}_fs AS 
                    (
                    SELECT * FROM {jg_name}
                    )
                    """
                    self.cur.execute(create_f1_jg_size)
                
                    q_tp_yes = f"""
                    SELECT COUNT(DISTINCT pnumber)
                    FROM {jg_name}_fs
                    WHERE is_user='yes'
                    """
                    self.cur.execute(q_tp_yes)
                    sample_recall_dict['yes'] = int(self.cur.fetchone()[0])

                    q_tp_no = f"""
                    SELECT COUNT(DISTINCT pnumber)
                    FROM {jg_name}_fs
                    WHERE is_user='no'
                    """
                    self.cur.execute(q_tp_no)
                    sample_recall_dict['no'] = int(self.cur.fetchone()[0])

                    self.stats.stopTimer('f1_sample')

                    recall_dicts['sample']=sample_recall_dict


            original_recall_dict={}

            q_tp_yes = f"""
            SELECT COUNT(DISTINCT pnumber)
            FROM {jg_name}
            WHERE is_user='yes'
            """
            self.cur.execute(q_tp_yes)
            original_recall_dict['yes'] = int(self.cur.fetchone()[0])

            q_tp_no = f"""
            SELECT COUNT(DISTINCT pnumber)
            FROM {jg_name}
            WHERE is_user='no'
            """
            self.cur.execute(q_tp_no)
            original_recall_dict['no'] = int(self.cur.fetchone()[0])

            recall_dicts['original'] = original_recall_dict


            self.stats.startTimer('LCA')
            attrs_from_spec_node = set([k for k in renaming_dict[jg.spec_node_key]['columns']])

            # logger.debug(f"jg_name: {jg_name}")
            # logger.debug(renaming_dict)

            # logger.debug(skip_cols)
            # logger.debug(attrs_from_spec_node)

            get_attrs_q = f"""
            select atr.attname
            from pg_class mv
              join pg_namespace ns on mv.relnamespace = ns.oid
              join pg_attribute atr 
                on atr.attrelid = mv.oid 
               and atr.attnum > 0 
               and not atr.attisdropped
            where mv.relkind = 'm'
            and mv.relname = '{jg_name}'
            """

            self.cur.execute(get_attrs_q)
            attrs = [x[0] for x in self.cur.fetchall()]

            considered_attrs_s = [x for x in attrs if x not in skip_cols and re.search(r'{}_'.format(attr_alias), x)]
            attrs_in_s = ','.join(considered_attrs_s)

            # logger.debug(f'considered_attrs_s:{considered_attrs_s}')

            considered_attrs_d = [x for x in attrs if (x not in skip_cols and re.search(r'{}_'.format(attr_alias), x)) 
                                  or (x=='is_user' or x=='pnumber')]

            attrs_in_d = ','.join(considered_attrs_d) 

            drop_prov_d = f"DROP MATERIALIZED VIEW IF EXISTS {jg_name}_d CASCADE;"
            # logger.debug(drop_prov_d)
            self.cur.execute(drop_prov_d)

            drop_prov_s = f"DROP MATERIALIZED VIEW IF EXISTS {jg_name}_s CASCADE;"
            # logger.debug(drop_prov_s)
            self.cur.execute(drop_prov_s)

            lca_sample_size = max([min(math.ceil(original_pt_size*s_rate_for_s), lca_s_max_size), lca_s_min_size])
            # logger.debug(f"sample size : {lca_sample_size}")

            if(lca_sample_size!=0):
                # make sure jg result is not empty            
                pattern_cond_attr_list = []
                nominal_pattern_attr_list = []
                ordinal_pattern_attr_list = []

                for attr in considered_attrs_s:
                    if(renaming_dict['dtypes'][attr] == 'nominal'):
                        one_attr_in_pattern = f"CASE WHEN l.{attr} = r.{attr} THEN l.{attr} ELSE NULL END AS {attr}"
                        pattern_cond_attr_list.append(one_attr_in_pattern)
                        nominal_pattern_attr_list.append(attr)
                    else:
                        ordinal_pattern_attr_list.append(attr)

                pattern_q_selection_clause = ",".join(pattern_cond_attr_list)
                nominal_pattern_attr_clause = ",".join(nominal_pattern_attr_list)

                sample_repeatable_clause = None
                w_sample_attr = None

                system_sample_rate = int(s_rate_for_s*100)
                
                setseed_q = f"""
                select setseed({seed})
                """

                if(prov_version=='fractional'): # Dont use this, not working...
                    prov_d_creation_q = f"""
                    CREATE MATERIALIZED VIEW {jg_name}_d AS
                    (
                    WITH d_{jg_name} AS 
                      (
                        SELECT {attrs_in_d}
                        FROM {jg_name}
                        ORDER BY RANDOM()
                        LIMIT {lca_sample_size}
                      ),
                      prov_groups AS
                      (
                      SELECT is_user, pnumber, count(*) AS group_size 
                      FROM d_{jg_name} 
                      GROUP BY is_user,pnumber
                      )
                      SELECT d.*, ROUND(CAST(1 AS NUMERIC)/CAST(pg.group_size AS NUMERIC),5) AS prov_value 
                      FROM d_{jg_name} d, prov_groups pg WHERE d.pnumber = pg.pnumber AND d.is_user=pg.is_user
                    );
                    """
                else:
                    prov_d_creation_q = f"""
                    CREATE MATERIALIZED VIEW {jg_name}_d AS
                    (
                        SELECT {attrs_in_d}
                        FROM {jg_name} 
                        ORDER BY RANDOM()
                        LIMIT {lca_sample_size} 
                    );
                """

                if(sample_repeatable):
                    self.cur.execute(setseed_q)
                self.cur.execute(prov_d_creation_q)

                prov_s_creation_q = f"""
                CREATE MATERIALIZED VIEW {jg_name}_s AS
                (
                    SELECT {attrs_in_d}
                    FROM {jg_name} 
                    ORDER BY RANDOM()
                    LIMIT {lca_sample_size} 
                );
                """
                if(sample_repeatable):
                    self.cur.execute(setseed_q)

                self.cur.execute(prov_s_creation_q)

                pattern_creation_q = f"""
                CREATE MATERIALIZED VIEW {jg_name}_p AS
                WITH cp AS
                (
                SELECT 
                {pattern_q_selection_clause}
                FROM {jg_name}_d l, {jg_name}_s r
                )
                SELECT COUNT(*) AS pattern_freq, 
                {nominal_pattern_attr_clause}
                FROM cp
                GROUP BY
                {nominal_pattern_attr_clause}
                ORDER BY pattern_freq DESC
                limit 30;
                """

                # pattern_creation_q = f"""
                # CREATE MATERIALIZED VIEW {jg_name}_p AS
                # WITH cp AS
                # (
                # SELECT 
                # {pattern_q_selection_clause}
                # FROM {jg_name}_d l, {jg_name}_s r
                # )
                # SELECT DISTINCT 
                # {nominal_pattern_attr_clause}
                # FROM cp;
                # """

                get_nominal_patterns_q = f"""
                SELECT {nominal_pattern_attr_clause} FROM {jg_name}_p;
                """

                # limit 10 patterns per jg for now

                # logger.debug(pattern_creation_q)
                self.cur.execute(pattern_creation_q)

                self.stats.stopTimer('LCA')

                nominal_pattern_df = pd.read_sql(get_nominal_patterns_q, self.conn)
                # logger.debug(nominal_pattern_df)

                nominal_pattern_dicts = nominal_pattern_df.to_dict('records')
                # logger.debug(nominal_pattern_dict_listcts)

                nominal_pattern_dict_list = []

                for n_pa in nominal_pattern_dicts:
                    n_pa_dict = {}
                    n_pa_dict['nominal_values'] = [[k, v] for k, v in n_pa.items() if (v is not None and not pd.isnull(v))]
                    if(n_pa_dict['nominal_values']):
                        nominal_pattern_dict_list.append(n_pa_dict)


                # filter based on recall thresh
                distinct_tuple_size = recall_dicts['original']['yes'] + recall_dicts['original']['no']
                for npd in nominal_pattern_dict_list:
                    if(npd['nominal_values']):
                        np_cond_list = []
                        for t in npd['nominal_values']:
                            if(isinstance(t[1], datetime.date)):
                                t[1] = t[1].strftime('%Y-%m-%d')
                            t_val = t[1].replace("'","''")
                            np_cond_list.append(f"{t[0]}='{t_val}'")
                        np_cond = ' AND '.join(np_cond_list)
                        np_recall_query = f"""
                        SELECT COUNT(DISTINCT pnumber)::NUMERIC/{distinct_tuple_size} + 
                        (SELECT COUNT(DISTINCT pnumber)::NUMERIC/{distinct_tuple_size} FROM {jg_name} WHERE {np_cond} 
                        AND is_user='yes') 
                        FROM {jg_name} WHERE {np_cond} AND is_user='no' 
                        """
                        self.stats.startTimer('check_recall')
                        self.cur.execute(np_recall_query)
                        self.stats.stopTimer('check_recall')
                        npd['np_recall'] = float(self.cur.fetchone()[0])
                    else:
                        npd['np_recall'] = 1

                nominal_pattern_dict_list = sorted(nominal_pattern_dict_list, key=lambda d:d['np_recall'], reverse=True)
                # logger.debug(nominal_pattern_dict_list)
                nominal_pattern_dict_list = nominal_pattern_dict_list[0:10]
                # logger.debug(nominal_pattern_dict_list)
                nominal_pattern_dict_list = [n for n in nominal_pattern_dict_list if n['np_recall']>=lca_recall_thresh]
                # logger.debug(nominal_pattern_dict_list)
                # logger.debug(len(nominal_pattern_dict_list))
                
                if(need_weighted_sampling==True): 
                    # if need weighted sampling, we start by sampling for 
                    # nominal patterns only, and we choose most diverse 
                    # one as the weighting factor 

                    self.stats.startTimer('f1_sample')
                    distinct_nominal_list = [f"COUNT(DISTINCT {x}) as cnt_{x}" for x in nominal_pattern_attr_list]
                    nominal_only_sample_q = f""" 
                    SELECT {','.join(distinct_nominal_list)}
                    FROM {jg_name}_p
                    """
                    self.cur.execute(nominal_only_sample_q)
                    distinct_cnts = self.cur.fetchone()
                    max_distinct_val = max(distinct_cnts)
                    i = distinct_cnts.index(max_distinct_val)

                    w_sample_attr = nominal_pattern_attr_list[i]

                    sample_jg_size = math.ceil(jg_apt_size * f1_calculation_sample_rate)

                    drop_w_nom_sample_q = f"""
                    DROP MATERIALIZED VIEW IF EXISTS {jg_name}_ws_nom CASCADE
                    """
                    self.cur.execute(drop_w_nom_sample_q)
                    create_f1_jg_size = f"""
                    CREATE MATERIALIZED VIEW {jg_name}_ws_nom AS 
                    WITH weight_factors AS
                    (SELECT {w_sample_attr} AS name, COUNT(*) AS cnt
                    FROM {jg_name}
                    GROUP BY {w_sample_attr}
                    )
                    SELECT j.* 
                    FROM {jg_name} j, weight_factors wf
                    WHERE j.{w_sample_attr} = wf.name
                    ORDER BY RANDOM() * wf.cnt DESC
                    LIMIT {sample_jg_size};
                    """
                    # logger.debug(create_f1_jg_size)
                    self.cur.execute(create_f1_jg_size)
                    self.stats.stopTimer('f1_sample')

                    nom_sample_dict = {}

                    q_tp_yes = f"""
                    SELECT COUNT(DISTINCT pnumber)
                    FROM {jg_name}_ws_nom
                    WHERE is_user='yes'
                    """
                    self.cur.execute(q_tp_yes)
                    nom_sample_dict['yes'] = int(self.cur.fetchone()[0])

                    q_tp_no = f"""
                    SELECT COUNT(DISTINCT pnumber)
                    FROM {jg_name}_ws_nom
                    WHERE is_user='no'
                    """
                    self.cur.execute(q_tp_no)
                    nom_sample_dict['no'] = int(self.cur.fetchone()[0])


                    self.weighted_sample_views[jg_name] = {'jg_samples': [], 'ws_index':1, 'nominal_only_recall': nom_sample_dict}
 
                valid_patterns = [] # list of all the valid patterns that can be generated from this nominal pattern

                if(numercial_attr_filter_method=='y'):
                    if(ordinal_pattern_attr_list and not just_lca):                        
                        # clustering + random forest to select important attributes 
                        # with a caveat in mind that at least one of the attributes
                        # comes from the "last node" specified in join graph 
                        # (either nominal or ordinal has to be in the patten)

                        ordinal_pattern_attr_list = [x for x in ordinal_pattern_attr_list if x not in jg.user_attrs]

                        self.stats.startTimer('feature_reduct')

                        Q_cor = f"""
                        SELECT {','.join(ordinal_pattern_attr_list)}, is_user 
                        FROM {jg_name} 
                        ORDER BY RANDOM()
                        LIMIT 10000
                        """;

                        if(sample_repeatable):
                            self.cur.execute(setseed_q)

                        raw_df = pd.read_sql(Q_cor, self.conn)
                        cor_df = raw_df[ordinal_pattern_attr_list]
                        rf_input_vars = []
                        rep_from_last_node = []
                        correlation_dict = {}

                        if(len(ordinal_pattern_attr_list)>=3): 
                            # if the number of numeric attributes is not large
                            # then dont need to do clustering 

                            # do clustering first

                            # remove constant
                            cor_df = cor_df.loc[:, (cor_df != cor_df.iloc[0]).any()] 

                            # logger.debug(cor_df.head())
                            variable_clustering = VarClusHi(cor_df, maxeigval2=1, maxclus=None)
                            variable_clustering.varclus()

                            cluster_dict = variable_clustering.rsquare[['Cluster', 'Variable']].groupby('Cluster')['Variable'].apply(list).to_dict()

                            for k,v in cluster_dict.items():
                                cluster_dict[k] = [[x,0,0] for x in v]

                            # logger.debug(cluster_dict)      

                            # entropy rank in each cluster to find the highest one 
                            # as the training input variable "representing" the cluster
                            # 3 elements list for each column, col[1] is for entropy, col[2] is flag indicating
                            # if it is from the  last node
                            # (from last node: 1 not from last node:0)

                            # this is for storing the correlated attrs with representative
                            
                            # random forest input variables
                            # logger.debug(attrs_from_spec_node)
                            for k,v in cluster_dict.items():
                                for col in v:
                                    value,counts = np.unique(cor_df[col[0]], return_counts=True)
                                    col[1] = entropy(counts)
                                    if(col[0] in attrs_from_spec_node):
                                        col[2]=1
                                cluster_dict[k] = sorted(v, key = lambda x: (x[2],x[1],x[0]), reverse=True)
                                representative_var_for_clust = cluster_dict[k][0][0]
                                # logger.debug(representative_var_for_clust)
                                if(representative_var_for_clust in attrs_from_spec_node):
                                    rep_from_last_node.append(representative_var_for_clust)
                                rf_input_vars.append(representative_var_for_clust)
                                correlation_dict[representative_var_for_clust] = [cora[0] for cora in cluster_dict[k][1:]]

                            # logger.debug("lagrge number of num attrs")
                            # logger.debug(rf_input_vars)
                            # logger.debug(correlation_dict)

                        # finish clustering here
                        else:
                            rf_input_vars = [x for x in ordinal_pattern_attr_list if x not in jg.user_attrs] 
                            # logger.debug("smaller number of num attrs!!!!!!")
                            # logger.debug(rf_input_vars)
                            rep_from_last_node = [r for r in rf_input_vars if r in attrs_from_spec_node]
                            correlation_dict = {i : [] for i in rf_input_vars}


                        rf_df = cor_df[rf_input_vars]
                        # logger.debug(rf_df.head())
                        target = raw_df['is_user']
                        le = LabelEncoder()
                        y = le.fit(target)
                        y = le.transform(target)
                        forest = RandomForestClassifier(n_estimators=500, max_depth=10)
                        forest.fit(rf_df, y)
                        importances = [list(t) for t in zip(rf_df, forest.feature_importances_)]
                        importances = sorted(importances, key = lambda x: x[1], reverse=True)
                        # logger.debug(importances)



                        self.stats.stopTimer('feature_reduct')

                        num_feature_to_consider = math.ceil(num_numerical_attr_rate*user_assigned_num_pred_cap)
                        # construct dictionary for each nominal pattern with ordinal attributes
                        # add patterns that only include nominal attributes

                        for npa in nominal_pattern_dict_list:

                            nominal_patterns = []
                            cur_pattern_candidates = [] 

                            # initialize 2 patterns with categorical attrs only
                            nominal_patterns.append({'join_graph':jg, 'recall':0, 'precision':0, 'nominal_values': npa['nominal_values'], 
                                'correlated_attrs': {}, 'attrs_with_const': None, 'ordinal_values':[],
                                'max_cluster_rank':-2, 'is_user':'yes'})

                            nominal_patterns.append({'join_graph':jg, 'recall':0, 'precision':0, 'nominal_values': npa['nominal_values'], 
                                'correlated_attrs': {}, 'attrs_with_const': None, 'ordinal_values':[],
                                'max_cluster_rank':-2, 'is_user':'no'})

                            for n_pat in nominal_patterns:
                                n_pat_processed, is_valid = self.get_fscore(prov_version=prov_version,
                                                                  f1_calculation_type=f1_calculation_type,
                                                                  pattern=n_pat,
                                                                  pattern_recall_threshold=pattern_recall_threshold,
                                                                  jg_name=jg_name,
                                                                  recall_dicts=recall_dicts,
                                                                  need_weighted_sampling=need_weighted_sampling,
                                                                  stddv_ranks_dict = None,
                                                                  w_sample_attr=w_sample_attr,
                                                                  sample_size=sample_f1_jg_size)
                                if(is_valid):
                                    cur_pattern_candidates.append(n_pat_processed)

                            if(cur_pattern_candidates):
                                # if at least one nominal pattern passed the 
                                # recall test then we continue to expand

                                self.stats.startTimer('refinment')

                                nominal_where_cond_list = []

                                for npair in npa['nominal_values']:
                                    npa['ordinal_quartiles'] = {}
                                    if(isinstance(npair[1], datetime.date)): # temp_fix for date type
                                        npair[1] = npair[1].strftime('%Y-%m-%d')
                                    nominal_where_cond_list.append("{}='{}'".format(npair[0],npair[1].replace("'","''")))

                                for n in importances:
                                    nominal_where_clause = ' AND '.join(nominal_where_cond_list)
                                    # logger.debug(f"attribute: {n}")
                                    q_get_quartiles = f"""
                                    SELECT percentile_cont(0.25) WITHIN GROUP (ORDER BY {n[0]}) AS {n[0]} FROM {jg_name} WHERE {nominal_where_clause}
                                    UNION
                                    SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY {n[0]}) AS {n[0]} FROM {jg_name} WHERE {nominal_where_clause}
                                    UNION
                                    SELECT percentile_cont(0.75) WITHIN GROUP (ORDER BY {n[0]}) AS {n[0]} FROM {jg_name} WHERE {nominal_where_clause}
                                    """
                                    # logger.debug(q_get_quartiles)
                                    self.cur.execute(q_get_quartiles)

                                    npa['ordinal_quartiles'][n[0]] = [x[0] for x in self.cur.fetchall()]

                                # logger.debug(npa['ordinal_quartiles'])

                                attrs_with_const_set = set([x[0] for x in npa['nominal_values']])

                                self.stats.stopTimer('refinment')

                                if(len(attrs_from_spec_node.intersection(attrs_with_const_set))>0): 
                                    # logger.debug("already has at least one attr from last node")
                                    
                                    self.stats.startTimer('refinment')

                                    # if nominal attrs already has at least one from last node: directly adding numerical attrs
                                    importance_feature_ranks = list(enumerate([x[0] for x in importances],0))

                                    max_number_of_numerical_possible = len(importance_feature_ranks)
                                    # logger.debug(f"max_number_of_numerical_possible : {max_number_of_numerical_possible} ")

                                    if(max_number_of_numerical_possible<=num_feature_to_consider):
                                        # if the number of clusters are smaller than the desired 
                                        # number of numerical features, then consider all
                                        numerical_variable_candidates = importance_feature_ranks
                                    else:
                                        numerical_variable_candidates = importance_feature_ranks[0:num_feature_to_consider]

                                    if(need_weighted_sampling==True and sorted_stddv_dct is None): # rank the variances

                                        num_names = [n[1] for n in numerical_variable_candidates]
                                        norm_stddv_num_list = [f"STDDEV({x})::numeric/AVG({x}) as std_{x}" for x in num_names]

                                        num_cand_variance_q = f"""
                                        SELECT {','.join(norm_stddv_num_list)}
                                        FROM {jg_name}                                                                                                                                                                                                                                               
                                        """

                                        # logger.debug(num_cand_variance_q)
                                        self.cur.execute(num_cand_variance_q)

                                        num_stddv_results = tuple(zip(num_names, list(self.cur.fetchone())))

                                        # logger.debug(num_stddv_results)

                                        sorted_stddvs = sorted(num_stddv_results, key = lambda x: x[1])
                                        sorted_stddv_dct = {k[0]: v for v, k in enumerate(sorted_stddvs)}

                                    # logger.debug(numerical_variable_candidates)

                                    cur_number_of_numercial_attrs = 0

                                    if(max_number_of_numerical_possible<=user_assigned_num_pred_cap):
                                        user_assigned_num_pred_cap = max_number_of_numerical_possible

                                    self.stats.stopTimer('refinment')

                                    # logger.debug(f"in has last node case: user_assigned_num_pred_cap: {user_assigned_num_pred_cap}")

                                    while(cur_pattern_candidates and cur_number_of_numercial_attrs<=user_assigned_num_pred_cap):
                                        good_candidates=[]

                                        for pc in cur_pattern_candidates:
                                            pc_processed, is_valid = self.get_fscore(prov_version=prov_version,
                                                                          f1_calculation_type=f1_calculation_type,
                                                                          pattern=pc,
                                                                          pattern_recall_threshold=pattern_recall_threshold,
                                                                          jg_name=jg_name,
                                                                          recall_dicts=recall_dicts,
                                                                          stddv_ranks_dict=sorted_stddv_dct,
                                                                          need_weighted_sampling=need_weighted_sampling,
                                                                          w_sample_attr=w_sample_attr,
                                                                          sample_size=sample_f1_jg_size)
                                            if(is_valid):
                                                val_pat = deepcopy(pc_processed)
                                                # logger.debug(val_pat)
                                                valid_patterns.append(val_pat)
                                                good_candidates.append(pc_processed)

                                        self.stats.startTimer('refinment')


                                        cur_pattern_candidates = self.extend_valid_candidates(good_candidates,
                                                                                              numerical_variable_candidates,
                                                                                              npa['ordinal_quartiles'],
                                                                                              correlation_dict)

                                        self.stats.stopTimer('refinment')


                                        cur_number_of_numercial_attrs+=1

                                else:
                                    if(rep_from_last_node):
                                        self.stats.startTimer('refinment')

                                        # logger.debug(importances)
                                        special_rep_from_last_node = rep_from_last_node[0]
                                        # logger.debug(special_rep_from_last_node)
                                        importance_list = [x[0] for x in importances]
                                        importance_list.remove(special_rep_from_last_node)
                                        importance_feature_ranks = list(enumerate(importance_list,0))

                                        max_number_of_numerical_possible = len(importance_feature_ranks)+1

                                        if(max_number_of_numerical_possible<=num_feature_to_consider):
                                            numerical_variable_candidates = importance_feature_ranks
                                        else:
                                            numerical_variable_candidates = importance_feature_ranks[0:num_feature_to_consider]

                                        cur_number_of_numercial_attrs = 1

                                        if(need_weighted_sampling==True and sorted_stddv_dct is None): # rank the variances

                                            num_names = [n[1] for n in numerical_variable_candidates]
                                            num_names.append(special_rep_from_last_node)
                                            norm_stddv_num_list = [f"STDDEV({x})::numeric/AVG({x}) as std_{x}" for x in num_names]

                                            num_cand_variance_q = f"""
                                            SELECT {','.join(norm_stddv_num_list)}
                                            FROM {jg_name}                                                                                                                                                                                                                                               
                                            """

                                            # logger.debug(num_cand_variance_q)
                                            self.cur.execute(num_cand_variance_q)

                                            num_stddv_results = tuple(zip(num_names, list(self.cur.fetchone())))

                                            # logger.debug(num_stddv_results)

                                            sorted_stddvs = sorted(num_stddv_results, key = lambda x: x[1])
                                            sorted_stddv_dct = {k[0]: v for v, k in enumerate(sorted_stddvs)}


                                        if(max_number_of_numerical_possible<=user_assigned_num_pred_cap):
                                            user_assigned_num_pred_cap = max_number_of_numerical_possible

                                        # need to add a numerical attribute 
                                        #from last node first to ensure patterns are valid


                                        initial_candidates = self.extend_valid_candidates(cur_pattern_candidates,
                                                                                          [(-1,special_rep_from_last_node)],
                                                                                          npa['ordinal_quartiles'],
                                                                                          correlation_dict)
                                        # logger.debug(f"initial_candidates: {initial_candidates}")

                                        self.stats.stopTimer('refinment')


                                        cur_pattern_candidates = []

                                        # since this case categorical pattern havent included 
                                        # a single attribute from the last node yet, we need to 
                                        # add a special step to make sure every pattern
                                        # that is about to be generated will be "valid"

                                        for ic in initial_candidates:
                                            ic_processed, is_valid = self.get_fscore(prov_version=prov_version,
                                                                          f1_calculation_type=f1_calculation_type,
                                                                          pattern=ic,
                                                                          pattern_recall_threshold=pattern_recall_threshold,
                                                                          jg_name=jg_name,
                                                                          recall_dicts=recall_dicts,
                                                                          stddv_ranks_dict=sorted_stddv_dct,
                                                                          need_weighted_sampling=need_weighted_sampling,
                                                                          w_sample_attr=w_sample_attr,
                                                                          sample_size=sample_f1_jg_size)
                                            if(is_valid):
                                                val_pat = deepcopy(ic_processed)
                                                valid_patterns.append(val_pat)
                                                # logger.debug(val_pat)
                                                cur_pattern_candidates.append(ic_processed)


                                        # logger.debug(f"in no last node case: user_assigned_num_pred_cap: {user_assigned_num_pred_cap}")

                                        while(cur_pattern_candidates and cur_number_of_numercial_attrs<=user_assigned_num_pred_cap):
                                            
                                            good_candidates=[]

                                            for pc in cur_pattern_candidates:

                                                self.stats.startTimer('refinment')
                                                new_candidates = self.extend_valid_candidates([pc],
                                                                                               numerical_variable_candidates,
                                                                                               npa['ordinal_quartiles'],
                                                                                               correlation_dict)
                                                self.stats.stopTimer('refinment')

                                                if(new_candidates):
                                                    for nc in new_candidates:
                                                        pc_processed, is_valid = self.get_fscore(prov_version=prov_version,
                                                                      f1_calculation_type=f1_calculation_type,
                                                                      pattern=nc,
                                                                      pattern_recall_threshold=pattern_recall_threshold,
                                                                      jg_name=jg_name,
                                                                      recall_dicts=recall_dicts,
                                                                      stddv_ranks_dict=sorted_stddv_dct,
                                                                      need_weighted_sampling=need_weighted_sampling,
                                                                      w_sample_attr=w_sample_attr,
                                                                      sample_size=sample_f1_jg_size)

                                                        if(is_valid):
                                                            val_pat = deepcopy(pc_processed)
                                                            valid_patterns.append(val_pat)
                                                            # logger.debug(val_pat)
                                                            good_candidates.append(pc_processed)

                                            cur_pattern_candidates = good_candidates
                                            cur_number_of_numercial_attrs+=1

                        # logger.debug(valid_patterns)
                    else:
                        for npa in nominal_pattern_dict_list:

                            nominal_patterns = []

                            # initialize 2 patterns with categorical attrs only
                            nominal_patterns.append({'join_graph':jg, 'recall':0, 'precision':0, 'nominal_values': npa['nominal_values'], 
                                'correlated_attrs': {}, 'attrs_with_const': None, 'ordinal_values':[],
                                'max_cluster_rank':-2, 'is_user':'yes'})

                            nominal_patterns.append({'join_graph':jg, 'recall':0, 'precision':0, 'nominal_values': npa['nominal_values'], 
                                'correlated_attrs': {}, 'attrs_with_const': None, 'ordinal_values':[],
                                'max_cluster_rank':-2, 'is_user':'no'})

                            if(just_lca):
                                valid_patterns.extend(nominal_patterns)

                            else:
                                for n_pat in nominal_patterns:
                                    n_pat_processed, is_valid = self.get_fscore(prov_version=prov_version,
                                                                      f1_calculation_type=f1_calculation_type,
                                                                      pattern=n_pat,
                                                                      pattern_recall_threshold=pattern_recall_threshold,
                                                                      jg_name=jg_name,
                                                                      recall_dicts=recall_dicts,
                                                                      need_weighted_sampling=need_weighted_sampling,
                                                                      stddv_ranks_dict = None,
                                                                      w_sample_attr=w_sample_attr,
                                                                      sample_size=sample_f1_jg_size)
                                    if(is_valid):
                                        valid_patterns.append(n_pat_processed)

                else:
                    if(ordinal_pattern_attr_list and not just_lca):

                        self.stats.startTimer('refinment')

                        patterns_passed_node_cond = []

                        if(need_weighted_sampling==True): # rank the variances
                            # logger.debug(ordinal_pattern_attr_list)
                            norm_stddv_num_list = [f"STDDEV({x})::numeric/AVG({x}) as std_{x}" for x in ordinal_pattern_attr_list]
                            num_cand_variance_q = f"""
                            SELECT {','.join(norm_stddv_num_list)}
                            FROM {jg_name}                                                                                                                                                                                                                                               
                            """

                            # logger.debug(num_cand_variance_q)
                            self.cur.execute(num_cand_variance_q)

                            num_stddv_results = tuple(zip(ordinal_pattern_attr_list, list(self.cur.fetchone())))

                            # logger.debug(num_stddv_results)

                            sorted_stddvs = sorted(num_stddv_results, key = lambda x: x[1])
                            sorted_stddv_dct = {k[0]: v for v, k in enumerate(sorted_stddvs)}

                        # construct dictionary for each nominal pattern with ordinal attributes
                        for npa in nominal_pattern_dict_list:
                            cur_number_numerical = 0
                            # add patterns that only include nominal attributes

                            patterns_passed_node_cond.append({'join_graph':jg, 'recall':0, 'precision':0, 
                                                              'nominal_values': npa['nominal_values'], 
                                                              'attrs_with_const':set([x[0] for x in npa['nominal_values']]),
                                                              'ordinal_values':[],
                                                              'is_user':'yes'})

                            patterns_passed_node_cond.append({'join_graph':jg, 'recall':0, 'precision':0, 
                                                              'nominal_values': npa['nominal_values'], 
                                                              'attrs_with_const':set([x[0] for x in npa['nominal_values']]), 
                                                              'ordinal_values':[],
                                                              'is_user':'no'})
                            
                            npa['ordinal_quartiles'] = {}

                            nominal_where_cond_list = []
                            
                            for npair in npa['nominal_values']:
                                npa['ordinal_quartiles'] = {}
                                if(isinstance(npair[1], datetime.date)): # temp_fix for date type
                                    # logger.debug(npair) 
                                    npair[1] = npair[1].strftime('%Y-%m-%d')
                                # logger.debug(npair)
                                nominal_where_cond_list.append("{}='{}'".format(npair[0],npair[1].replace("'","''")))

                            for n in ordinal_pattern_attr_list:
                                nominal_where_clause = ' AND '.join(nominal_where_cond_list)
                                # logger.debug(f"attribute: {n}")
                                q_get_quartiles = f"""
                                SELECT percentile_cont(0.25) WITHIN GROUP (ORDER BY {n}) AS {n} FROM {jg_name} WHERE {nominal_where_clause}
                                UNION
                                SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY {n}) AS {n} FROM {jg_name} WHERE {nominal_where_clause}
                                UNION
                                SELECT percentile_cont(0.75) WITHIN GROUP (ORDER BY {n}) AS {n} FROM {jg_name} WHERE {nominal_where_clause}
                                """
                                self.cur.execute(q_get_quartiles)

                                npa['ordinal_quartiles'][n] = [x[0] for x in self.cur.fetchall()]

                            # patterns with one ordinal attribute
                            ordi_one_attr = list(npa['ordinal_quartiles'])
                            
                            if(cur_number_numerical<user_assigned_num_pred_cap):
                                cur_number_numerical+=1
                                for one in ordi_one_attr:
                                    attrs_with_const_set = set([x[0] for x in npa['nominal_values']] + [one])
                                    # last node has to have an predicate attribute
                                    if(len( attrs_from_spec_node.intersection(attrs_with_const_set))>0):
                                        for val in npa['ordinal_quartiles'][one]:
                                            for one_dir in ['>','<']:
                                                # one_patt_with_ordi_dir_yes 
                                                patterns_passed_node_cond.append({'join_graph':jg, 'recall':0, 'precision':0, 'nominal_values': npa['nominal_values'], 
                                                    'ordinal_values': [(one,one_dir,val)], 'attrs_with_const':attrs_with_const_set, 'is_user':'yes'})
                                                # one_patt_with_ordi_dir_no 
                                                patterns_passed_node_cond.append({'join_graph':jg, 'recall':0, 'precision':0, 'nominal_values': npa['nominal_values'], 
                                                    'ordinal_values': [(one,one_dir,val)], 'attrs_with_const':attrs_with_const_set , 'is_user':'no'})

                            if(cur_number_numerical<user_assigned_num_pred_cap):
                                cur_number_numerical+=1
                                ordi_two_attr_pairs = list(itertools.combinations(list(npa['ordinal_quartiles']),2))
                                dir_combinations = list(itertools.product(['>', '<'], repeat=2))
                                for n in ordi_two_attr_pairs:
                                    attrs_with_const_set = set([x[0] for x in npa['nominal_values']] + list(n))
                                    if(len(attrs_from_spec_node.intersection(attrs_with_const_set))>0):
                                        for val_pair in itertools.product(npa['ordinal_quartiles'][n[0]],npa['ordinal_quartiles'][n[1]]):
                                            for one_dir in dir_combinations:
                                                # two_patt_dicts_with_dir_yes
                                                patterns_passed_node_cond.append({'join_graph':jg, 'recall':0, 'precision':0, 'nominal_values': npa['nominal_values'], 
                                                    'ordinal_values': list(zip(n,one_dir,val_pair)), 'attrs_with_const':attrs_with_const_set,  'is_user':'yes'})
                                                # two_patt_dicts_with_dir_no
                                                patterns_passed_node_cond.append({'join_graph':jg, 'recall':0, 'precision':0, 'nominal_values': npa['nominal_values'], 
                                                    'ordinal_values': list(zip(n,one_dir,val_pair)), 'attrs_with_const':attrs_with_const_set, 'is_user':'no'})

                            if(cur_number_numerical<user_assigned_num_pred_cap):
                                cur_number_numerical+=1
                                ordi_three_attr_pairs = list(itertools.combinations(list(npa['ordinal_quartiles']),3))
                                dir_combinations = list(itertools.product(['>', '<'], repeat=3))
                                # logger.debug(ordi_three_attr_pairs)
                                for n in ordi_three_attr_pairs:
                                    # logger.debug(n)
                                    attrs_with_const_set = set([x[0] for x in npa['nominal_values']] + list(n))
                                    if(len(attrs_from_spec_node.intersection(attrs_with_const_set))>0):
                                        for val_pair in itertools.product(npa['ordinal_quartiles'][n[0]],npa['ordinal_quartiles'][n[1]],npa['ordinal_quartiles'][n[2]]):
                                            # if(len(patterns_passed_node_cond)>1000000):
                                            #     break
                                            for one_dir in dir_combinations:
                                                # three_patt_dicts_with_dir_yes
                                                patterns_passed_node_cond.append({'join_graph':jg, 'recall':0, 'precision':0, 'nominal_values': npa['nominal_values'], 
                                                    'ordinal_values': list(zip(n,one_dir,val_pair)), 'attrs_with_const':attrs_with_const_set,  'is_user':'yes'})
                                                # three_patt_dicts_with_dir_no
                                                patterns_passed_node_cond.append({'join_graph':jg, 'recall':0, 'precision':0, 'nominal_values': npa['nominal_values'], 
                                                    'ordinal_values': list(zip(n,one_dir,val_pair)), 'attrs_with_const':attrs_with_const_set, 'is_user':'no'})

                        self.stats.stopTimer('refinment')


                        for ppnc in patterns_passed_node_cond:
                            pc_processed, is_valid = self.get_fscore(prov_version=prov_version,
                                          f1_calculation_type=f1_calculation_type,
                                          pattern=ppnc,
                                          pattern_recall_threshold=pattern_recall_threshold,
                                          jg_name=jg_name,
                                          recall_dicts=recall_dicts,
                                          stddv_ranks_dict=sorted_stddv_dct,
                                          need_weighted_sampling=need_weighted_sampling,
                                          w_sample_attr=w_sample_attr,
                                          sample_size=sample_f1_jg_size,
                                          )
                            if(is_valid):
                                valid_patterns.append(pc_processed)
                    else:
                        for npa in nominal_pattern_dict_list:

                            nominal_patterns = []

                            # initialize 2 patterns with categorical attrs only
                            nominal_patterns.append({'join_graph':jg, 'recall':0, 'precision':0, 'nominal_values': npa['nominal_values'], 
                                'correlated_attrs': {}, 'attrs_with_const': None, 'ordinal_values':[],
                                'max_cluster_rank':-2, 'is_user':'yes'})

                            nominal_patterns.append({'join_graph':jg, 'recall':0, 'precision':0, 'nominal_values': npa['nominal_values'], 
                                'correlated_attrs': {}, 'attrs_with_const': None, 'ordinal_values':[],
                                'max_cluster_rank':-2, 'is_user':'no'})
                            
                            if(just_lca):
                                valid_patterns.extend(nominal_patterns)
                            else:
                                for n_pat in nominal_patterns:
                                    n_pat_processed, is_valid = self.get_fscore(prov_version=prov_version,
                                                                      f1_calculation_type=f1_calculation_type,
                                                                      pattern=n_pat,
                                                                      pattern_recall_threshold=pattern_recall_threshold,
                                                                      jg_name=jg_name,
                                                                      recall_dicts=recall_dicts,
                                                                      need_weighted_sampling=need_weighted_sampling,
                                                                      stddv_ranks_dict = None,
                                                                      w_sample_attr=w_sample_attr,
                                                                      sample_size=sample_f1_jg_size)
                                    if(is_valid):
                                        valid_patterns.append(n_pat_processed)

                if(valid_patterns):
                    # logger.debug(f"number of valid patterns {len(valid_patterns)}")
                    for vp in valid_patterns:
                        self.stats.startTimer('pattern_recover')
                        vp_recovered = self.pattern_recover(renaming_dict, vp, user_questions_map)
                        self.stats.params['n_p_pass_node_rule_and_recall_thresh']+=1
                        self.stats.stopTimer('pattern_recover')
                        self.pattern_pool.append(vp_recovered)
                        self.pattern_by_jg[jg].append(vp_recovered)
            else:
                self.stats.stopTimer('create_samples')








