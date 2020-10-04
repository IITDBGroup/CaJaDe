import pandas as pd
import logging
import math
import re
import itertools
import random
import time
from instrumentation import ExecStats
from statistics import mean
from varclushi import VarClusHi
import numpy as np
from scipy.stats import entropy
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder
from copy import deepcopy

logger = logging.getLogger(__name__)


class PatternGeneratorStats(ExecStats):
    """
    Statistics gathered during mining
    """

    TIMERS = {'create_samples',
              'get_nominal_patterns',
              'enumerate_all_from_nominal_patterns',
              'validate_patterns_recall_constraint',
              'run_F1_query',
              'numercial_attr_filter',
              'pattern_recover',
              'rank_patterns',
              'deepcopy',
              'create_f1_sample_jg'
              }

    PARAMS = {'n_p_pass_node_rule',
              'n_p_pass_node_rule_and_recall_thresh'
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

    def pattern_recover(self, renaming_dict, pending_pattern, user_questions_map):

        pending_pattern['desc'] = []

        for nt in pending_pattern['nominal_values']:
            for k_n, v_n in renaming_dict.items():
                if(k_n=='max_rel_index' or k_n=='max_attr_index' or k_n=='dtypes'):
                    continue
                else:
                    for k1,v1 in v_n['columns'].items():
                        if(v1==nt[0]):
                            pending_pattern['desc'].append(f"{v_n['label']}_{k_n}.{k1}={nt[1]}")


        if('ordinal_values' in pending_pattern):
            for ot in pending_pattern['ordinal_values']:
                for k_o, v_o in renaming_dict.items():
                    if(k_o=='max_rel_index' or k_o=='max_attr_index' or k_o=='dtypes'):
                        continue
                    else:
                        for k2,v2 in v_o['columns'].items():
                            if(v2 == ot[0]):
                                pending_pattern['desc'].append(f"{v_o['label']}_{k_o}.{k2}{ot[1]}{ot[2]}")

        if('correlated_attrs' in pending_pattern and bool(pending_pattern['correlated_attrs'])):
            correlated_attrs_recover_dict = {}
            for k,v in pending_pattern['correlated_attrs'].items():
                for k_r, v_r in renaming_dict.items():
                    if(k_r=='max_rel_index' or k_r=='max_attr_index' or k_r=='dtypes'):
                        continue
                    else:
                        for k2,v2 in v_r['columns'].items():
                            if(v2 == k):
                                correlated_attrs_recover_dict[k2] = v

            for used_col, cor_cols in correlated_attrs_recover_dict.items():
                for i in range(len(cor_cols)):
                    for k_rr, v_rr in renaming_dict.items():
                        if(k_rr=='max_rel_index' or k_rr=='max_attr_index' or k_rr=='dtypes'):
                            continue
                        else:
                            for k2,v2 in v_rr['columns'].items():
                                if(v2 == cor_cols[i][0]):
                                    correlated_attrs_recover_dict[used_col][i] = k2

            pending_pattern['correlated_attrs'] = correlated_attrs_recover_dict

        pending_pattern['desc'] = ','.join(sorted(pending_pattern['desc']))
        pending_pattern['num_edges'] = pending_pattern['join_graph'].num_edges
        pending_pattern['is_user'] = user_questions_map[pending_pattern['is_user']]

        return pending_pattern


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
        for pte in patterns_to_extend:
            for n in numerical_variable_candidates:
                if(n[0]>pte['max_cluster_rank']):
                    for val in numerical_quartiles[n[1]]:
                        for one_dir in ['>','<']:
                            self.stats.startTimer('deepcopy')
                            new_ordinal_values = deepcopy(pte['ordinal_values'])
                            new_ordinal_values.append((n[1],one_dir,val))
                            new_correlated_attrs = deepcopy(pte['correlated_attrs'])
                            self.stats.stopTimer('deepcopy')
                            new_correlated_attrs[n[1]] = correlation_dict[n[1]]  
                            is_user=pte['is_user']
                            # one_patt_with_ordi_dir_yes 
                            candidates_to_return.append({'join_graph':pte['join_graph'], 'recall':0, 'precision':0, 
                                'nominal_values': pte['nominal_values'], 
                                'ordinal_values': new_ordinal_values,
                                'correlated_attrs': new_correlated_attrs,
                                'max_cluster_rank': n[0],                                            
                                'is_user': is_user})

        
        return candidates_to_return

    
    def get_fscore(self,
                   sample_repeatable,
                   seed,
                   f1_calculation_type,
                   f1_calculation_sample_rate,
                   f1_calculation_min_size,
                   pattern,
                   pattern_recall_threshold,
                   jg_name,
                   jg_apt_size,
                   recall_dict
                   ):


        # use to keep the true positive value here since 
        # true postive for recall will always be the same


        self.stats.startTimer('validate_patterns_recall_constraint')
        where_cond_list = []
        if('ordinal_values' in pattern):
            for ot in pattern['ordinal_values']:
                where_cond_list.append(f"{ot[0]}{ot[1]}{ot[2]}")
            for nt in pattern['nominal_values']:
                n_value = nt[1].replace("'","''")
                where_cond_list.append(f"{nt[0]}='{n_value}'")
        else:
            for nt in pattern['nominal_values']:
                n_value = nt[1].replace("'","''")
                where_cond_list.append(f"{nt[0]}='{n_value}'")


        pattern_conditions = " AND ".join(where_cond_list)
        where_cond_list.append(f"is_user='{pattern['is_user']}'")
        true_positive_conditions = " AND ".join(where_cond_list)


        check_recall_threshold_q = f"""
        SELECT ROUND
        (
          (
          SELECT COUNT(DISTINCT prov_number) 
          FROM {jg_name}
          WHERE {true_positive_conditions}
          )::NUMERIC
          /
          NULLIF(
          {recall_dict['no'] if pattern['is_user']=='no' else recall_dict['yes']}
          ,0)
        ,5
        );
        """

        self.cur.execute(check_recall_threshold_q)

        recall_result = self.cur.fetchone()[0]

        if(recall_result is None):
            recall_result = 0
        else:
            recall_result = float(recall_result)

        if(recall_result<pattern_recall_threshold):
            self.stats.stopTimer('validate_patterns_recall_constraint')
            return pattern, False
        else:
            pattern['recall'] = recall_result
            self.stats.stopTimer('validate_patterns_recall_constraint')

            F1_q = f"""
            WITH precision AS 
            (SELECT 
                (
                SELECT COUNT(DISTINCT prov_number)
                FROM {jg_name}
                WHERE {true_positive_conditions}
                )::NUMERIC
                /
                NULLIF(
                (
                SELECT SUM(prov_number_sum) FROM 
                (
                SELECT COUNT(DISTINCT prov_number) AS prov_number_sum
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
        self.cur.execute(F1_q)
        self.stats.stopTimer('run_F1_query')
        result = self.cur.fetchone()
        prec,f1 = result[0], result[1]

        if(f1 is None):
            pattern['F1'] = 0
        else:
            pattern['F1'] = float(f1)

        if(prec is None):
            pattern['precision'] = 0
        else:
            pattern['precision'] = float(prec)

        return pattern, True


    def gen_patterns(self, 
                      jg, 
                      jg_name, 
                      renaming_dict, 
                      skip_cols,
                      user_questions_map,
                      original_pt_size,
                      attr_alias='a',
                      max_sample_factor=5,
                      s_rate_for_s=0.5,
                      pattern_recall_threshold=0.3, 
                      numercial_attr_filter_method = 'n',
                      sample_repeatable = True,
                      seed = 0.5,
                      f1_calculation_type = 'o',
                      f1_calculation_sample_rate = 0.3,
                      f1_calculation_min_size = 100,
                      user_assigned_num_pred_cap = 3,
                      num_numerical_attr_rate = 1.5
                      ):

        """
        jg_name: view created from jg_generator
        
        skip_cols: renamed keys will be ignored
        
        attr_alias: used to further ignore colmns from user query in provenance
        
        s_rate_for_s: sample rate for s: this is a must, usually very small compared to d size 
                
        user_assigned_num_pred_cap: maximum number of numerical attributes allowed.
        doesnt need to meet this number: (could be filtered by recall thresh or invalidated by
        max number of clusters)

        num_numerical_attr_rate: if filter method is 'varclus', after random forest, how 
        many important features will be considered? it is equal to user_assigned_num_pred_cap*num_numerical_attr_rate
        """

        # logger.debug(jg.jg_number)

        self.pattern_by_jg[jg] = []

        recall_dict = {'yes':0, 'no':0}

        count_user_prov = f"SELECT count(*) as size FROM {jg_name} WHERE is_user='yes'"
        self.cur.execute(count_user_prov)
        user_p_size = int(self.cur.fetchone()[0])

        count_n_user_prov = f"SELECT count(*) as size FROM {jg_name} WHERE is_user='no'"
        self.cur.execute(count_n_user_prov)
        n_user_p_size = int(self.cur.fetchone()[0])

        jg_apt_size = user_p_size+n_user_p_size

        # based on f1 calcuation type set up recall dict and materialized apt to evaluate f1
        if(f1_calculation_type=='s' and jg_apt_size>f1_calculation_min_size):

            self.stats.startTimer('create_f1_sample_jg')

            sample_f1_jg_size = math.ceil(jg_apt_size * f1_calculation_sample_rate)

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
            # use {jg_name}_fs as jg to calculate recall and f1
        
            q_tp_yes = f"""
            SELECT COUNT(DISTINCT prov_number)
            FROM {jg_name}
            WHERE is_user='yes'
            """
            self.cur.execute(q_tp_yes)
            recall_dict['yes'] = int(self.cur.fetchone()[0])

            q_tp_no = f"""
            SELECT COUNT(DISTINCT prov_number)
            FROM {jg_name}
            WHERE is_user='no'
            """
            self.cur.execute(q_tp_no)
            recall_dict['no'] = int(self.cur.fetchone()[0])

            self.stats.stopTimer('create_f1_sample_jg')

            fscore_calculation_jg = f"{jg_name}_fs"
        
        else:
            q_tp_yes = f"""
            SELECT COUNT(DISTINCT prov_number)
            FROM {jg_name}
            WHERE is_user='yes'
            """
            self.cur.execute(q_tp_yes)
            recall_dict['yes'] = int(self.cur.fetchone()[0])

            q_tp_no = f"""
            SELECT COUNT(DISTINCT prov_number)
            FROM {jg_name}
            WHERE is_user='no'
            """
            self.cur.execute(q_tp_no)
            recall_dict['no'] = int(self.cur.fetchone()[0])

            fscore_calculation_jg = jg_name


        self.stats.startTimer('create_samples')
        attrs_from_spec_node = set([v for k,v in renaming_dict[jg.spec_node_key]['columns'].items()])

        # logger.debug(renaming_dict)
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
                              or (x=='is_user' or x=='prov_number')]
        attrs_in_d = ','.join(considered_attrs_d) 

        drop_prov_d = f"DROP MATERIALIZED VIEW IF EXISTS {jg_name}_d CASCADE;"
        # logger.debug(drop_prov_d)
        self.cur.execute(drop_prov_d)

        drop_prov_s = f"DROP MATERIALIZED VIEW IF EXISTS {jg_name}_s CASCADE;"
        # logger.debug(drop_prov_s)
        self.cur.execute(drop_prov_s)

        s_user_p_sample_size = min(max_sample_factor*original_pt_size, math.ceil(user_p_size*s_rate_for_s))
        d_user_p_sample_size = s_user_p_sample_size

        s_n_user_p_sample_size = min(max_sample_factor*original_pt_size, math.ceil(n_user_p_size*s_rate_for_s))
        d_n_user_p_sample_size = s_n_user_p_sample_size

        if(d_user_p_sample_size!=0):
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
            nominal_pattern_attr_clause = ','.join(nominal_pattern_attr_list)

            sample_repeatable_clause = None

            if(sample_repeatable):
                sample_repeatable_clause = 'REPEATABLE({})'.format(seed)
            else:
                sample_repeatable_clause =''
            system_sample_rate = int(s_rate_for_s*100)
            
            setseed_q = f"""
            select setseed({seed})
            """

            prov_d_creation_q = f"""
            CREATE MATERIALIZED VIEW {jg_name}_d AS
            (
                (
                SELECT {attrs_in_d}
                FROM {jg_name} 
                WHERE is_user = 'yes'
                ORDER BY RANDOM()
                LIMIT {d_user_p_sample_size} 
                )       
            UNION ALL
                (
                SELECT {attrs_in_d}
                FROM {jg_name} 
                WHERE is_user = 'no'
                ORDER BY RANDOM()
                LIMIT {d_n_user_p_sample_size}
                )
            );
            """

            if(sample_repeatable):
                self.cur.execute(setseed_q)
            self.cur.execute(prov_d_creation_q)

            prov_s_creation_q = f"""
            CREATE MATERIALIZED VIEW {jg_name}_s AS
            (
                (
                SELECT {attrs_in_s} 
                FROM {jg_name} 
                WHERE is_user = 'yes'
                ORDER BY RANDOM()
                LIMIT {d_user_p_sample_size} 
                )       
            UNION ALL
                (
                SELECT {attrs_in_s} 
                FROM {jg_name} 
                WHERE is_user = 'no'
                ORDER BY RANDOM()
                LIMIT {d_n_user_p_sample_size}
                )
            );
            """
            if(sample_repeatable):
                self.cur.execute(setseed_q)

            self.cur.execute(prov_s_creation_q)

            self.stats.stopTimer('create_samples')
            self.stats.startTimer('get_nominal_patterns')

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
            limit 10;
            """

            get_nominal_patterns_q = f"""
            SELECT {nominal_pattern_attr_clause} FROM {jg_name}_p;
            """

            # limit 10 patterns per jg for now

            # logger.debug(pattern_creation_q)
            self.cur.execute(pattern_creation_q)

            nominal_pattern_df = pd.read_sql(get_nominal_patterns_q, self.conn)
            # logger.debug(nominal_pattern_df)

            nominal_pattern_dicts = nominal_pattern_df.to_dict('records')
            self.stats.stopTimer('get_nominal_patterns')

            nominal_pattern_dict_list = []

            for n_pa in nominal_pattern_dicts:
                n_pa_dict = {}
                n_pa_dict['nominal_values'] = [(k, v) for k, v in n_pa.items() if (v is not None and not pd.isnull(v))]
                nominal_pattern_dict_list.append(n_pa_dict) 

            valid_patterns = [] # list of all the valid patterns that can be generated from this nominal pattern


            if(numercial_attr_filter_method=='y'):

                # clustering + random forest to select important attributes 
                # with a caveat in mind that at least one of the attributes
                # comes from the "last node" specified in join graph 
                # (either nominal or ordinal has to be in the patten)

                self.stats.startTimer('numercial_attr_filter')
                # do clustering first
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

                # remove constant
                cor_df = cor_df.loc[:,(cor_df != cor_df.iloc[0]).any()] 

                variable_clustering = VarClusHi(cor_df, maxeigval2=1, maxclus=None)
                variable_clustering.varclus()

                cluster_dict = variable_clustering.rsquare[['Cluster', 'Variable']].groupby('Cluster')['Variable'].apply(list).to_dict()

                for k,v in cluster_dict.items():
                    cluster_dict[k] = [[x,0,0] for x in v]      

                # entropy rank in each cluster to find the highest one 
                # as the training input variable "representing" the cluster
                # 3 elements list for each column, col[1] is for entropy, col[2] is flag indicating
                # if it is from the  last node
                # (from last node: 1 not from last node:0)

                rf_input_vars = []
                rep_from_last_node = []
                correlation_dict = {} 
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
                    if(representative_var_for_clust in attrs_from_spec_node):
                        rep_from_last_node.append(representative_var_for_clust)
                    rf_input_vars.append(representative_var_for_clust)
                    correlation_dict[representative_var_for_clust] = [cora[0] for cora in cluster_dict[k][1:]]

                # finish clustering here

                rf_df = cor_df[rf_input_vars]
                target = raw_df['is_user']
                le = LabelEncoder()
                y = le.fit(target)
                y = le.transform(target)
                forest = RandomForestClassifier(n_estimators=500, max_depth=10)
                forest.fit(rf_df, y)
                importances = [list(t) for t in zip(rf_df, forest.feature_importances_)]
                importances = sorted(importances, key = lambda x: x[1], reverse=True)

                self.stats.stopTimer('numercial_attr_filter')

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
                        n_pat_processed, is_valid = self.get_fscore(sample_repeatable=sample_repeatable,
                                                          seed=seed,
                                                          f1_calculation_type=f1_calculation_type,
                                                          f1_calculation_sample_rate=f1_calculation_sample_rate,
                                                          f1_calculation_min_size=f1_calculation_min_size,
                                                          pattern=n_pat,
                                                          pattern_recall_threshold=pattern_recall_threshold,
                                                          jg_name=fscore_calculation_jg,
                                                          jg_apt_size=jg_apt_size,
                                                          recall_dict=recall_dict)
                        if(is_valid):
                            cur_pattern_candidates.append(n_pat_processed)

                    if(cur_pattern_candidates):
                        # if at least one nominal pattern passed the 
                        # recall test then we continue to expand

                        self.stats.startTimer('enumerate_all_from_nominal_patterns')

                        for npair in npa['nominal_values']:
                            npa['ordinal_quartiles'] = {}
                            nominal_where_cond_list = []
                            # logger.debug(npair)
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
                            self.cur.execute(q_get_quartiles)

                            npa['ordinal_quartiles'][n[0]] = [x[0] for x in self.cur.fetchall()]

                        attrs_with_const_set = set([x[0] for x in npa['nominal_values']])

                        self.stats.stopTimer('enumerate_all_from_nominal_patterns')

                        if(len(attrs_from_spec_node.intersection(attrs_with_const_set))>0): 
                            
                            self.stats.startTimer('enumerate_all_from_nominal_patterns')

                            # if nominal attrs already has at least one from last node: directly adding numerical attrs
                            importance_feature_ranks = list(enumerate([x[0] for x in importances],0))

                            max_number_of_numerical_possible = len(importance_feature_ranks)

                            if(max_number_of_numerical_possible<=num_feature_to_consider):
                                # if the number of clusters are smaller than the desired 
                                # number of numerical features, then consider all
                                numerical_variable_candidates = importance_feature_ranks
                            else:
                                numerical_variable_candidates = importance_feature_ranks[0:num_feature_to_consider]

                            cur_number_of_numercial_attrs = 0

                            if(max_number_of_numerical_possible<=user_assigned_num_pred_cap):
                                user_assigned_num_pred_cap = max_number_of_numerical_possible

                            self.stats.stopTimer('enumerate_all_from_nominal_patterns')

                            # logger.debug(f"in has last node case: user_assigned_num_pred_cap: {user_assigned_num_pred_cap}")
                            while(cur_pattern_candidates and cur_number_of_numercial_attrs<user_assigned_num_pred_cap):
                                good_candidates=[]

                                for pc in cur_pattern_candidates:
                                    pc_processed, is_valid = self.get_fscore(sample_repeatable=sample_repeatable,
                                                                  seed=seed,
                                                                  f1_calculation_type=f1_calculation_type,
                                                                  f1_calculation_sample_rate=f1_calculation_sample_rate,
                                                                  f1_calculation_min_size=f1_calculation_min_size,
                                                                  pattern=pc,
                                                                  pattern_recall_threshold=pattern_recall_threshold,
                                                                  jg_name=fscore_calculation_jg,
                                                                  jg_apt_size=jg_apt_size,
                                                                  recall_dict=recall_dict)
                                    if(is_valid):
                                        self.stats.startTimer('deepcopy')
                                        val_pat = deepcopy(pc_processed)
                                        self.stats.stopTimer('deepcopy')
                                        valid_patterns.append(val_pat)
                                        good_candidates.append(pc_processed)

                                self.stats.startTimer('enumerate_all_from_nominal_patterns')


                                cur_pattern_candidates = self.extend_valid_candidates(good_candidates,
                                                                                      numerical_variable_candidates,
                                                                                      npa['ordinal_quartiles'],
                                                                                      correlation_dict)

                                self.stats.stopTimer('enumerate_all_from_nominal_patterns')


                                cur_number_of_numercial_attrs+=1

                        else:
                            if(rep_from_last_node):
                                self.stats.startTimer('enumerate_all_from_nominal_patterns')

                                # logger.debug(importances)
                                special_rep_from_last_node = rep_from_last_node[0]
                                # logger.debug(special_rep_from_last_node)
                                importance_list = [x[0] for x in importances]
                                importance_list.remove(special_rep_from_last_node)
                                importance_feature_ranks = list(enumerate(importance_list,0))

                                max_number_of_numerical_possible = len(importance_feature_ranks)+1

                                # need to add a numerical attribute 
                                #from last node first to ensure patterns are valid


                                initial_candidates = self.extend_valid_candidates(cur_pattern_candidates,
                                                                                  [(-1,special_rep_from_last_node)],
                                                                                  npa['ordinal_quartiles'],
                                                                                  correlation_dict)
                                # logger.debug(f"initial_candidates: {initial_candidates}")

                                self.stats.stopTimer('enumerate_all_from_nominal_patterns')


                                cur_pattern_candidates = []

                                # since this case categorical pattern havent included 
                                # a single attribute from the last node yet, we need to 
                                # add a special step to make sure every pattern
                                # that is about to be generated will be "valid"

                                for ic in initial_candidates:
                                    ic_processed, is_valid = self.get_fscore(sample_repeatable=sample_repeatable,
                                                                  seed=seed,
                                                                  f1_calculation_type=f1_calculation_type,
                                                                  f1_calculation_sample_rate=f1_calculation_sample_rate,
                                                                  f1_calculation_min_size=f1_calculation_min_size,
                                                                  pattern=ic,
                                                                  pattern_recall_threshold=pattern_recall_threshold,
                                                                  jg_name=fscore_calculation_jg,
                                                                  jg_apt_size=jg_apt_size,
                                                                  recall_dict=recall_dict)
                                    if(is_valid):
                                        self.stats.startTimer('deepcopy')
                                        val_pat = deepcopy(ic_processed)
                                        self.stats.stopTimer('deepcopy')
                                        valid_patterns.append(val_pat)
                                        cur_pattern_candidates.append(ic_processed)

                                if(max_number_of_numerical_possible<=num_feature_to_consider):
                                    numerical_variable_candidates = importance_feature_ranks
                                else:
                                    numerical_variable_candidates = importance_feature_ranks[0:num_feature_to_consider]

                                cur_number_of_numercial_attrs = 1


                                if(max_number_of_numerical_possible<=user_assigned_num_pred_cap):
                                    user_assigned_num_pred_cap = max_number_of_numerical_possible

                                # logger.debug(f"in no last node case: user_assigned_num_pred_cap: {user_assigned_num_pred_cap}")

                                while(cur_pattern_candidates and cur_number_of_numercial_attrs<user_assigned_num_pred_cap):
                                    
                                    good_candidates=[]

                                    for pc in cur_pattern_candidates:

                                        self.stats.startTimer('enumerate_all_from_nominal_patterns')
                                        new_candidates = self.extend_valid_candidates([pc],
                                                                                       numerical_variable_candidates,
                                                                                       npa['ordinal_quartiles'],
                                                                                       correlation_dict)
                                        self.stats.stopTimer('enumerate_all_from_nominal_patterns')

                                        if(new_candidates):
                                            for nc in new_candidates:
                                                pc_processed, is_valid = self.get_fscore(sample_repeatable=sample_repeatable,
                                                              seed=seed,
                                                              f1_calculation_type=f1_calculation_type,
                                                              f1_calculation_sample_rate=f1_calculation_sample_rate,
                                                              f1_calculation_min_size=f1_calculation_min_size,
                                                              pattern=nc,
                                                              pattern_recall_threshold=pattern_recall_threshold,
                                                              jg_name=fscore_calculation_jg,
                                                              jg_apt_size=jg_apt_size,
                                                              recall_dict=recall_dict)
                                                if(is_valid):
                                                    self.stats.startTimer('deepcopy')
                                                    val_pat = deepcopy(pc_processed)
                                                    self.stats.stopTimer('deepcopy')
                                                    valid_patterns.append(val_pat)
                                                    good_candidates.append(pc_processed)

                                    cur_pattern_candidates = good_candidates
                                    cur_number_of_numercial_attrs+=1

                        self.stats.params['n_p_pass_node_rule'] += len(valid_patterns)

            else:
                self.stats.startTimer('enumerate_all_from_nominal_patterns')

                patterns_passed_node_cond = []
                # construct dictionary for each nominal pattern with ordinal attributes
                for npa in nominal_pattern_dict_list:
                    # add patterns that only include nominal attributes
                    patterns_passed_node_cond.append({'join_graph':jg, 'recall':0, 'precision':0, 
                                                      'nominal_values': npa['nominal_values'], 
                                                      'attrs_with_const':set([x[0] for x in npa['nominal_values']]),
                                                      'is_user':'yes'})

                    patterns_passed_node_cond.append({'join_graph':jg, 'recall':0, 'precision':0, 
                                                      'nominal_values': npa['nominal_values'], 
                                                      'attrs_with_const':set([x[0] for x in npa['nominal_values']]), 
                                                      'is_user':'no'})
                    
                    npa['ordinal_quartiles'] = {}

                    nominal_where_cond_list = []
                    
                    for npair in npa['nominal_values']:
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

                self.stats.params['n_p_pass_node_rule'] += len(patterns_passed_node_cond)

                self.stats.stopTimer('enumerate_all_from_nominal_patterns')


                for ppnc in patterns_passed_node_cond:
                    pc_processed, is_valid = self.get_fscore(sample_repeatable=sample_repeatable,
                                  seed=seed,
                                  f1_calculation_type=f1_calculation_type,
                                  f1_calculation_sample_rate=f1_calculation_sample_rate,
                                  f1_calculation_min_size=f1_calculation_min_size,
                                  pattern=ppnc,
                                  pattern_recall_threshold=pattern_recall_threshold,
                                  jg_name=fscore_calculation_jg,
                                  jg_apt_size=jg_apt_size,
                                  recall_dict=recall_dict)
                    if(is_valid):
                        valid_patterns.append(pc_processed)


            if(valid_patterns):
                for vp in valid_patterns:
                    # logger.debug(vp)
                    self.stats.startTimer('pattern_recover')
                    vp_recovered = self.pattern_recover(renaming_dict, vp, user_questions_map)
                    self.stats.params['n_p_pass_node_rule_and_recall_thresh']+=1
                    self.stats.stopTimer('pattern_recover')
                    self.pattern_pool.append(vp_recovered)
                    self.pattern_by_jg[jg].append(vp_recovered)




