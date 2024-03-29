from src.provenance_getter import provenance_getter
from src.gprom_wrapper import  GProMWrapper
from src.jg_generator import Join_Graph_Generator
from src.jg_materializer import Join_Graph_Materializer
from src.pattern_generator import Pattern_Generator
from src.sg_generator import Schema_Graph_Generator
from src.workloads import mimic_workloads, nba_workloads
from src.case_study import mimic_cases, nba_cases
from src.instrumentation import ExecStats
from src.renaming import encode
import src.config
from networkx import MultiGraph
import networkx as nx
import psycopg2
import colorful
import logging 
import re
import colorful
import random
from statistics import mean 
import argparse
from datetime import datetime
from time import strftime


#####
##import app.py
###from app import colNum

logger = logging.getLogger(__name__)


class ExperimentParams(ExecStats):
    """
    Statistics gathered during mining
    """
    TIMERS = {'one_jg_timer'}

    PARAMS = {'result_schema',
              'user_questions',
              'dbname',
              'sample_rate_for_s',
              'maximum_edges',
              'min_recall_threshold',
              'numercial_attr_filter_method',
              'exclude_high_cost_jg',
              'f1_calculation_type',
              'f1_sample_rate',
              'f1_min_sample_size_threshold'
              }



def Create_Stats_Table(conn, stats_trackers, stats_relation_name, schema):
  """
  stats_trackers: a list of objects keep tracking of the stats from experiment
  stats_relation_name: the relation name storing the stats, needs to be re-created if number of attrs change
  schema: the schema where stats_relation located in
  """
  cur = conn.cursor()

  cur.execute('CREATE SCHEMA IF NOT EXISTS '+schema)

  attr = ''

  timers_list = []
  counters_list = []
  params_list = []

  for stats_tracker in stats_trackers:
      timers_list.extend(list(stats_tracker.time))
      counters_list.extend(list(stats_tracker.counters))
      params_list.extend(list(stats_tracker.params))

  stats_list = timers_list+counters_list+params_list+['total']
      
  for stat in stats_list:
      attr += stat+' varchar,'

  attr+='exp_desc varchar, exp_time varchar'

  
  cur.execute('create table IF NOT EXISTS ' + schema + '.' + stats_relation_name + ' (' +
                           'id serial primary key,' +
                           attr +');')

def Create_jg_time_stats(conn, stats_relation_name, schema):

  cur = conn.cursor()

  cur.execute('CREATE SCHEMA IF NOT EXISTS ' + schema)

  cur.execute('create table IF NOT EXISTS ' + schema + '.' + stats_relation_name + ' (' +
                           'id serial primary key, exp_time text, exp_desc text, jg text, timecost float);')


def Insert_jg_time_stats(conn, jg_time_dict, stats_relation_name, schema, exp_time, exp_desc):
  cur = conn.cursor()
  for k,v in jg_time_dict.items():
    # logger.debug(f"INSERT INTO {schema}.{stats_relation_name}(exp_time, exp_desc, jg, timecost) VALUES ('{exp_time}', '{exp_desc}', '{str(k)}', {str(v)})")
    cur.execute(f"INSERT INTO {schema}.{stats_relation_name}(exp_time, exp_desc, jg, timecost) VALUES ('{exp_time}', '{exp_desc}', '{str(k)}', {str(v)})")

def InsertPatterns(conn, exp_desc, patterns, pattern_relation_name, schema, exp_time, result_type='s'):

  # result_type: 
  #             s: sample type, use this for fastest results
  #             e: evaluation type, will add sample_precision, sample_f1 as more info required

  # logger.debug(patterns[0:5])


  cur = conn.cursor()

  cur.execute('CREATE SCHEMA IF NOT EXISTS '+schema)

  cols = ['exp_time', 'exp_desc', 'is_user', 'jg', 'jg_details', 'jg_name', 'num_edges', 'p_desc', 
  'recall', 'precision', 'fscore', 'sample_recall', 'sample_precision', 'sample_F1']

  cols_with_types = ''

  for col in cols:
      cols_with_types += col+' varchar,'

  cols_with_types+='user_question_map jsonb, pattern_attr_mappings jsonb'

  cols.extend(['user_question_map', 'pattern_attr_mappings'])

  cur.execute('create table IF NOT EXISTS ' + schema + '.' + pattern_relation_name + ' (' +
                           'id serial primary key,' +
                           cols_with_types +');')

  limit = 5000
  patterns_num = len(patterns)
  cur_batch_size = 0
  patterns_to_insert = []

  for p in patterns:
    if(cur_batch_size<limit):
      cur_batch_size+=1
      p_print = p['desc'].replace("'","''")
      jg_print = str(p['join_graph']).replace("'","''")
      jg_details = repr(p['join_graph']).replace("'", "''")
      if(result_type=='s' or result_type=='o'):
        patterns_to_insert.append(f"('{exp_time}','{exp_desc}', '{p['is_user']}', '{jg_print}', '{jg_details}', '{p['jg_name']}', '{p['num_edges']}', '{p_print}', \
          '{p['recall']}', '{p['precision']}','{p['F1']}', '', '', '', '{p['user_question_map']}', '{p['pattern_attr_mappings']}')")
      elif(result_type=='e'):
        patterns_to_insert.append(f"('{exp_time}','{exp_desc}', '{p['is_user']}', '{jg_print}', '{jg_details}', '{p['jg_name']}', '{p['num_edges']}', '{p_print}', \
          '{p['recall']}', '{p['precision']}','{p['F1']}','{p['sample_recall']}', '{p['sample_precision']}', '{p['sample_F1']}', '{p['user_question_map']}', \
           '{p['pattern_attr_mappings']}')")
      else:
        patterns_to_insert.append(f"('{exp_time}','{exp_desc}', '{p['is_user']}', '{jg_print}', '', '{p['num_edges']}', '{p_print}', \
          '{p['recall']}', '{p['precision']}','','', '', '', '{p['user_question_map']}', '{p['pattern_attr_mappings']}')")
    else:
      cur.execute(
          'INSERT INTO ' + schema + '.' + pattern_relation_name + ' ('+ ','.join(cols) +')' + ' values '+ ', '.join(patterns_to_insert)
          )
      patterns_to_insert = []
      if(result_type=='s' or result_type=='o'):
       patterns_to_insert.append(f"('{exp_time}','{exp_desc}', '{p['is_user']}', '{jg_print}', '{jg_details}', '{p['jg_name']}', '{p['num_edges']}', '{p_print}', \
         '{p['recall']}', '{p['precision']}','{p['F1']}', '', '', '', '{p['user_question_map']}', '{p['pattern_attr_mappings']}')")
      elif(result_type=='e'):
       patterns_to_insert.append(f"('{exp_time}','{exp_desc}', '{p['is_user']}', '{jg_print}', '{jg_details}', '{p['jg_name']}', '{p['num_edges']}', '{p_print}', \
         '{p['recall']}', '{p['precision']}','{p['F1']}','{p['sample_recall']}', '{p['sample_precision']}', '{p['sample_F1']}', '{p['user_question_map']}', \
          '{p['pattern_attr_mappings']}')")
      else:
        patterns_to_insert.append(f"('{exp_time}','{exp_desc}', '{p['is_user']}', '{jg_print}', '', '{p['num_edges']}', '{p_print}', \
         '{p['recall']}', '{p['precision']}','','', '', '', '{p['user_question_map']}', '{p['pattern_attr_mappings']}')")

      cur_batch_size=1

  if(patterns_to_insert):
    cur.execute(
        'INSERT INTO ' + schema + '.' + pattern_relation_name + ' ('+ ','.join(cols) +')' + ' values '+ ', '.join(patterns_to_insert)
        )        


def InsertStats(conn, stats_trackers, stats_relation_name, schema, exp_time, exp_desc):

    timers_vals = []
    counters_vals = []
    params_vals = []

    timers_attrs = []
    counters_attrs = []
    params_attrs = []


    total_time = 0

    for stats_tracker in stats_trackers:
        timers_attrs.extend(list(stats_tracker.time))
        counters_attrs.extend(list(stats_tracker.counters))
        params_attrs.extend(list(stats_tracker.params))

    attr_list = timers_attrs+counters_attrs+params_attrs+['total','exp_desc','exp_time']
    attrs = ','.join(attr_list)

    for stats_tracker in stats_trackers:
        for v in stats_tracker.time.values():
            total_time+=float(v)
        timers_vals.extend([str(round(x,2)) for x in stats_tracker.time.values()]) 
        counters_vals.extend([str(x) for x in stats_tracker.counters.values()])
        params_vals.extend([str(x) for x in stats_tracker.params.values()])

    values = ','.join(timers_vals+counters_vals+params_vals+[f"'{str(total_time)}', '{exp_desc}', '{exp_time}'"])
    # logger.debug(f'InsertStats: {values}')

    cur = conn.cursor()
    # logger.debug('INSERT INTO ' + schema + '.' + stats_relation_name + ' ('+ attrs +')' + ' values('+ values +')')
    cur.execute(
        'INSERT INTO ' + schema + '.' + stats_relation_name + ' ('+ attrs +')' + ' values('+ values +')'
        )


def run_experiment(conn=None,
                   result_schema='demotest',
                   user_query = ("provenance of (select count(*) as win, s.season_name from team t, game g, season s where t.team_id = g.winner_id and g.season_id = s.season_id and t.team= 'GSW' group by s.season_name);",'test'),
                   user_questions = ["season_name='2015-16'","season_name='2012-13'"],
                   user_questions_map = {'yes':'2015-16', 'no':'2012-13'},
                   user_specified_attrs=[('team','team'),('season','season_name')],
                   user_name='juseung', #lchenjie
                   password='1234',
                   host='localhost',
                   port='5432',
                   dbname='nba', 
                   sample_rate_for_s=0.1,
                   lca_s_max_size=100,
                   lca_s_min_size=100,
                   maximum_edges=2, #set maximum_edges=1 
                   min_recall_threshold=0.2,
                   numercial_attr_filter_method='y',
                   f1_sample_rate=0.3,
                   f1_sample_type='s',
                   exclude_high_cost_jg = (False, 'f'),
                   f1_calculation_type = 'o',
                   user_assigned_max_num_pred = 3,
                   f1_min_sample_size_threshold=100,
                   lca_eval_mode=False,
                   statstracker=ExperimentParams(),
                   gui=False):
    # added a gui parameter, if true bypass pt creation step
    # f1_calculation_type: "o": evaluate on original materialized jg
    #                      "s": evaluate on a sampled materialized jg only sample size is decided
    #                           based on f1_sample_rate and f1_min_sample_size_threshold
    #                      "e": f1 sample rate evaluation mode: this is to investigate the sample
    #                           rate effect on the accuracy in terms of fscore, dont use this mode
    #                           to measure runtime.
    
    statstracker.params['result_schema'] = "'{}'".format(result_schema)
    statstracker.params['user_query'] = "'{}'".format(user_query[1])
    statstracker.params['user_questions']="'{}'".format(" VS ".join([x.replace("'", '') for x in user_questions]))
    statstracker.params['dbname']= "'{}'".format(dbname)
    statstracker.params['sample_rate_for_s']= "'{}'".format(sample_rate_for_s)
    statstracker.params['maximum_edges']="'{}'".format(maximum_edges)
    statstracker.params['min_recall_threshold']="'{}'".format(min_recall_threshold)
    statstracker.params['numercial_attr_filter_method']= "'{}'".format(numercial_attr_filter_method)
    statstracker.params['lca_s_max_size']="'{}'".format(lca_s_max_size)
    statstracker.params['lca_s_min_size']="'{}'".format(lca_s_min_size)
    statstracker.params['just_lca'] = "{}".format(str(lca_eval_mode))
    statstracker.params['exclude_high_cost_jg'] = "'{}'".format(exclude_high_cost_jg[1])
    statstracker.params['f1_calculation_type'] = "'{}'".format(f1_calculation_type)
    statstracker.params['f1_sample_rate'] = "'{}'".format(f1_sample_rate)
    statstracker.params['f1_min_sample_size_threshold'] = "'{}'".format(f1_min_sample_size_threshold)
    statstracker.params['f1_sample_type'] = "'{}'".format(f1_sample_type)
    statstracker.params['gui'] = "'{}'".format(str(gui))

    exp_time = datetime.now().strftime("%Y_%m_%d__%H_%M_%S")

    exp_desc = '__'.join([user_query[1], dbname, str(sample_rate_for_s), str(lca_s_max_size), str(lca_s_min_size), str(lca_eval_mode),
                str(maximum_edges), str(min_recall_threshold), str(numercial_attr_filter_method), 
                exclude_high_cost_jg[1], str(f1_calculation_type), str(f1_sample_rate), str(f1_min_sample_size_threshold)])


    logger.debug(exp_desc)
    logger.debug(user_questions_map)

    for k,v in statstracker.params.items():
      logger.debug(f'{k} : {v}')


    if(conn is None):
      conn = psycopg2.connect(f"dbname={dbname} user={user_name} password={password} port={port}")
      conn.autocommit = True
    else:
      conn = conn
      
    w = GProMWrapper(user= user_name, passwd=password, host=host, 
        port=port, db=dbname, frontend='', backend='postgres', options={})

    scj = Schema_Graph_Generator(conn)
    G = MultiGraph()
    sg, attr_dict = scj.generate_graph(G)
    pg = provenance_getter(conn = conn, gprom_wrapper = w, db_dict=attr_dict)

    if(not gui):
      pg.create_original_pt(user_query[0])
        
    user_pt_size, pt_dict, pt_relations = pg.gen_provenance_table(user_questions=user_questions, 
                                                    user_specified_attrs=user_specified_attrs)

    attr_dict['PT'] = pt_dict

    jgg = Join_Graph_Generator(schema_graph = sg, attr_dict = attr_dict, gwrapper=w)

    # logger.debug('generate new valid_jgs')
    valid_result = jgg.Generate_JGs(pt_rels=pt_relations, num_edges=maximum_edges, customize=False)
    
    # logger.debug(f"Before filtering any, we have {len(valid_result)} valid jgs \n")

    jgm = Join_Graph_Materializer(conn=conn, db_dict=attr_dict, gwrapper=w, user_query=user_query[0])
    jgm.init_cost_estimator()


    pgen = Pattern_Generator(conn)


    pattern_ranked_within_jg = {}
    jg_individual_times_dict = {}

    cost_friendly_jgs = []
    not_cost_friendly_jgs = []


    if(exclude_high_cost_jg[0]==False):
      # logger.debug("DO include high cost jg!!!!!!!!!!!!!")
      # logger.debug(f"before filtering intermediate, we have {len(valid_result)} jgs")

      intermediate_jgs = [v for v in valid_result if v.intermediate]
      valid_result = [v for v in valid_result if not v.intermediate]
      
      for ijg in intermediate_jgs:
        logger.debug(ijg)

      # logger.debug(f"after filtering out intermediate we have {len(valid_result)} valid jgs \n")


      jgm.stats.startTimer('materialize_jg')
      for n in valid_result:
        cost_estimate, renaming_dict, apt_q = jgm.materialize_jg(n)
        # logger.debug(n.ignored_attrs)
        if(apt_q is not None):
          n.cost = cost_estimate
          n.apt_create_q = apt_q
          n.renaming_dict = renaming_dict
        else:
          n.redundant = True
          continue
      jgm.stats.stopTimer('materialize_jg')

      # logger.debug(f"before filtering redundant, we have {len(valid_result)} jgs")
      valid_result = [v for v in valid_result if not v.redundant]
      jgg.stats.params['valid_jgs']=len(valid_result)
      if(gui):
        Create_Stats_Table(conn=conn, stats_trackers=[jgg.stats], stats_relation_name='total_jgs', schema=f"{result_schema}_gui")
        InsertStats(conn=conn, stats_trackers=[jgg.stats], stats_relation_name='total_jgs', schema=f"{result_schema}_gui", exp_time=exp_time, exp_desc=exp_desc)

      # logger.debug(f"after filtering out redund[]ant we have {len(valid_result)} valid jgs \n")

      # logger.debug(f'we found {len(valid_result)} valid join graphs, now materializing and generating patterns')
      
      jg_cnt=1

      for vr in valid_result:
        # if(str(vr)=='1: PT, 2: icustays| 2: icustays, 3: patients'):
        jg_individual_times_dict[vr] = 0
        jgm.stats.startTimer('materialize_jg')
        # logger.debug(f'we are on join graph number {jg_cnt}')
        jg_cnt+=1
        # logger.debug(vr)
        drop_if_exist_jg_view = "DROP MATERIALIZED VIEW IF EXISTS {} CASCADE;".format('jg_{}'.format(vr.jg_number))
        jg_query_view = "CREATE MATERIALIZED VIEW {} AS {}".format('jg_{}'.format(vr.jg_number), vr.apt_create_q)
        jgm.cur.execute(drop_if_exist_jg_view)
        jgm.cur.execute(jg_query_view)
        apt_size_query = f"SELECT count(*) FROM jg_{vr.jg_number}"
        jgm.cur.execute(apt_size_query)
        apt_size = int(jgm.cur.fetchone()[0])
        jgm.stats.stopTimer('materialize_jg')
        pgen.stats.startTimer('per_jg_timer')
        pgen.gen_patterns(jg=vr,
                            jg_name=f"jg_{vr.jg_number}", 
                            renaming_dict=vr.renaming_dict, 
                            skip_cols=vr.ignored_attrs, 
                            s_rate_for_s=sample_rate_for_s,
                            lca_s_max_size = lca_s_max_size,
                            lca_s_min_size = lca_s_min_size,
                            just_lca = lca_eval_mode,
                            lca_recall_thresh=min_recall_threshold,
                            numercial_attr_filter_method=numercial_attr_filter_method,
                            user_pt_size=user_pt_size,
                            original_pt_size=apt_size,
                            user_questions_map=user_questions_map,
                            f1_calculation_type=f1_calculation_type,
                            f1_sample_type = f1_sample_type,
                            f1_calculation_sample_rate=f1_sample_rate,
                            f1_calculation_min_size=f1_min_sample_size_threshold,
                            user_assigned_num_pred_cap=user_assigned_max_num_pred
                            )
        if(gui):
          logger.debug("gui mode! insert by jg!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
          patterns_to_insert = pgen.top_pattern_from_one_jg(vr)
          if(patterns_to_insert):
            InsertPatterns(conn=conn, exp_desc=exp_desc, patterns=patterns_to_insert, pattern_relation_name='topk_patterns_from_top_jgs', schema=result_schema, 
              exp_time=exp_time, result_type=f1_calculation_type)
        pgen.stats.stopTimer('per_jg_timer')
        jg_individual_times_dict[vr] = pgen.stats.time['per_jg_timer']
        pgen.stats.resetTimer('per_jg_timer')
      # logger.debug(jg_individual_times_dict)
    else:
      # cost_estimate_dict 
      valid_result = [v for v in valid_result if not v.intermediate]

      cost_estimate_dict = {i:[] for i in range(0,maximum_edges+1)}
      # logger.debug(cost_estimate_dict)
      for vr in valid_result:
        jgm.stats.startTimer('materialize_jg')
        cost_estimate, renaming_dict, apt_q = jgm.materialize_jg(vr,cost_estimate=True)
        if(apt_q is not None):
          vr.cost = cost_estimate
          vr.apt_create_q = apt_q
          vr.renaming_dict = renaming_dict
          cost_estimate_dict[vr.num_edges].append(vr.cost)
        else:
          vr.redundant=True
          continue
        jgm.stats.stopTimer('materialize_jg')

      valid_result = [v for v in valid_result if not v.redundant]

      # logger.debug(cost_estimate_dict)

      avg_cost_estimate_by_num_edges = {k:mean(v) for k,v in cost_estimate_dict.items() if v}
      # logger.debug(avg_cost_estimate_by_num_edges)
      # logger.debug(f'we found {len(valid_result)} valid join graphs, now materializing and generating patterns')
      jg_cnt=1

      valid_result = [n for n in valid_result if n.cost<=avg_cost_estimate_by_num_edges[n.num_edges]]
      jgg.stats.params['valid_jgs']=len(valid_result)
      if(gui):
        Create_Stats_Table(conn=conn, stats_trackers=[jgg.stats], stats_relation_name='total_jgs', schema=f"{result_schema}_gui")
        InsertStats(conn=conn, stats_trackers=[jgg.stats], stats_relation_name='total_jgs', schema=f"{result_schema}_gui", exp_time=exp_time, exp_desc=exp_desc)

      for n in valid_result:
        # logger.debug(f'we are on join graph number {jg_cnt}')
        # logger.debug(n)
        jg_cnt+=1
        # if(n.cost<=avg_cost_estimate_by_num_edges[n.num_edges]):
        jg_individual_times_dict[n] = 0
        cost_friendly_jgs.append(n) 
        jgm.stats.startTimer('materialize_jg')
        drop_if_exist_jg_view = "DROP MATERIALIZED VIEW IF EXISTS {} CASCADE;".format('jg_{}'.format(n.jg_number))
        jg_query_view = "CREATE MATERIALIZED VIEW {} AS {}".format('jg_{}'.format(n.jg_number), n.apt_create_q)
        jgm.cur.execute(drop_if_exist_jg_view)
        jgm.cur.execute(jg_query_view)
        jgm.stats.stopTimer('materialize_jg')
        apt_size_query = f"SELECT count(*) FROM jg_{n.jg_number}"
        jgm.cur.execute(apt_size_query)
        apt_size = int(jgm.cur.fetchone()[0])
        pgen.stats.startTimer('per_jg_timer')
        pgen.gen_patterns(jg=n,
                          jg_name=f"jg_{n.jg_number}", 
                          renaming_dict=n.renaming_dict, 
                          skip_cols=n.ignored_attrs, 
                          s_rate_for_s=sample_rate_for_s,
                          lca_s_max_size = lca_s_max_size,
                          lca_s_min_size = lca_s_min_size,
                          just_lca = lca_eval_mode,
                          lca_recall_thresh=min_recall_threshold,
                          numercial_attr_filter_method = numercial_attr_filter_method,
                          user_pt_size=user_pt_size,
                          original_pt_size = apt_size,
                          user_questions_map = user_questions_map,
                          f1_calculation_type = f1_calculation_type,
                          f1_calculation_sample_rate=f1_sample_rate,
                          f1_sample_type = f1_sample_type,
                          f1_calculation_min_size=f1_min_sample_size_threshold,
                          user_assigned_num_pred_cap=user_assigned_max_num_pred
                        )
        if(gui):
          logger.debug("gui mode! insert by jg!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
          patterns_to_insert = pgen.top_pattern_from_one_jg(n)
          if(patterns_to_insert):
            InsertPatterns(conn=conn, exp_desc=exp_desc, patterns=patterns_to_insert, pattern_relation_name='topk_patterns_from_top_jgs', schema=result_schema, 
            exp_time=exp_time, result_type=f1_calculation_type)
        pgen.stats.stopTimer('per_jg_timer')
        jg_individual_times_dict[vr] = pgen.stats.time['per_jg_timer']
        pgen.stats.resetTimer('per_jg_timer')

    if(lca_eval_mode):
      patterns_all = pgen.pattern_pool
    else:
      ranked_pattern_by_jg = pgen.rank_patterns(ranking_type = 'by_jg')

      top_k_from_each_jg = []

      for k,v in ranked_pattern_by_jg.items():
        # logger.debug(v)
        if(v):
          top_k_from_each_jg.extend(pgen.pattern_diversification(v))

      topk_from_top_jgs = pgen.topk_jg_patterns(num_jg=5, k_p=5, sortby='entropy')

      global_rankings = sorted(top_k_from_each_jg, key = lambda p: p['F1'], reverse=True)

      patterns_all = pgen.rank_patterns(ranking_type = 'global')

    logger.debug(f'total number of patterns {len(patterns_all)}')

    # collect stats 
    stats_trackers = [jgg.stats, jgm.stats, pgen.stats, statstracker]

    Create_Stats_Table(conn=conn, stats_trackers=stats_trackers, stats_relation_name='time_and_params', schema=result_schema)
    InsertStats(conn=conn, stats_trackers=stats_trackers, stats_relation_name='time_and_params', schema=result_schema, exp_time=exp_time, exp_desc=exp_desc)
    if(lca_eval_mode):
      InsertPatterns(conn=conn, exp_desc=exp_desc, patterns=patterns_all, pattern_relation_name='patterns', schema=result_schema, exp_time=exp_time, result_type='l')
    if(not lca_eval_mode):
      Create_jg_time_stats(conn=conn, stats_relation_name='jgs_time_dist', schema=result_schema)
      Insert_jg_time_stats(conn=conn, jg_time_dict=jg_individual_times_dict, stats_relation_name ='jgs_time_dist', schema=result_schema, exp_time=exp_time, exp_desc=exp_desc)
      InsertPatterns(conn=conn, exp_desc=exp_desc, patterns=global_rankings, pattern_relation_name='global_results', schema=result_schema, exp_time=exp_time, result_type=f1_calculation_type)
      InsertPatterns(conn=conn, exp_desc=exp_desc, patterns=patterns_all, pattern_relation_name='patterns', schema=result_schema, exp_time=exp_time, result_type=f1_calculation_type)
      if(not gui):
        InsertPatterns(conn=conn, exp_desc=exp_desc, patterns=topk_from_top_jgs, pattern_relation_name='topk_patterns_from_top_jgs', schema=result_schema, exp_time=exp_time, result_type=f1_calculation_type)
    # conn.close()


def drop_jg_views(conn):
    cur = conn.cursor()
    q_get_num_tree_views = """
    SELECT relname  FROM pg_class WHERE relname LIKE 'jg_%' AND relname NOT LIKE '%_s' 
    AND relname not LIKE '%_d' AND relname not LIKE '%_p' AND relkind = 'm';
    """
    cur.execute(q_get_num_tree_views)
    all_jg_names = cur.fetchall()
    logger.debug(all_jg_names)  
    for n in all_jg_names:
        q = "DROP MATERIALIZED VIEW {} CASCADE".format(n[0])
        cur.execute(q)
        conn.commit()

def main():

  parser = argparse.ArgumentParser(description='Running experiments of CaJaDe')

  parser.add_argument('-M','--maximum_edges', metavar="\b", type=int, default=3, 
    help='Maximum number of edges allowed in a join graph (default: %(default)s)')

  parser.add_argument('-F','--f1_sample_rate', metavar="\b", type=float, default=0.3, 
    help='Sample rate of apt when calculating the f1 score (default: %(default)s)')

  parser.add_argument('-w','--f1_sample_type', metavar="\b", type=str, default='weighted', 
    help='Sample type of apt when calculating the f1 score (default: %(default)s)')

  parser.add_argument('-z','--f1_sample_thresh', metavar="\b", type=int, default=100, 
    help='Sample threshold of APT, only sample if apt bigger than this (default: %(default)s)')

  parser.add_argument('-o','--optimized', metavar="\b", type=str, default='y', 
    help='use opt or not (y: yes, n: no), (default: %(default)s)')

  parser.add_argument('-i','--ignore_expensive', metavar="\b", type=str, default='true', 
    help='skip expensive jg or not, (default: %(default)s)')

  parser.add_argument('-m','--min_recall_threshold', metavar="\b", type=float, default=0.3, 
    help='recall threshold (default: %(default)s)')

  parser.add_argument('-r','--sample_rate_for_lca', metavar="\b", type=float, default=0.05, 
  help='sample rate for lca (default: %(default)s)')

  parser.add_argument('-s','--min_lca_s_size', metavar="\b", type=int, default=100,
    help='min size of sample used for lca cross product (default: %(default)s)')

  parser.add_argument('-S','--max_lca_s_size', metavar="\b", type=int, default=1000,
   help='min size of sample used for lca cross product (default: %(default)s)')

  parser.add_argument('-H','--db_host', metavar="\b", type=str, default='localhost',
    help='database host, (default: %(default)s)')

  parser.add_argument('-P','--port', metavar="\b", type=int, default=5432,
    help='database port, (default: %(default)s)')

  parser.add_argument('-D','--result_schema', metavar="\b", type=str, default="none",
    help='result_schema_name_prefix, (default: exp_[timestamp of the start]')

  parser.add_argument('-t','--f1_calc_type', metavar="\b", type=str, default='s',
    help='f1 score type (s sample, o original, e: evaluate_sample) (default: %(default)s)')

  parser.add_argument('-W','--workloads', metavar="\b", type=str, default='false',
    help='using questions from workloads? (default: %(default)s)')

  parser.add_argument('-C','--case_studies', metavar="\b", type=str, default='false',
    help='using questions from case studies? (default: %(default)s)')

  parser.add_argument('-L','--evaluate_lca_mode', metavar='\b', type=str, default='false',
    help='generate LCA results only, will not generate pattern results, (default: %(default)s)')
  
  requiredNamed = parser.add_argument_group('required named arguments')

  requiredNamed.add_argument('-U','--user_name', metavar="\b", type=str, required=True,
    help='owner of the database (required)')

  requiredNamed.add_argument('-p','--password', metavar="\b", type=str, required=True,
    help='password to the database (required)')

  requiredNamed.add_argument('-d','--db_name', metavar="\b", type=str, required=True,
    help='database name (required)')



  # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    #####
    ##print("colNum: "+colNum)
    #####
  user_query = "provenance of (select count(*) as win, s.season_name from team t, game g, season s where t.team_id = g.winner_id and g.season_id = s.season_id and t.team= 'GSW' group by s.season_name);"
  # u_query = (user_query, 'gsw wins : 15 vs 12') 
  u_query = (user_query, 'demo')
  u_question =["season_name='2015-16'","season_name='2012-13'"]
  user_specified_attrs = [('team','team'),('season','season_name')]



  # user_query = "provenance of (select team from team);"
  # # u_query = (user_query, 'gsw wins : 15 vs 12') 
  # u_query = (user_query, 'demo')
  # u_question = ["team='BOS'","team='DET'"]
  # user_specified_attrs = [('team', 'team')]


  # user_query = 'provenance of (select insurance, 1.0*SUM(hospital_expire_flag)/count(*) as death_rate from admissions group by insurance);'
  # u_query = (user_query, 'death rate: medicare vs private')
  # u_question =["insurance='Private'","insurance='Medicare'"]
  # user_specified_attrs = [('admissions',  'insurance'), ('admissions', 'hospital_expire_flag')]
  

  args=parser.parse_args()

  now=datetime.now()

  if(args.result_schema=='none'):
    str_time = now.strftime("%Y_%m_%d_%H_%M_%S")
    result_schema = f"exp_{str_time}"
  else:
    result_schema = args.result_schema

  if(args.ignore_expensive=='true'):
    exclude_high_cost_jg = (True, 't')
  else:
    exclude_high_cost_jg = (False, 'f')

  if(args.evaluate_lca_mode=='false'):
    eval_lca = False
  else:
    eval_lca = True


  if(args.workloads=='false' and args.case_studies=='false'):
    run_experiment(
      result_schema = result_schema,
      user_query = u_query,
      user_questions=u_question,
      # user_questions_map = {'yes':'Private', 'no':'Medicare'},
      user_questions_map = {'yes':'2015-16', 'no':'2012-13'},
      user_specified_attrs=user_specified_attrs,
      user_name=args.user_name,
      password=args.password,
      host=args.db_host,
      port=args.port,
      dbname=args.db_name, 
      sample_rate_for_s=args.sample_rate_for_lca,
      lca_s_max_size=args.max_lca_s_size,
      lca_s_min_size=args.min_lca_s_size, 
      maximum_edges=args.maximum_edges,
      min_recall_threshold=args.min_recall_threshold,
      numercial_attr_filter_method=args.optimized,
      user_assigned_max_num_pred = 2,
      exclude_high_cost_jg=exclude_high_cost_jg,
      f1_calculation_type =args.f1_calc_type,
      f1_sample_rate = args.f1_sample_rate,
      f1_sample_type = args.f1_sample_type,
      f1_min_sample_size_threshold=args.f1_sample_thresh,
      lca_eval_mode=eval_lca,
      )
  else:
    if(args.workloads!='false'):
      if(re.search(r'nba', args.db_name)):
        for w in nba_workloads:
          run_experiment(
            result_schema = result_schema,
            user_query = w['uquery'],
            user_questions= w['question'],
            user_questions_map = w['umap'],
            user_specified_attrs=w['uattrs'],
            user_name=args.user_name,
            password=args.password,
            host=args.db_host,
            port=args.port,
            dbname=args.db_name, 
            sample_rate_for_s=args.sample_rate_for_lca,
            lca_s_max_size=args.max_lca_s_size,
            lca_s_min_size=args.min_lca_s_size, 
            maximum_edges=args.maximum_edges,
            min_recall_threshold=args.min_recall_threshold,
            numercial_attr_filter_method=args.optimized,
            user_assigned_max_num_pred = 2,
            exclude_high_cost_jg=exclude_high_cost_jg,
            f1_calculation_type =args.f1_calc_type,
            f1_sample_rate = args.f1_sample_rate,
            f1_sample_type = args.f1_sample_type,
            f1_min_sample_size_threshold=args.f1_sample_thresh,
            lca_eval_mode=eval_lca,
            )
      if(re.search(r'mimic', args.db_name)):
        for w in mimic_workloads:
          run_experiment(
            result_schema = result_schema,
            user_query = w['uquery'],
            user_questions= w['question'],
            user_questions_map = w['umap'],
            user_specified_attrs=w['uattrs'],
            user_name=args.user_name,
            password=args.password,
            host=args.db_host,
            port=args.port,
            dbname=args.db_name, 
            sample_rate_for_s=args.sample_rate_for_lca,
            lca_s_max_size=args.max_lca_s_size,
            lca_s_min_size=args.min_lca_s_size, 
            maximum_edges=args.maximum_edges,
            min_recall_threshold=args.min_recall_threshold,
            numercial_attr_filter_method=args.optimized,
            user_assigned_max_num_pred = 2,
            exclude_high_cost_jg=exclude_high_cost_jg,
            f1_calculation_type =args.f1_calc_type,
            f1_sample_rate = args.f1_sample_rate,
            f1_sample_type = args.f1_sample_type,
            f1_min_sample_size_threshold=args.f1_sample_thresh,
            lca_eval_mode=eval_lca,
            )
      # else:
      #   for w in nba_cases:
      #     run_experiment(
      #       result_schema = result_schema,
      #       user_query = w['uquery'],
      #       user_questions= w['question'],
      #       user_questions_map = w['umap'],
      #       user_specified_attrs=w['uattrs'],
      #       user_name=args.user_name,
      #       password=args.password,
      #       host=args.db_host,
      #       port=args.port,
      #       dbname=args.db_name, 
      #       sample_rate_for_s=args.sample_rate_for_lca,
      #       lca_s_max_size=args.max_lca_s_size,
      #       lca_s_min_size=args.min_lca_s_size, 
      #       maximum_edges=args.maximum_edges,
      #       min_recall_threshold=args.min_recall_threshold,
      #       numercial_attr_filter_method=args.optimized,
      #       user_assigned_max_num_pred = 2,
      #       exclude_high_cost_jg=exclude_high_cost_jg,
      #       f1_calculation_type =args.f1_calc_type,
      #       f1_sample_rate = args.f1_sample_rate,
      #       f1_sample_type = args.f1_sample_type,
      #       f1_min_sample_size_threshold=args.f1_sample_thresh,
      #       lca_eval_mode=eval_lca,
      #       )

      #   for w in mimic_cases:
      #     run_experiment(
      #       result_schema = result_schema,
      #       user_query = w['uquery'],
      #       user_questions= w['question'],
      #       user_questions_map = w['umap'],
      #       user_specified_attrs=w['uattrs'],
      #       user_name=args.user_name,
      #       password=args.password,
      #       host=args.db_host,
      #       port=args.port,
      #       dbname=args.db_name, 
      #       sample_rate_for_s=args.sample_rate_for_lca,
      #       lca_s_max_size=args.max_lca_s_size,
      #       lca_s_min_size=args.min_lca_s_size, 
      #       maximum_edges=args.maximum_edges,
      #       min_recall_threshold=args.min_recall_threshold,
      #       numercial_attr_filter_method=args.optimized,
      #       user_assigned_max_num_pred = 2,
      #       exclude_high_cost_jg=exclude_high_cost_jg,
      #       f1_calculation_type =args.f1_calc_type,
      #       f1_sample_rate = args.f1_sample_rate,
      #       f1_sample_type = args.f1_sample_type,
      #       f1_min_sample_size_threshold=args.f1_sample_thresh,
      #       lca_eval_mode=eval_lca,
      #       )

if __name__ == '__main__':
  main()
