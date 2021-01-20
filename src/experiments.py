from sg_generator import Schema_Graph_Generator
from networkx import MultiGraph
import networkx as nx
import psycopg2
from provenance_getter import provenance_getter
from gprom_wrapper import  GProMWrapper
from jg_generator import Join_Graph_Generator
from jg_materializer import Join_Graph_Materializer
from pattern_generator import Pattern_Generator
import colorful
import logging 
from renaming import encode
import re
import colorful
import random
import config
from instrumentation import ExecStats
from statistics import mean 
import argparse
from datetime import datetime
from time import strftime


logger = logging.getLogger(__name__)


class ExperimentParams(ExecStats):
    """
    Statistics gathered during mining
    """

    PARAMS = {'result_schema', # this should align with the push
              'user_questions',
              'dbname',
              'sample_rate_for_s',
              'max_sample_factor',
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
        
    for stat in stats_list[:-1]:
        attr += stat+' varchar,'

    attr+=stats_list[-1]+' varchar'

    
    cur.execute('create table IF NOT EXISTS ' + schema + '.' + stats_relation_name + ' (' +
                             'id serial primary key,' +
                             attr +');')

def InsertPatterns(conn, exp_desc, patterns, pattern_relation_name, schema):

  # logger.debug(patterns[0:5])

  cur = conn.cursor()
  cols = ['exp_desc', 'is_user', 'jg', 'jg_name', 'num_edges', 'p_desc', 'recall', 'precision', 'fscore', 'sample_recall']
  cols_with_types = ''

  for col in cols[:-1]:
      cols_with_types += col+' varchar,'

  cols_with_types+=cols[-1]+' varchar'

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
      patterns_to_insert.append(f"('{exp_desc}', '{p['is_user']}', '{jg_print}', '{p['jg_name']}', '{p['num_edges']}', '{p_print}', \
        '{p['recall']}', '{p['precision']}','{p['F1']}','{p['sample_recall']}')")
      continue
    else:
      cur.execute(
          'INSERT INTO ' + schema + '.' + pattern_relation_name + ' ('+ ','.join(cols) +')' + ' values '+ ', '.join(patterns_to_insert)
          )
      patterns_to_insert = []
      patterns_to_insert.append(f"('{exp_desc}', '{p['is_user']}', '{jg_print}', '{p['jg_name']}', '{p['num_edges']}', '{p_print}', \
        '{p['recall']}', '{p['precision']}','{p['F1']}','{p['sample_recall']}')")

      cur_batch_size=1

  if(patterns_to_insert):
    cur.execute(
        'INSERT INTO ' + schema + '.' + pattern_relation_name + ' ('+ ','.join(cols) +')' + ' values '+ ', '.join(patterns_to_insert)
        )        


def InsertStats(conn, stats_trackers, stats_relation_name, schema):

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

    attr_list = timers_attrs+counters_attrs+params_attrs+['total']
    attrs = ','.join(attr_list)

    for stats_tracker in stats_trackers:
        for v in stats_tracker.time.values():
            total_time+=float(v)
        timers_vals.extend([str(round(x,2)) for x in stats_tracker.time.values()]) 
        counters_vals.extend([str(x) for x in stats_tracker.counters.values()])
        params_vals.extend([str(x) for x in stats_tracker.params.values()])

    values = ','.join(timers_vals+counters_vals+params_vals+[str(total_time)])
    # logger.debug(f'InsertStats: {values}')

    cur = conn.cursor()
    # logger.debug('INSERT INTO ' + schema + '.' + stats_relation_name + ' ('+ attrs +')' + ' values('+ values +')')
    cur.execute(
        'INSERT INTO ' + schema + '.' + stats_relation_name + ' ('+ attrs +')' + ' values('+ values +')'
        )


def run_experiment(result_schema,
                   user_query,
                   user_questions,
                   user_questions_map,
                   user_specified_attrs,
                   user_name,
                   password,
                   host,
                   port,
                   dbname, 
                   sample_rate_for_s,
                   maximum_edges,
                   min_recall_threshold,
                   numercial_attr_filter_method,
                   f1_sample_rate,
                   exclude_high_cost_jg = (False, 'f'),
                   f1_calculation_type = 'o',
                   user_assigned_max_num_pred = 3,
                   f1_min_sample_size_threshold=100,
                   max_sample_factor=5, 
                   statstracker=ExperimentParams()):
    
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
    statstracker.params['max_sample_factor'] = "'{}'".format(max_sample_factor)
    statstracker.params['exclude_high_cost_jg'] = "'{}'".format(exclude_high_cost_jg[1])
    statstracker.params['f1_calculation_type'] = "'{}'".format(f1_calculation_type)
    statstracker.params['f1_sample_rate'] = "'{}'".format(f1_sample_rate)
    statstracker.params['f1_min_sample_size_threshold'] = "'{}'".format(f1_min_sample_size_threshold)


    exp_desc = '__'.join([user_query[1], dbname, str(sample_rate_for_s), 
                str(max_sample_factor), str(maximum_edges), str(min_recall_threshold), str(numercial_attr_filter_method), 
                exclude_high_cost_jg[1], str(f1_calculation_type), str(f1_sample_rate), str(f1_min_sample_size_threshold)])


    logger.debug(exp_desc)

    for k,v in statstracker.params.items():
      logger.debug(f'{k} : {v}')


    conn = psycopg2.connect(f"dbname={dbname} user={user_name} password={password} port={port}")

    conn.autocommit = True

    w = GProMWrapper(user= user_name, passwd=password, host=host, 
        port=port, db=dbname, frontend='', backend='postgres', options={})

    scj = Schema_Graph_Generator(conn)
    G = MultiGraph()
    sg, attr_dict = scj.generate_graph(G)
    pg = provenance_getter(conn = conn, gprom_wrapper = w, db_dict=attr_dict)
        
    pt_size, pt_dict, pt_relations = pg.gen_provenance_table(query=user_query[0],
                                                    user_questions=user_questions, 
                                                    user_specified_attrs=user_specified_attrs)

    attr_dict['PT'] = pt_dict

    jgg = Join_Graph_Generator(schema_graph = sg, attr_dict = attr_dict, gwrapper=w)

    logger.debug('generate new valid_jgs')
    valid_result = jgg.Generate_JGs(pt_rels=pt_relations, num_edges=maximum_edges, customize=False)
    jgm = Join_Graph_Materializer(conn=conn, db_dict=attr_dict, gwrapper=w, user_query=user_query[0])
    jgm.init_cost_estimator()

    pgen = Pattern_Generator(conn) 

    pattern_ranked_within_jg = {}

    cost_friendly_jgs = []
    not_cost_friendly_jgs = []


    if(exclude_high_cost_jg[0]==False):
      valid_result = [v for v in valid_result if not v.intermediate]
      for n in valid_result:
        jgm.stats.startTimer('materialize_jg')
        cost_estimate, renaming_dict, apt_q = jgm.materialize_jg(n)
        if(apt_q is not None):
          n.cost = cost_estimate
          n.apt_create_q = apt_q
          n.renaming_dict = renaming_dict
        else:
          n.redundant = True
          continue
          
      valid_result = [v for v in valid_result if not v.redundant]

      for vr in valid_result:
        drop_if_exist_jg_view = "DROP MATERIALIZED VIEW IF EXISTS {} CASCADE;".format('jg_{}'.format(vr.jg_number))
        jg_query_view = "CREATE MATERIALIZED VIEW {} AS {}".format('jg_{}'.format(vr.jg_number), vr.apt_create_q)
        jgm.cur.execute(drop_if_exist_jg_view)
        jgm.cur.execute(jg_query_view)
        jgm.stats.stopTimer('materialize_jg')
        pgen.gen_patterns(jg=vr,
                          jg_name=f"jg_{vr.jg_number}", 
                          renaming_dict=vr.renaming_dict, 
                          skip_cols=vr.ignored_attrs, 
                          s_rate_for_s=sample_rate_for_s,
                          pattern_recall_threshold=min_recall_threshold,
                          numercial_attr_filter_method=numercial_attr_filter_method,
                          max_sample_factor=max_sample_factor,
                          original_pt_size=pt_size,
                          user_questions_map=user_questions_map,
                          f1_calculation_type=f1_calculation_type,
                          f1_calculation_sample_rate=f1_sample_rate,
                          f1_calculation_min_size=f1_min_sample_size_threshold,
                          user_assigned_num_pred_cap=user_assigned_max_num_pred
                          )
    else:
      # cost_estimate_dict 
      valid_result = [v for v in valid_result if not v.intermediate]

      cost_estimate_dict = {i:[] for i in range(0,maximum_edges+1)}
      # logger.debug(cost_estimate_dict)
      for vr in valid_result:
            cost_estimate, renaming_dict, apt_q = jgm.materialize_jg(vr,cost_estimate=True)
            if(apt_q is not None):
              vr.cost = cost_estimate
              vr.apt_create_q = apt_q
              vr.renaming_dict = renaming_dict
              cost_estimate_dict[vr.num_edges].append(vr.cost)
            else:
              vr.redundant=True
              continue
      valid_result = [v for v in valid_result if not v.redundant]

      avg_cost_estimate_by_num_edges = {k:mean(v) for k,v in cost_estimate_dict.items()}
      logger.debug(f'we found {len(valid_result)} valid join graphs, now materializing and generating patterns')
      jg_cnt=1
      for n in valid_result:
        logger.debug(f'we are on join graph number {jg_cnt}')
        logger.debug(n)
        jg_cnt+=1
        if(n.intermediate==False):
          if(n.cost<=avg_cost_estimate_by_num_edges[n.num_edges]*1.5 and not n.redundant):
            cost_friendly_jgs.append(n) 
            jgm.stats.startTimer('materialize_jg')
            drop_if_exist_jg_view = "DROP MATERIALIZED VIEW IF EXISTS {} CASCADE;".format('jg_{}'.format(n.jg_number))
            jg_query_view = "CREATE MATERIALIZED VIEW {} AS {}".format('jg_{}'.format(n.jg_number), n.apt_create_q)
            jgm.cur.execute(drop_if_exist_jg_view)
            jgm.cur.execute(jg_query_view)
            jgm.stats.stopTimer('materialize_jg')
            pgen.gen_patterns(jg=n,
                              jg_name=f"jg_{n.jg_number}", 
                              renaming_dict=n.renaming_dict, 
                              skip_cols=n.ignored_attrs, 
                              s_rate_for_s=sample_rate_for_s,
                              pattern_recall_threshold=min_recall_threshold,
                              numercial_attr_filter_method = numercial_attr_filter_method,
                              max_sample_factor = max_sample_factor,
                              original_pt_size = pt_size,
                              user_questions_map = user_questions_map,
                              f1_calculation_type = f1_calculation_type,
                              f1_calculation_sample_rate=f1_sample_rate,
                              f1_calculation_min_size=f1_min_sample_size_threshold,
                              user_assigned_num_pred_cap=user_assigned_max_num_pred
                              )
          else:
            not_cost_friendly_jgs.append(n)

      jgg.stats.params['valid_jgs_cost_high']=len(not_cost_friendly_jgs)

    ranked_pattern_by_jg = pgen.rank_patterns(ranking_type = 'by_jg')

    topk_from_top_jgs = pgen.topk_avg_jg_patterns(num_jg=5, k_p=5, sortby='entropy')

    patterns_all = pgen.rank_patterns(ranking_type = 'global')

    logger.debug(f'total number of patterns {len(patterns_all)}')

    # collect stats 
    stats_trackers = [jgg.stats, jgm.stats, pgen.stats, statstracker]

    Create_Stats_Table(conn=conn, stats_trackers=stats_trackers, stats_relation_name='time_and_params', schema=result_schema)
    InsertStats(conn=conn, stats_trackers=stats_trackers, stats_relation_name='time_and_params', schema=result_schema)
    InsertPatterns(conn=conn, exp_desc=exp_desc, patterns=patterns_all, pattern_relation_name='patterns', schema=result_schema)
    InsertPatterns(conn=conn, exp_desc=exp_desc, patterns=topk_from_top_jgs, pattern_relation_name='topk_patterns_from_top_jgs', schema=result_schema)
    conn.close()


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


if __name__ == '__main__':

  parser = argparse.ArgumentParser(description='Running experiments of CaJaDe')

  parser.add_argument('-M','--maximum_edges', metavar="\b", type=int, default=3, 
    help='Maximum number of edges allowed in a join graph (default: %(default)s)')

  parser.add_argument('-F','--f1_sample_rate', metavar="\b", type=float, default=1.0, 
    help='Sample rate of apt when calculating the f1 score (default: %(default)s)')

  parser.add_argument('-o','--optimized', metavar="\b", type=str, default='y', 
    help='use opt or not (y: yes, n: no), (default: %(default)s)')

  parser.add_argument('-s','--db_size', metavar="\b", type=float, default=1.0, 
    help='scale factor of database, (default: %(default)s)')

  parser.add_argument('-m','--min_recall_threshold', metavar="\b", type=float, default=0.5, 
    help='recall threshold when calculating f1 score (default: %(default)s)')

  parser.add_argument('-r','--sample_rate_for_lca', metavar="\b", type=float, default=0.05, 
  help='sample rate for lca (default: %(default)s)')

  parser.add_argument('-f','--sample_factor_for_lca', metavar="\b", type=float, default=2, 
  help='sample factor for lca (default: %(default)s)')

  parser.add_argument('-H','--db_host', metavar="\b", type=str, default='localhost',
    help='database host, (default: %(default)s)')

  parser.add_argument('-P','--port', metavar="\b", type=int, default=5432,
    help='database port, (default: %(default)s)')

  parser.add_argument('-D','--result_schema', metavar="\b", type=str, default="none",
    help='result_schema_name_prefix, (default: exp_[timestamp of the start]')

  parser.add_argument('-t','--f1_calc_type', metavar="\b", type=str, default='s',
    help='f1 score type (s sample, o original, e: evaluate_sample) (default: %(default)s)')
  
  requiredNamed = parser.add_argument_group('required named arguments')

  requiredNamed.add_argument('-U','--user_name', metavar="\b", type=str, required=True,
    help='owner of the database (required)')

  requiredNamed.add_argument('-p','--password', metavar="\b", type=str, required=True,
    help='password to the database (required)')

  requiredNamed.add_argument('-d','--db_name', metavar="\b", type=str, required=True,
    help='database name (required)')


# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

  user_query = "provenance of (select count(*) as win, s.season_name from team t, game g, season s where t.team_id = g.winner_id and g.season_id = s.season_id and t.team= 'GSW' group by s.season_name);"
  u_query = (user_query, 'n1')
  u_question =["season_name='2015-16'","season_name='2012-13'"]
  user_specified_attrs = (('team','team'),('season','season_name'))
  max_sample_factor = 2
  exclude_high_cost_jg = (False,'f')

  args=parser.parse_args()

  now=datetime.now()

  if(args.result_schema=='none'):
    str_time = now.strftime("%Y-%m-%d-%H-%M-%S")
    result_schema = f"exp_{str_time}"
  else:
    result_schema = args.result_schema

  run_experiment(
    result_schema = result_schema,
    user_query = u_query,
    user_questions=u_question,
    user_questions_map = {'yes':'2015-16', 'no':'2012-13'},
    user_specified_attrs=user_specified_attrs,
    user_name=args.user_name,
    password=args.password,
    host=args.db_host,
    port=args.port,
    dbname=args.db_name, 
    sample_rate_for_s=args.sample_rate_for_lca,
    max_sample_factor=args.sample_factor_for_lca, 
    maximum_edges=args.maximum_edges,
    min_recall_threshold=args.min_recall_threshold,
    numercial_attr_filter_method=args.optimized,
    user_assigned_max_num_pred = 2,
    exclude_high_cost_jg=exclude_high_cost_jg,
    f1_calculation_type =args.f1_calc_type,
    f1_sample_rate = args.f1_sample_rate,
    f1_min_sample_size_threshold=1000,
    )
  # logger.debug('\n\n')
