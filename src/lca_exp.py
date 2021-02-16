import pandas as pd
import psycopg2
from instrumentation import ExecStats
import argparse
import logging
import config

logger = logging.getLogger(__name__)


class ExperimentParams(ExecStats):
    """
    Statistics gathered during mining
    """

    TIMERS = {'time',
              }


    PARAMS = {'result_schema',
              'sample_size',
              'num_attrs',
              'num_result_p',
              'apt_size',
              'cnt_disincts'
              }

class lca_analyzer:
  
  def __init__(self, conn, schema):
    self.conn = conn
    self.cur = self.conn.cursor()
    self.conn.autocommit = True
    self.schema = schema

  def analyze(self, 
    lca_sample_size, 
    jg_name, 
    renaming_dict, 
    considered_attrs,
    stats_tracker=ExperimentParams()):

    exp_desc = '__'.join([str(lca_sample_size), jg_name])
    stats_tracker.params['result_schema'] = "'{}'".format(str(self.schema))
    stats_tracker.params['sample_size'] = "'{}'".format(str(lca_sample_size)) 

    apt_size_q = f"SELECT count(*) FROM {jg_name}"

    self.cur.execute(apt_size_q)

    stats_tracker.params['apt_size'] = "'{}'".format(str(int(self.cur.fetchone()[0])))

    attr_alias = 'a'

    attrs_in_d = ','.join(considered_attrs) 


    drop_prov_d = f"DROP MATERIALIZED VIEW IF EXISTS lca_{jg_name}_d_{lca_sample_size} CASCADE;"
    # logger.debug(drop_prov_d)
    self.cur.execute(drop_prov_d)

    drop_prov_s = f"DROP MATERIALIZED VIEW IF EXISTS lca_{jg_name}_s_{lca_sample_size} CASCADE;"
    # logger.debug(drop_prov_s)
    self.cur.execute(drop_prov_s)

    # make sure jg result is not empty            
    pattern_cond_attr_list = []
    nominal_pattern_attr_list = []
    cnt_disincts_list = []

    for attr in considered_attrs:
        if(renaming_dict[attr] == 'nominal'):
            one_attr_in_pattern = f"CASE WHEN l.{attr} = r.{attr} THEN l.{attr} ELSE NULL END AS {attr}"
            pattern_cond_attr_list.append(one_attr_in_pattern)
            nominal_pattern_attr_list.append(attr)
            cnt_disincts_list.append(f"COUNT(DISTINCT {attr})")

    cnt_disincts_q = f"SELECT {', '.join(cnt_disincts_list)} FROM {jg_name}"
    
    self.cur.execute(cnt_disincts_q)

    stats_tracker.params['cnt_disincts'] = "'({})'".format(', '.join([str(x) for x in self.cur.fetchone()]))

    stats_tracker.startTimer('time')

    prov_d_creation_q = f"""
    CREATE MATERIALIZED VIEW lca_{jg_name}_d_{lca_sample_size} AS
    (
        SELECT {attrs_in_d}
        FROM {jg_name} 
        ORDER BY RANDOM()
        LIMIT {lca_sample_size} 
    );

    """
    prov_s_creation_q = f"""
    CREATE MATERIALIZED VIEW lca_{jg_name}_s_{lca_sample_size} AS
    (
    SELECT {attrs_in_d}
    FROM {jg_name} 
    ORDER BY RANDOM()
    LIMIT {lca_sample_size} 
    );
    """

    self.cur.execute(prov_d_creation_q)
    self.cur.execute(prov_s_creation_q)

    pattern_q_selection_clause = ",".join(pattern_cond_attr_list)

    stats_tracker.params['num_attrs'] = "'{}'".format(str(len(pattern_cond_attr_list)))

    nominal_pattern_attr_clause = ",".join(nominal_pattern_attr_list)

    pattern_creation_q = f"""
    CREATE MATERIALIZED VIEW lca_{jg_name}_p_{lca_sample_size} AS
    WITH cp AS
    (
    SELECT 
    {pattern_q_selection_clause}
    FROM lca_{jg_name}_d_{lca_sample_size} l, lca_{jg_name}_s_{lca_sample_size} r
    )
    SELECT COUNT(*) AS pattern_freq, 
    {nominal_pattern_attr_clause}
    FROM cp
    GROUP BY
    {nominal_pattern_attr_clause}
    ORDER BY pattern_freq DESC
    limit 10;
    """

    self.cur.execute(pattern_creation_q)

    get_nominal_patterns_q = f"""
    SELECT {nominal_pattern_attr_clause} FROM lca_{jg_name}_p_{lca_sample_size};
    """

    stats_tracker.stopTimer('time')

    nominal_pattern_df = pd.read_sql(get_nominal_patterns_q, self.conn)
    logger.debug(nominal_pattern_df)

    nominal_pattern_dicts = nominal_pattern_df.to_dict('records')
    logger.debug(nominal_pattern_dicts)

    pattern_descs = []

    stats_tracker.params['num_result_p'] = "'{}'".format(str(len(nominal_pattern_dicts))) 

    for nom_p in nominal_pattern_dicts:
      p_desc = ','.join([f"{k}={v}" for k,v in nom_p.items()])
      pattern_descs.append(p_desc)
      logger.debug(p_desc)

    logger.debug(pattern_descs)

    self.Create_Stats_Table(stats_tracker, 'lca_exp_stats', exp_desc)
    self.InsertPatterns(exp_desc, pattern_descs, 'patterns')
    self.InsertStats(stats_tracker, 'lca_exp_stats', exp_desc)

  def Create_Stats_Table(self, stats_tracker, stats_relation_name, exp_desc):

      """
      stats_trackers: a list of objects keep tracking of the stats from experiment
      stats_relation_name: the relation name storing the stats, needs to be re-created if number of attrs change
      schema: the schema where stats_relation located in
      """

      self.cur.execute('CREATE SCHEMA IF NOT EXISTS '+self.schema)

      attr = ''

      timers_list = []
      counters_list = []
      params_list = []

      timers_list.extend(list(stats_tracker.time))
      counters_list.extend(list(stats_tracker.counters))
      params_list.extend(list(stats_tracker.params))

      stats_list = timers_list+counters_list+params_list
          
      for stat in stats_list:
          attr += stat+' varchar,'

      attr+='exp_desc varchar'
      
      self.cur.execute('create table IF NOT EXISTS ' + self.schema + '.' + stats_relation_name + ' (' +
                               'id serial primary key,' +
                               attr +');')

  def InsertPatterns(self, exp_desc, patterns, pattern_relation_name):

    cur = conn.cursor()

    cols = ['exp_desc', 'pattern']

    cols_with_types = ''

    for col in cols[:-1]:
        cols_with_types += col+' varchar,'

    cols_with_types+=cols[-1]+' varchar'

    self.cur.execute('create table IF NOT EXISTS ' + self.schema + '.' + pattern_relation_name + ' (' +
                             'id serial primary key,' +
                             cols_with_types +');')

    patterns_to_insert = []

    for p in patterns:
      p_print = p.replace("'","''")
      patterns_to_insert.append(f"('{exp_desc}',  '{p_print}')")

    self.cur.execute(
        'INSERT INTO ' + self.schema + '.' + pattern_relation_name + ' ('+ ','.join(cols) +')' + ' values '+ ', '.join(patterns_to_insert)
        )

  def InsertStats(self, stats_tracker, stats_relation_name, exp_desc):

      timers_vals = []
      counters_vals = []
      params_vals = []

      timers_attrs = []
      counters_attrs = []
      params_attrs = []


      total_time = 0

      timers_attrs.extend(list(stats_tracker.time))
      counters_attrs.extend(list(stats_tracker.counters))
      params_attrs.extend(list(stats_tracker.params))

      attr_list = timers_attrs+counters_attrs+params_attrs+['exp_desc']
      attrs = ','.join(attr_list)

      timers_vals.extend([str(round(x,2)) for x in stats_tracker.time.values()]) 
      counters_vals.extend([str(x) for x in stats_tracker.counters.values()])
      params_vals.extend([str(x) for x in stats_tracker.params.values()])

      values = ','.join(timers_vals+counters_vals+params_vals+[f"'{exp_desc}'"])
      # logger.debug(f'InsertStats: {values}')


      x =  'INSERT INTO ' + self.schema + '.' + stats_relation_name + ' ('+ attrs +')' + ' values('+ values +')'
      logger.debug(x)

      self.cur.execute(x)

  def finalize(self):
    find_max_sample_desc = f"""
    CREATE OR REPLACE VIEW {self.schema}.result AS
    select t.id,t.time,t.sample_size,t.apt_size,t.num_result_p,
    t.num_attrs,t.result_schema,t.exp_desc, count(*) as num_match
    from {self.schema}.lca_exp_stats t, {self.schema}.patterns as tp
    where tp.exp_desc = t.exp_desc and tp.pattern in
    (
    select pattern from {self.schema}.patterns where exp_desc = (
    select exp_desc  
    from {self.schema}.lca_exp_stats 
    order by sample_size::numeric 
    desc limit 1
    )
    )
    group by t.id,t.time,t.sample_size,t.apt_size,t.num_result_p,
    t.num_attrs,t.result_schema,t.exp_desc
    """  
    self.cur.execute(find_max_sample_desc)



if __name__ == '__main__':

  parser = argparse.ArgumentParser(description='Testing effectiveness of LCA sample size of CaJaDe')

  parser.add_argument('-j','--jg_name', metavar="\b", type=str, default='', 
    help='jg_name (default: %(default)s)')

  parser.add_argument('-s','--sample_size', metavar="\b", type=int, default=50, 
    help='Sample size for LCA (default: %(default)s)')

  parser.add_argument('-P','--port', metavar="\b", type=int, default=5432,
    help='database port, (default: %(default)s)')

  parser.add_argument('-f','--finalize', metavar="\b", type=bool, default=False,
    help='database port, (default: %(default)s)')

  parser.add_argument('-D','--result_schema', metavar="\b", type=str, default="none",
    help='result schema name that will be created in db, (default: exp_[timestamp of the start]')

  requiredNamed = parser.add_argument_group('required named arguments')

  requiredNamed.add_argument('-U','--user_name', metavar="\b", type=str, required=True,
    help='owner of the database (required)')

  requiredNamed.add_argument('-p','--password', metavar="\b", type=str, required=True,
    help='password to the database (required)')

  requiredNamed.add_argument('-d','--db_name', metavar="\b", type=str, required=True,
    help='database name (required)')

  args=parser.parse_args()


  conn = psycopg2.connect(f"dbname={args.db_name} user={args.user_name} port={args.port}")


  jn = args.jg_name
  ss = args.sample_size
  rs = args.result_schema

  nba_288_rd = {'win': 'nominal', 'season_name': 'nominal', 'a_1': 'ordinal', 'a_2': 'nominal', 'a_3': 'nominal', 'a_4': 'ordinal', 'a_5': 'ordinal',
   'a_6': 'ordinal', 'a_7': 'ordinal', 'a_8': 'ordinal', 'a_9': 'ordinal', 'a_10': 'ordinal', 'a_11': 'ordinal', 'a_12': 'ordinal', 'a_13': 'nominal',
    'a_14': 'nominal', 'pnumber': 'nominal', 'is_user': 'nominal', 'a_15': 'ordinal', 'a_16': 'ordinal', 'a_17': 'ordinal', 'a_18': 'ordinal', 
    'a_19': 'ordinal', 'a_20': 'ordinal', 'a_21': 'ordinal', 'a_22': 'ordinal', 'a_23': 'ordinal', 'a_24': 'ordinal', 'a_25': 'ordinal', 'a_26': 
    'ordinal', 'a_27': 'ordinal', 'a_28': 'ordinal', 'a_29': 'ordinal', 'a_30': 'ordinal', 'a_31': 'ordinal', 'a_32': 'ordinal', 'a_33': 'ordinal', 
    'a_34': 'ordinal', 'a_35': 'ordinal', 'a_36': 'ordinal', 'a_37': 'ordinal', 'a_38': 'ordinal', 'a_39': 'ordinal', 'a_40': 'ordinal', 'a_41': 
    'ordinal', 'a_42': 'ordinal', 'a_43': 'ordinal', 'a_44': 'ordinal', 'a_45': 'ordinal', 'a_46': 'ordinal', 'a_47': 'ordinal', 'a_48': 'ordinal', 
    'a_49': 'ordinal', 'a_50': 'ordinal', 'a_51': 'ordinal', 'a_52': 'ordinal', 'a_53': 'ordinal', 'a_54': 'ordinal', 'a_55': 'ordinal', 'a_56': 'ordinal',
     'a_57': 'ordinal', 'a_58': 'ordinal', 'a_59': 'ordinal', 'a_60': 'nominal', 'a_61': 'ordinal', 'a_62': 'ordinal', 'a_63': 'ordinal', 'a_64': 'ordinal', 
     'a_65': 'ordinal', 'a_66': 'ordinal', 'a_67': 'ordinal', 'a_68': 'ordinal', 'a_69': 'ordinal', 'a_70': 'ordinal', 'a_71': 'ordinal', 'a_72': 'ordinal', 'a_73': 'ordinal', 
     'a_74': 'ordinal', 'a_75': 'ordinal', 'a_76': 'ordinal', 'a_77': 'ordinal', 'a_78': 'ordinal', 'a_79': 'ordinal', 'a_80': 'ordinal', 'a_81': 'ordinal', 'a_82': 'ordinal', 
     'a_83': 'nominal'}

  nba_288_ca = ['a_4', 'a_5', 'a_6', 'a_7', 'a_14', 'a_15', 'a_16', 'a_17', 'a_18', 'a_19', 'a_20', 'a_21', 'a_22', 'a_23', 'a_24', 'a_25', 'a_26', 'a_27', 'a_28', 
  'a_29', 'a_30', 'a_32', 'a_33', 'a_34', 'a_35', 'a_36', 'a_37', 'a_38', 'a_39', 'a_40', 'a_41', 'a_42', 'a_43', 'a_44', 'a_45', 'a_46', 'a_47', 'a_48', 'a_49', 'a_50', 'a_51', 'a_52', 'a_53', 'a_54', 'a_55', 
  'a_56', 'a_57', 'a_58', 'a_61', 'a_62', 'a_63', 'a_64', 'a_65', 'a_66', 'a_67', 'a_68', 'a_69', 'a_70', 'a_71', 'a_72', 'a_73', 'a_74', 'a_75', 'a_76', 'a_77', 'a_78', 'a_79', 'a_80', 'a_81', 'a_83']


  nba_31_rd = {'win': 'nominal', 'season_name': 'nominal', 'a_1': 'ordinal', 'a_2': 'nominal', 'a_3': 'nominal', 'a_4': 'ordinal', 'a_5': 'ordinal', 'a_6': 'ordinal',
   'a_7': 'ordinal', 'a_8': 'ordinal', 'a_9': 'ordinal', 'a_10': 'ordinal', 'a_11': 'ordinal', 'a_12': 'ordinal', 'a_13': 'nominal', 
   'a_14': 'nominal', 'pnumber': 'nominal', 'is_user': 'nominal', 'a_15': 'ordinal', 'a_16': 'ordinal', 'a_17': 'ordinal', 'a_18': 'ordinal', 'a_19': 'nominal'}
  
  nba_31_ca = ['a_4', 'a_5', 'a_6', 'a_7', 'a_14', 'a_16', 'a_19']


  mimic_24_rd = {'insurance': 'nominal', 'death_rate': 'nominal', 'a_1': 'ordinal', 'a_2': 'nominal', 'a_3': 'nominal', 'a_4': 'nominal', 'a_5': 'nominal', 'a_6': 'nominal', 
  'a_7': 'nominal', 'a_8': 'nominal', 'a_9': 'nominal', 'a_10': 'nominal', 'a_11': 'nominal', 'a_12': 'nominal', 'a_13': 'ordinal', 'a_14': 'ordinal', 
  'pnumber': 'nominal', 'is_user': 'nominal', 'a_15': 'ordinal', 'a_16': 'nominal', 'a_17': 'nominal', 'a_18': 'nominal', 'a_19': 'ordinal', 'a_20': 'ordinal',
   'a_21': 'nominal', 'a_22': 'nominal', 'a_23': 'nominal', 'a_24': 'nominal', 'a_25': 'nominal', 'a_26': 'nominal', 'a_27': 'ordinal'}


  mimic_24_ca = ['a_2', 'a_3', 'a_4', 'a_5', 'a_6', 'a_7', 'a_9', 'a_10', 'a_11', 'a_12', 'a_14', 'a_15', 'a_16', 'a_17', 'a_18',
   'a_21', 'a_22', 'a_23', 'a_24', 'a_25', 'a_26']


  mimic_9_rd = {'insurance': 'nominal', 'death_rate': 'nominal', 'a_1': 'ordinal', 'a_2': 'nominal', 'a_3': 'nominal', 'a_4': 'nominal', 'a_5': 'nominal',
    'a_6': 'nominal', 'a_7': 'nominal', 'a_8': 'nominal', 'a_9': 'nominal', 'a_10': 'nominal', 'a_11': 'nominal', 'a_12': 'nominal', 'a_13': 'ordinal', 'a_14': 'ordinal',
     'pnumber': 'nominal', 'is_user': 'nominal', 'a_15': 'ordinal', 'a_16': 'ordinal', 'a_17': 'ordinal', 'a_18': 'nominal', 'a_19': 'nominal', 'a_20': 'nominal', 
     'a_21': 'nominal', 'a_22': 'nominal', 'a_23': 'nominal', 'a_24': 'nominal', 'a_25': 'nominal', 'a_26': 'ordinal'}

  mimic_9_ca = ['a_2', 'a_3', 'a_4', 'a_5', 'a_6', 'a_7', 'a_9', 'a_10', 'a_11', 'a_12', 'a_14', 'a_18', 'a_19', 'a_20', 'a_21', 'a_22', 'a_23', 'a_24', 'a_25']


  mimic_1_rd = {'insurance': 'nominal', 'death_rate': 'nominal', 'a_1': 'ordinal', 'a_2': 'nominal', 'a_3': 'nominal', 'a_4': 'nominal', 'a_5': 'nominal', 'a_6': 'nominal', 
  'a_7': 'nominal', 'a_8': 'nominal', 'a_9': 'nominal', 'a_10': 'nominal', 'a_11': 'nominal', 'a_12': 'nominal', 'a_13': 'ordinal', 'a_14': 'ordinal', 
  'pnumber': 'nominal', 'is_user': 'nominal'}
  
  mimic_1_ca = ['a_2', 'a_3', 'a_4', 'a_5', 'a_6', 'a_7', 'a_9', 'a_10', 'a_11', 'a_12', 'a_14']


  la = lca_analyzer(conn, rs)
  
  # nba 
  if(args.db_name=='nba_lca'):
    if(jn=='jg_288'):
      if(args.finalize==True):
        la.finalize()
      else:
        la.analyze(lca_sample_size=ss, jg_name=jn, renaming_dict=nba_288_rd, considered_attrs=nba_288_ca)

    elif(jn=='jg_31'):
      if(args.finalize==True):
        la.finalize()
      else:
        la.analyze(lca_sample_size=ss, jg_name=jn, renaming_dict=nba_31_rd, considered_attrs=nba_31_ca)
  
  if(args.db_name=='mimic_lca'):
    if(jn=='jg_24'):
      if(args.finalize==True):
        la.finalize()
      else:
        la.analyze(lca_sample_size=ss, jg_name=jn, renaming_dict=mimic_24_rd, considered_attrs=mimic_24_ca)

    elif(jn=='jg_9'):
      if(args.finalize==True):
        la.finalize()
      else:
        la.analyze(lca_sample_size=ss, jg_name=jn, renaming_dict=mimic_9_rd, considered_attrs=mimic_9_ca)
    elif(jn=='jg_1'):
      if(args.finalize==True):
        la.finalize()
      else:
        la.analyze(lca_sample_size=ss, jg_name=jn, renaming_dict=mimic_1_rd, considered_attrs=mimic_1_ca)

  # mimic
  # la.analyze(lca_sample_size=ss, jg_name=jn, renaming_dict=rd, considered_attrs=ca)
  # la.analyze(lca_sample_size=ss, jg_name=jn, renaming_dict=rd, considered_attrs=ca)

