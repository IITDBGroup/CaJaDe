from causalgraphicalmodels import CausalGraphicalModel
import logging
import re
import pandas
import math
import dame_flame
from src.instrumentation import ExecStats


logger = logging.getLogger(__name__)


class MatchingPatternsGeneratorStats(ExecStats):
    TIMERS = {'calculate_ate',
              'input_flame',
              'sort_dict'
              }

class Improvements:

    def __init__(self):
        self.dummy_pattern_pool = []
        self.attr_ranges = {}
        self.stats = MatchingPatternsGeneratorStats()

    '''
    This function creates a new table from the JG (materialized view) applying to each column the corresponding summarization
    function.
    The renaming dict, and the mapping_table is used to summarize the columns and to do the mapping from the columns to
    its appropriate summarization function. After the mapping is done, a query to create the table is constructed and executed.
    '''
    def create_new_APT(self, jg, conn):
        logger.debug(f'This are the details for the jg_{jg.jg_number} -> {repr(jg)}')

        cur = conn.cursor()
        summ_query, select_query = [], []
        self.attr_ranges[f'summ_jg_{jg.jg_number}'] = {}
        range_q = """
        SELECT n1, min({0}), max({0}) 
        FROM (SELECT {0}, NTILE(3) OVER (ORDER BY {0}) AS n1 FROM 
        (SELECT avg({0})::numeric(10,3) as {0} 
        FROM jg_{1} GROUP BY is_user) as grouped_table) as test_table GROUP BY n1
        """

        # we get the mapping table values
        summ_val_q = "select * from mapping_summ"
        summ_map_val = pandas.read_sql_query(summ_val_q, conn)

        for key, value in jg.renaming_dict.items():
            if (key == 'max_rel_index' or key == 'max_attr_index' or key == 'dtypes'):
                continue
            else:
                for dummy, attr in jg.renaming_dict[key]['columns'].items():
                    # Check the summarization function for each attribute and construct the query accordingly
                    attr = self.get_variable(attr)
                    summ_func = summ_map_val.loc[summ_map_val['attributes'] == attr, 'summ_function'].iloc[0]

                    if  summ_func == 'drop':
                        continue
                    else:
                        if summ_func == 'keep':
                            summ_query.append(f'{dummy}')
                            if (attr == 'win_ratio'):
                                select_query.append(f'avg({dummy})::numeric(10,5) as {dummy}')
                            else:
                                select_query.append(f'{dummy}')
                        else:
                            summ_query.append(f'{summ_func} OVER (ORDER BY {dummy}) as {dummy}')
                            select_query.append(f'avg({dummy}) as {dummy}')

                        ranges = pandas.read_sql_query(range_q.format(dummy, jg.jg_number), conn)
                        self.attr_ranges[f'summ_jg_{jg.jg_number}'][dummy] = ranges

        # use the query made to create the table from the jg_number
        drop_new_apt = f"DROP TABLE IF EXISTS summ_jg_{jg.jg_number} CASCADE;"
        new_apt_query = f"""
        CREATE TABLE summ_jg_{jg.jg_number} AS
        SELECT {','.join(summ_query)}
        FROM (
        SELECT {','.join(select_query)} FROM jg_{jg.jg_number}
        GROUP BY is_user) as grouped_table
        """

        logger.debug(new_apt_query)
        cur.execute(drop_new_apt)
        cur.execute(new_apt_query)


    def gen_patterns_v2(self, jg, jg_name, conn, skip_cols, original_pt_size, attr_alias='a', lca_s_max_size = 1000,
                      lca_s_min_size = 100, s_rate_for_s=0.1):
        cur = conn.cursor()

        get_attrs_q = f"""
        select atr.attname
        from pg_class mv
            join pg_namespace ns on mv.relnamespace = ns.oid
            join pg_attribute atr 
                on atr.attrelid = mv.oid 
                and atr.attnum > 0 
                and not atr.attisdropped
        where mv.relkind = 'r'
        and mv.relname = '{jg_name}'
        """

        cur.execute(get_attrs_q)
        attrs = [x[0] for x in cur.fetchall()]

        considered_attrs_s = [x for x in attrs if x not in skip_cols and re.search(r'{}_'.format(attr_alias), x)]

        considered_attrs_d = [x for x in attrs if (x not in skip_cols and re.search(r'{}_'.format(attr_alias), x))
                              or (x == 'is_user' or x == 'pnumber')]
        attrs_in_d = ','.join(considered_attrs_d)

        drop_prov_d = f"DROP MATERIALIZED VIEW IF EXISTS {jg_name}_d CASCADE;"
        cur.execute(drop_prov_d)

        drop_prov_s = f"DROP MATERIALIZED VIEW IF EXISTS {jg_name}_s CASCADE;"
        cur.execute(drop_prov_s)

        lca_sample_size = max([min(math.ceil(original_pt_size * s_rate_for_s), lca_s_max_size), lca_s_min_size])

        if (lca_sample_size != 0):
            pattern_cond_attr_list = []
            pattern_attr_list = []

            for attr in considered_attrs_s:
                one_attr_in_pattern = f"CASE WHEN l.{attr} = r.{attr} THEN l.{attr} ELSE NULL END AS {attr}"
                pattern_cond_attr_list.append(one_attr_in_pattern)
                pattern_attr_list.append(attr)

            pattern_q_selection_clause = ",".join(pattern_cond_attr_list)
            pattern_attr_clause = ",".join(pattern_attr_list)

            prov_d_creation_q = f"""
            CREATE MATERIALIZED VIEW {jg_name}_d AS (
                SELECT {attrs_in_d}
                FROM {jg_name} 
                ORDER BY RANDOM()
                LIMIT {lca_sample_size});
            """
            cur.execute(prov_d_creation_q)

            prov_s_creation_q = f"""
            CREATE MATERIALIZED VIEW {jg_name}_s AS (
                SELECT {attrs_in_d}
                FROM {jg_name} 
                ORDER BY RANDOM()
                LIMIT {lca_sample_size});
            """
            cur.execute(prov_s_creation_q)

            pattern_creation_q = f"""
            CREATE MATERIALIZED VIEW {jg_name}_p AS
            WITH cp AS (
            SELECT {pattern_q_selection_clause}
            FROM {jg_name}_d l, {jg_name}_s r)
            SELECT COUNT(*) AS pattern_freq,
            {pattern_attr_clause}
            FROM cp
            GROUP BY {pattern_attr_clause}
            ORDER BY pattern_freq DESC
            limit 30;
            """

            get_patterns_q = f' SELECT {pattern_attr_clause} FROM {jg_name}_p;'

            cur.execute(pattern_creation_q)

            pattern_df = pandas.read_sql_query(get_patterns_q, conn)
            pattern_dicts = pattern_df.to_dict('records')

            pattern_dict_list = []
            for pa in pattern_dicts:
                pa_dict = {}
                pa_dict['values'] = [[k, v] for k, v in pa.items() if (v is not None and not pandas.isnull(v))]
                if (pa_dict['values']):
                    pa_dict['join_graph'] = jg
                    pa_dict['jg_name'] = jg_name
                    pattern_dict_list.append(pa_dict)

            # create a pattern with the jg, jg_name, renaming dict, the patterns itself and append it to the pool
            for pattern in pattern_dict_list:
                self.dummy_pattern_pool.append(pattern)


    # This function is used to get the average treatment effect of each pattern to see if it should be dropped or not
    def matching_patterns(self, dummy_patterns, conn):
        cur = conn.cursor()
        ate_dict = {}

        for pattern in dummy_patterns:
            clauses = []
            jg = pattern['join_graph']
            renaming_dict = jg.renaming_dict
            jg_name = pattern['jg_name']

            for true_p in pattern['values']:
                clause = true_p[0] + '=' + str(true_p[1])
                clauses.append(clause)

            c_query = ' and '.join(clauses)

            if (self.is_pattern_treated(cur, jg_name, c_query)):
                real_pattern = self.translate_pattern(pattern, jg_name, renaming_dict)
                logger.debug(f'This is the real pattern {real_pattern}')

                self.stats.startTimer('input_flame')
                df = self.get_input_FLAME(conn, jg_name, c_query)
                self.stats.stopTimer('input_flame')

                self.stats.startTimer('calculate_ate')
                ate = self.calculate_ATE(df)
                self.stats.stopTimer('calculate_ate')

                if ate > 0:
                    ate_dict[real_pattern] = {}
                    ate_dict[real_pattern]['ATE'] = ate
                    ate_dict[real_pattern]['jg_details'] = f'This are the details for the jg_{jg.jg_number} -> {repr(jg)}'

        self.stats.startTimer('sort_dict')
        dict_sorted = self.sort_dict(ate_dict)
        # logger.debug(f'This is the dict for the ATE {dict_sorted}')
        self.stats.stopTimer('sort_dict')


    '''
    This function checks whether the pattern should be treated or discarded before calculating its ATE.
    This checking is done by calculating the average values that match the pattern in the APT.
    '''
    def is_pattern_treated(self, cur, jg_name, c_query):
        treated_query = f"""
        select avg(treated) between 0.2 and 0.8 as good_coverage 
        from (select 
        case when {c_query}
        then 1 else 0 end treated 
        from {jg_name}) as unit_treated;
        """

        cur.execute(treated_query)
        is_treated = cur.fetchone()[0]

        return is_treated

    '''
    This functions returns the DataFrame that is going to be feed to FLAME. For now, the DataFrame is composed of 
    the average of all the ordinal columns as well as the 'treated' column that tells us whether that row is treated
    or not
    '''
    def get_input_FLAME(self, conn, jg_name, c_query):
        # get the columns from the jg_name
        attr_to_keep = []

        column_query = f"select column_name from information_schema.columns where table_name='{jg_name}'"
        columns = pandas.read_sql_query(column_query, conn)

        for column in list(columns['column_name']):
            if column == 'is_user':
                continue
            elif column == 'win_ratio':
                attr_to_keep.append(f'{column} as outcome')
            else:
                attr_to_keep.append(f'{column}')

        flame_query = f"select * from (select {','.join(attr_to_keep)}, case when {c_query} then 1 else 0 end treated from {jg_name}) as unit_treated; "

        df = pandas.read_sql_query(flame_query, conn)

        return df


    '''
    This function calculates the ATE from FLAME giving a df
    '''
    def calculate_ATE(self, df):
        model = dame_flame.matching.FLAME(early_stop_un_c_frac=0.1, early_stop_un_t_frac=0.1)
        model.fit(df)
        result = model.predict(df)

        logger.debug(f'This is the result: {result}')

        # logger.debug(f'This is unit per group {model.units_per_group}')

        if len(model.units_per_group) == 0:
            return 0

        ate = dame_flame.utils.post_processing.ATE(matching_object=model)

        logger.debug(f'This is the ATE for the pattern: {ate}')

        return ate

    '''
    This function checks whether the pattern is causally independent from the outcome variable or not
    '''
    def check_causality(self, patterns, outcome_var):
        '''
        We receive the patterns generated and together with the causal graph we determine whether the variables
        inside the patterns are causally independent from the outcome variable or not.
        '''
        for pattern in patterns:
            is_pattern_causal = False
            pattern_desc = pattern['desc']
            diff_var = pattern_desc.split('∧')

            for var in diff_var:
                if ('prov' in var):
                    var = re.compile('info_(.*)[=><]').search(var).group(1)
                    var = var.replace('__', '_')
                else:
                    var = re.compile('\.(.*)[=><]').search(var).group(1)

                indep = self.causalGraph.is_d_separated(self.relations.get(var), self.relations.get(outcome_var), {})
                if (not indep):
                    is_pattern_causal = True
                    continue

            logger.debug(pattern_desc)
            logger.debug(is_pattern_causal)


    '''
    This function checks the support of each pattern to see if the pattern is good enough or if it should be 
    dropped
    '''
    def is_treatment(self, patterns, user_questions, conn):
        cur = conn.cursor()
        # unit = re.compile('(.*)[=]').search(user_questions[0]).group(1)
        total_samples = {}
        counts, queries = 0, 0
        good_patterns = []

        for pattern in patterns:
            logger.debug(pattern['jg_name'])
            pattern_desc = pattern['desc']
            logger.debug(pattern_desc)
            diff_var = pattern_desc.split('∧')
            clause, table = [], []

            for var in diff_var:
                table = self.get_table(var, table)
                clause = self.get_clause(var, clause)
                # table, clause = self.get_table_and_clause(var, table, clause)

            # check the percentage of counts (support)
            t_query = ' natural inner join '.join(table)
            t_query_u1 = t_query + ' where ' + user_questions[0]
            t_query_u2 = t_query + ' where ' + user_questions[1]

            if (not t_query_u1 in total_samples):
                queries += 2
                samples_q = 'select count(*) from ' + t_query_u1
                cur.execute(samples_q)
                samples = cur.fetchone()[0]
                total_samples[t_query_u1] = samples

                samples_q = 'select count(*) from ' + t_query_u2
                cur.execute(samples_q)
                samples = cur.fetchone()[0]
                total_samples[t_query_u2] = samples

            c_query = ' and '.join(clause)

            query_u1 = 'select count(*) from ' + t_query + ' where ' + c_query + ' and ' + user_questions[0]
            query_u2 = 'select count(*) from ' + t_query + ' where ' + c_query + ' and ' + user_questions[1]
            logger.debug(query_u1)
            logger.debug(query_u2)

            # 1st unit
            cur.execute(query_u1)
            patt_samples = cur.fetchone()[0]
            support_u1 = patt_samples / total_samples[t_query_u1]

            cur.execute(query_u2)
            patt_samples = cur.fetchone()[0]
            support_u2 = patt_samples / total_samples[t_query_u2]

            logger.debug(support_u1)
            logger.debug(support_u2)

            if ((support_u1 > 0.5 and support_u2 < 0.5) or (support_u1 < 0.5 and support_u2 > 0.5)):
                good_patterns.append(pattern_desc)
                counts += 1

        for k, v in total_samples.items():
            logger.debug(k)
            logger.debug(v)

        logger.debug(counts)
        logger.debug(queries)

        for pt in good_patterns:
            logger.debug(pt)


    ''' 
    This function retrieves the table and the clause that has to be used for a pattern
    '''
    def get_table_and_clause(self, var, table, clause):
        if ('prov' in var):
            matches = re.compile('prov_([A-z]*__)*([A-z]*?)_(.*)').search(var)  # prov_([A-z]*__[A-z]*?)_(.*)
            if (matches.group(1)):
                new_table = (matches.group(1) + matches.group(2)).replace('__', '_').strip()
            else:
                new_table = matches.group(2).replace('__', '_').strip()

            new_clause = matches.group(3).replace('__', '_').strip()
        else:
            new_clause = re.compile('\.(.*)').search(var).group(1).strip()
            new_table = re.compile('(.*?)_[0-9]').search(var).group(1).strip()

        if (not new_clause in clause):
            variable = re.compile('(.*)[=><]').search(new_clause).group(0)
            value = re.compile('[=><](.*)').search(new_clause).group(1)
            if "'" in value:
                value = value.replace("'", "''")

            if '.' in value and variable != 'player_name=':
                new_clause = variable + value
            else:
                new_clause = variable + "'" + value + "'"

            clause.append(new_clause)

        if (not new_table in table):
            table.append(new_table)

        return (table, clause)


    ''' 
    This function retrieves the table that has to be used for a pattern
    '''
    def get_table(self, var, table):
        if ('prov' in var):
            matches = re.compile('prov_([A-z]*__)*([A-z]*?)_(.*)').search(var)  # prov_([A-z]*__[A-z]*?)_(.*)
            if (matches.group(1)):
                new_table = (matches.group(1) + matches.group(2)).replace('__', '_').strip()
            else:
                new_table = matches.group(2).replace('__', '_').strip()

        else:
            new_table = re.compile('(.*?)_[0-9]').search(var).group(1).strip()

        if (not new_table in table):
            table.append(new_table)

        return table


    ''' 
    This function retrieves the clause that has to be used for a pattern
    '''
    def get_clause(self, var, clause):
        if ('prov' in var):
            matches = re.compile('prov_([A-z]*__)*([A-z]*?)_(.*)').search(var)  # prov_([A-z]*__[A-z]*?)_(.*)
            new_clause = matches.group(3).replace('__', '_').strip()
        else:
            new_clause = re.compile('\.(.*)').search(var).group(1).strip()

        if (not new_clause in clause):
            variable = re.compile('(.*)[=><]').search(new_clause).group(0)
            value = re.compile('[=><](.*)').search(new_clause).group(1)
            if "'" in value:
                value = value.replace("'", "''")

            if '.' in value and variable != 'player_name=':
                new_clause = variable + value
            else:
                new_clause = variable + "'" + value + "'"

            clause.append(new_clause)

        return clause

    def get_variable(self, var):
        if ('prov' in var):
            matches = re.compile('prov_([A-z]*__)*([A-z]*?)_(.*)').search(var)  # prov_([A-z]*__[A-z]*?)_(.*)
            variable = matches.group(3).replace('__', '_').strip()
        else:
            variable = var # re.compile('\.(.*)').search(var).group(1).strip()

        return variable


    def get_dummy_clause(self, pattern, clauses):
        for nt in pattern['nominal_values']:
            if "'" in nt[1]:
                nt[1] = nt[1].replace("'", "''")

            new_clause = nt[0] + "='" + nt[1] + "'"
            if (not new_clause in clauses):
                clauses.append(new_clause)

        if ('ordinal_values' in pattern):
            for ot in pattern['ordinal_values']:
                new_clause = ot[0] + ot[1] + str(ot[2])
                if (not new_clause in clauses):
                    clauses.append(new_clause)

        return clauses


    def get_ordinal_attributes(self, renaming_dict, rel_attr_dict):
        ordinal_attr = []
        attr_list = []

        for key, value in renaming_dict['dtypes'].items():
            if value == 'ordinal' and key not in ['season_name', 'pnumber', 'is_user']:
                attr = rel_attr_dict[key]
                if ('prov' in attr):
                    matches = re.compile('prov_([A-z]*__)*([A-z]*?)_(.*)').search(attr)
                    attr = matches.group(3).replace('__', '_').strip()

                if (attr not in attr_list):
                    key = f'''avg({key})::numeric(10,2) as {attr},'''
                    ordinal_attr.append(key)
                    attr_list.append(attr)

        return ordinal_attr


    def get_rel_attr_dict(self, renaming_dict, rel_attr_dict, jg_name):
        rel_dummy_to_attr = {}

        for key, value in renaming_dict.items():
            if (key == 'max_rel_index' or key == 'max_attr_index' or key == 'dtypes'):
                continue
            else:
                for dummy, attr in renaming_dict[key]['columns'].items():
                    rel_dummy_to_attr[dummy] = attr

        rel_attr_dict[jg_name] = rel_dummy_to_attr

        return rel_attr_dict

    def translate_pattern(self, pattern, jg_name, renaming_dict):
        final_pattern = []
        for true_p in pattern['values']:
            df = self.attr_ranges[jg_name][true_p[0]]
            range = f"[{df.loc[df['n1'] == true_p[1], 'min'].iloc[0]}, {df.loc[df['n1'] == true_p[1], 'max'].iloc[0]}]"
            # logger.debug(f'This is the range for the pattern {range}')

            for key, value in renaming_dict.items():
                if (key == 'max_rel_index' or key == 'max_attr_index' or key == 'dtypes'):
                    continue
                else:
                    for dummy, attr in renaming_dict[key]['columns'].items():
                        if dummy == true_p[0]:
                            real_name = self.get_variable(attr)

            # logger.debug(f'This is the real pattern: {real_name} between {range}')
            final_pattern.append(f'{real_name} between {range}')

        return ' and '.join(final_pattern)

    def sort_dict(self, d):
        new_dict = {}
        for w in sorted(d, key=lambda x: d[x]['ATE'], reverse=True):
            logger.debug('-> Pattern: ' + w + '. ATE: ' + str(d[w]['ATE']) + '\n-> JG_details: ' + d[w]['jg_details'] + '\n')
            new_dict[w] = d[w]

        return(new_dict)