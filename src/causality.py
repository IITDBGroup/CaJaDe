from causalgraphicalmodels import CausalGraphicalModel
import logging
import re
import pandas

logger = logging.getLogger(__name__)


class Causality:

    def __init__(self):
        pass

    '''
    This function is used to get the average treatment effect of each pattern to see if it should be dropped or not
    '''

    def matching_patterns(self, patterns, dummy_patterns, user_specified_attrs, conn):
        cur = conn.cursor()

        jg_name_list = []
        rel_attr_dict = {}

        for pattern in patterns:
            logger.debug(pattern['desc'])

        for idx, pattern in enumerate(dummy_patterns):
            clauses = []
            jg = pattern['join_graph']
            renaming_dict = jg.renaming_dict
            jg_name = pattern['jg_name']

            if (jg_name not in jg_name_list):
                rel_attr_dict = self.get_rel_attr_dict(renaming_dict, rel_attr_dict, jg_name)
                jg_name_list.append(jg_name)

            clauses = self.get_dummy_clause(pattern, clauses)
            c_query = ' and '.join(clauses)
            ordinal_attr = ' '.join(self.get_ordinal_attributes(renaming_dict, rel_attr_dict[jg_name]))

            if (self.is_pattern_treated(cur, jg_name, c_query)):
                df = self.get_input_FLAME(conn, jg_name, c_query, ordinal_attr)

                logger.debug(pattern)
                # logger.debug(c_query)
                logger.debug(df)
                logger.debug("It's goood!!!")


    '''
    This function checks whether the pattern should be treated or discarded before calculating its ATE.
    This checking is done by calculating the average values that match the pattern in the APT.
    '''

    def is_pattern_treated(self, cur, jg_name, c_query):
        treated_query = f"""
        select avg(treated) between 0.2 and 0.8 as good_coverage 
        from (select 
        case when 
        (count(distinct(case when {c_query} then pnumber else 0 end))::float / 
        count(distinct pnumber)::float) 
        > 0.8 
        then 1 else 0 end treated 
        from {jg_name}
        group by a_2, season_name) as unit_treated;
        """

        # logger.debug(treated_query)

        cur.execute(treated_query)
        is_treated = cur.fetchone()[0]

        return is_treated

    '''
    This functions returns the DataFrame that is going to be feed to FLAME. For now, the DataFrame is composed of 
    the average of all the ordinal columns as well as the 'treated' column that tells us whether that row is treated
    or not
    '''

    def get_input_FLAME(self, conn, jg_name, c_query, ordinal_attr):
        treated_query = f"""
        select * 
        from (select 
        {ordinal_attr} case when 
        (count(distinct(case when {c_query} then pnumber else 0 end))::float / 
        count(distinct pnumber)::float) 
        > 0.8 
        then 1 else 0 end treated 
        from {jg_name}
        group by a_2, season_name) as unit_treated;
        """

        logger.debug(treated_query)

        treated = pandas.read_sql_query(treated_query, conn)

        return treated

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
