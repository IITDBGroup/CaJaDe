from causalgraphicalmodels import CausalGraphicalModel
import logging
import re

logger = logging.getLogger(__name__)


class Causality:

    def __init__(self):
        pass

    def matching_patterns(self, patterns, user_specified_attrs, conn):
        cur = conn.cursor()

        for pattern in patterns:
            # jg = pattern['join_graph']
            jg_name = pattern['jg_name']
            # renaming_dict = jg.renaming_dict
            ren_attr = pattern['pattern_attr_mappings']

            logger.debug(jg_name)
            logger.debug(ren_attr)

            pattern_desc = pattern['desc']
            diff_var = pattern_desc.split('∧')
            clauses, dummy_clause = [], []

            for var in diff_var:
                clauses = self.get_clause(var, clauses)

            logger.debug(clauses)

            for c in clauses:
                variable = c.split("=")[0]




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
