from causalgraphicalmodels import CausalGraphicalModel
import logging
import re

logger = logging.getLogger(__name__)

class Causality:

    def __init__(self):
        self.causalGraph = CausalGraphicalModel(
            nodes = ["AGE", "SOD", "PRO", "SBP", "EYES", "HAIR"],
            edges = [("AGE", "SOD"), ("AGE", "SBP"), ("SOD", "PRO"), ("SBP", "PRO"), ("SOD", "SBP")]
        )
        self.relations = {'age': 'AGE', 'sodium_gr': 'SOD', 'protein': 'PRO', 'blood_pres': 'SBP',
                          'color_eyes': 'EYES', 'hair_color': 'HAIR'}


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
                if(not indep):
                    is_pattern_causal = True
                    continue

            logger.debug(pattern_desc)
            logger.debug(is_pattern_causal)


    def is_treatment(self, patterns, user_questions, conn):
        cur = conn.cursor()
        unit = re.compile('(.*)[=]').search(user_questions[0]).group(1)
        total_samples = {}
        counts = 0

        for pattern in patterns:
            is_treatment = True
            pattern_desc = pattern['desc']
            diff_var = pattern_desc.split('∧')
            clause, table = [], []

            for var in diff_var:
                if ('prov' in var):
                    matches = re.compile('prov_([A-z]*__[A-z]*?)_(.*)').search(var)
                    new_clause = matches.group(2).replace('__', '_').strip()
                    new_table = matches.group(1).replace('__', '_').strip()
                else:
                    new_clause = re.compile('\.(.*)').search(var).group(1).strip()
                    new_table = re.compile('(.*?)_[0-9]').search(var).group(1).strip()

                if (not new_clause in clause):
                    variable = re.compile('(.*)[=><]').search(new_clause).group(0)
                    value = re.compile('[=><](.*)').search(new_clause).group(1)
                    new_clause = variable + "'" + value + "'"
                    clause.append(new_clause)

                if (not new_table in table):
                    table.append(new_table)


            # check the percentage of counts (support)
            t_query = ' natural inner join '.join(table)
            if (not t_query in total_samples):
                samples_q = 'select count(*) from ' + t_query
                cur.execute(samples_q)
                samples = cur.fetchone()[0]
                total_samples[t_query] = samples

            c_query = ' and '.join(clause)

            query = 'select count(*) from ' + t_query + ' where ' + c_query
            cur.execute(query)
            patt_samples = cur.fetchone()[0]

            support = patt_samples / total_samples[t_query]

            if(support > 0.5):
                counts += 1

            logger.debug(pattern_desc)
        logger.debug(counts)