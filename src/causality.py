from causalgraphicalmodels import CausalGraphicalModel


class Causality():

    def __init__(self):
        self.causalGraph = CausalGraphicalModel(
            nodes=["AGE", "SOD", "PRO", "SBP", "EYES"],
            edges=[("AGE", "SOD"), ("AGE", "SBP"), ("SOD", "PRO"), ("SBP", "PRO"), ("SOD", "SBP")]
        )


    def check_causality(self, patterns, outcome_var):
        '''
        We receive the patterns generated and together with the causal graph we determine whether the variables
        inside the patterns are causally independent from the outcome variable or not.
        '''
        for pattern in patterns:
            for var in pattern:
                independence = self.causalGraph.is_d_separated(var, outcome_var, {})
                if (independence):
                    # We should put the column 'keep_pattern' to false
                    keep_pattern = False
                    continue

            keep_pattern = True