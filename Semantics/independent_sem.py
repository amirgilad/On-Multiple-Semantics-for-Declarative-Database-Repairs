from Semantics.abs_sem import *


class IndependentSemantics(AbsSemantics):
    """This class implements independent semantics. This is the semantics of considering
    all possible assignemnts leading to delta tuples and finding the smallest set of
    tuples whose deletion will not enable any of the rules to be satisfied"""

    def __init__(self, db_conn, rules, tbl_names):
        super(IndependentSemantics, self).__init__(db_conn, rules, tbl_names)

    def find_mss(self):
        """implementation of approximation algorithm for independent semantics.
        Store the provenance of all possible delta tuples as a CNF and find the
        smallest satisfying assignment using a SAT solver"""
        mss = set()

        # delete database and reload with all possible and impossible delta tuples
        self.db.delete_tables(self.delta_tuples.keys())
        self.db.load_database_tables(self.delta_tuples.keys(), is_delta=True)

        # convert the rules so they will store the provenance
        prov_rules = self.convert_to_prov_rules()

        # set to store the provenance
        prov = set()

        # use end semantics to derive all delta tuples and store the provenance
        changed = True
        prev_len = 0
        while changed:
            for i in range(len(self.rules)):
                cur_prov = self.db.execute_query(self.rules[i][1])
                prov.update([(self.rules[i][0], row) for row in cur_prov])
                changed = prev_len != len(mss)
                prev_len = len(mss)
        return mss

    def gen_prov_rules(self):
        """convert every rule to a rule that outputs the provenance"""
        prov_rules = []
        for query in self.rules:
            q_parts = query[1].lower().split("from")
            proj = q_parts[0].split('select')[1].strip().split(',')
            proj = [e.strip() for e in proj]
            rest = q_parts[1].split("where")
            prov_tbls = rest[0].strip().split(',')
            prov_proj = ""
            prov_lst = []
            for tbl in prov_tbls:
                if "as" in tbl:
                    prov_proj += tbl.split("as")[1] + ".*, "
                    prov_lst.append(tbl.split("as")[0][:-1].strip())
                else:
                    prov_proj += tbl + ".*, "
            prov_proj = prov_proj[:-2]
            q_prov = "SELECT " + prov_proj + " FROM" + rest[0] + "WHERE" + rest[1]
            prov_rules.append((query[0], q_prov))
        return prov_rules
