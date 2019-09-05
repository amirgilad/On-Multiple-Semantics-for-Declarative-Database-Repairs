from Semantics.abs_sem import *


class IndependentSemantics(AbsSemantics):
    """This class implements independent semantics. This is the semantics of considering
    all possible assignemnts leading to delta tuples and finding the smallest set of
    tuples whose deletion will not enable any of the rules to be satisfied"""

    def __init__(self, db_conn, rules, tbl_names):
        super(IndependentSemantics, self).__init__(db_conn, rules, tbl_names)

        self.provenance = {}# dict in the form {delta tuple: [assign1, assign2, ...]}
        self.prov_notations = {}# dict in the form {notation: tuple}

    def find_mss(self, schema):
        """implementation of approximation algorithm for independent semantics.
        Store the provenance of all possible delta tuples as a CNF and find the
        smallest satisfying assignment using a SAT solver"""
        mss = set()

        # delete database and reload with all possible and impossible delta tuples
        self.db.delete_tables(self.delta_tuples.keys())
        self.db.load_database_tables(self.delta_tuples.keys(), is_delta=True)

        # convert the rules so they will store the provenance
        prov_rules, prov_tbls, proj = self.gen_prov_rules()

        # var to store the assignments
        assignments = []

        # use end semantics to derive all delta tuples and store the provenance
        changed = True
        prev_len = 0
        while changed:
            for i in range(len(self.rules)):
                cur_rows = self.db.execute_query(prov_rules[i][1])
                cur_assignments = self.rows_to_prov(self, cur_rows, prov_tbls[i], schema, proj, prov_rules[i])
                assignments.append(cur_assignments)
                changed = prev_len != len(mss)
                prev_len = len(mss)

        # process provenance into a formula


        return mss

    def gen_prov_rules(self):
        """convert every rule to a rule that outputs the provenance"""
        prov_rules = []
        prov_tbls = []
        for i in range(len(self.rules)):
            query = self.rules[i]
            q_parts = query[1].lower().split("from")
            proj = q_parts[0].split('select')[1].strip().split(',')
            proj = [e.strip() for e in proj]
            rest = q_parts[1].split("where")
            prov_tbls.append([tbl.strip() for tbl in rest[0].strip().split(',')])
            prov_proj = ""
            prov_lst = []
            for tbl in prov_tbls[i]:
                if "as" in tbl:
                    prov_proj += tbl.split("as")[1] + ".*, "
                    prov_lst.append(tbl.split("as")[0][:-1].strip())
                else:
                    prov_proj += tbl + ".*, "
            prov_proj = prov_proj[:-2]
            q_prov = "SELECT " + prov_proj + " FROM" + rest[0] + "WHERE" + rest[1]
            prov_rules.append((query[0], q_prov))
        return prov_rules, prov_tbls, proj

    def handle_assignment(self, row, example_tuples, schema, prov_tbls, rule):
        s = 0
        str_row = [str(e) for e in row]
        ans = ("", "")
        for tbl in prov_tbls:
            e = len(schema[tbl]) + s
            attrs = ",".join(["'" + t + "'" if "\r" not in t else "'" + t[:-4] + "'" for t in str_row[s:e]])
            txt_tbl = (tbl, "(" + attrs + ")")
            # self.prov_notations[]
            example_tuples.append(txt_tbl)
            if rule[0] == txt_tbl[0]:
                ans = ("delta_" + txt_tbl[0], txt_tbl[1])
            s = e
        return example_tuples, ans

    def rows_to_prov(self, res, prov_tbls, schema, proj, rule):
        # separate every result row into provenance tuples
        proj_attrs = []
        for p in proj:
            t = tuple(p.split("."))
            proj_attrs.append(t)
        assignments = []
        for i in range(len(res)):
            example_tuples = []
            row = res[i]
            example_tuples, ans = self.handle_assignment(row, example_tuples, schema, prov_tbls, rule)
            example_tuples = [ans] + example_tuples
            assignments.append(example_tuples)
        return assignments

    def process_provenance(self, assignments):
        # get the provenance of all tuples
        for assign in assignments:
            if assign[0] not in self.provenance:
                self.provenance[assign[0]] = []
            self.provenance[assign[0]].append(assign[1:])# add assignment to the prov of this tuple

