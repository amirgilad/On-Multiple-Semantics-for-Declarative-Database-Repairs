from Semantics.abs_sem import *
from z3 import parse_smt2_string, Optimize, Int, sat, BoolRef
import random
import string
import re


class IndependentSemantics(AbsSemantics):
    """This class implements independent semantics. This is the semantics of considering
    all possible assignments leading to delta tuples and finding the smallest set of
    tuples whose deletion will not enable any of the rules to be satisfied"""

    def __init__(self, db_conn, rules, tbl_names):
        super(IndependentSemantics, self).__init__(db_conn, rules, tbl_names)

        self.provenance = {}  # dict in the form {delta tuple: [assign1, assign2, ...]}
        self.prov_notations = {}  # dict in the form {notation: tuple}

    def find_mss(self, schema):
        """implementation of approximation algorithm for independent semantics.
        Store the provenance of all possible delta tuples as a CNF and find the
        smallest satisfying assignment using a SAT solver"""

        if not self.rules:   # verify more than 0 rules
            return set()

        # delete database and reload with all possible and impossible delta tuples
        self.db.delete_tables(self.delta_tuples.keys())
        self.db.load_database_tables(self.delta_tuples.keys(), is_delta=True)

        # convert the rules so they will store the provenance
        prov_rules, prov_tbls, proj = self.gen_prov_rules()

        # var to store the assignments
        assignments = []

        # use end semantics to derive all delta tuples and store the provenance
        changed = True
        derived_tuples = set()
        prev_len = 0
        while changed:
            for i in range(len(self.rules)):
                cur_rows = self.db.execute_query(prov_rules[i][1])
                cur_assignments = self.rows_to_prov(cur_rows, prov_tbls[i], schema, proj, prov_rules[i])
                for assignment in cur_assignments:
                    if assignment not in assignments:
                        assignments.append(assignment)
                        derived_tuples.add(assignment[0])
            if prev_len == len(derived_tuples):
                changed = False
            prev_len = len(derived_tuples)

        # process provenance into a formula
        self.process_provenance(assignments)
        bf = self.convert_to_bool_formula()

        # find minimum satisfying assignment
        sol, size = self.solve_boolean_formula_with_z3_smt2(bf)

        # process solution to mss
        mss = self.convert_sat_sol_to_mss(sol)
        return mss

    def gen_prov_rules(self):
        """convert every rule to a rule that outputs the provenance"""
        prov_rules = []
        prov_tbls = []
        proj = []
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

    def handle_assignment(self, row, assignment_tuples, schema, prov_tbls, rule):
        """convert a row from the result set into an assignment of tuples"""
        s = 0
        str_row = [str(e) for e in row]
        ans = ("", "")
        for tbl in prov_tbls:
            e = len(schema[tbl]) + s if 'delta_' not in tbl else len(schema[tbl[6:]]) + s
            # attrs = ",".join(["'" + t + "'" if "\r" not in t else "'" + t[:-4] + "'" for t in str_row[s:e]])
            attrs = ",".join([t if "\r" not in t else t[:-4] for t in str_row[s:e]])
            txt_tbl = (tbl, "(" + attrs + ")")
            # self.prov_notations[]
            assignment_tuples.append(txt_tbl)
            if rule[0] == txt_tbl[0]:
                ans = ("delta_" + txt_tbl[0], txt_tbl[1])
            s = e
        return assignment_tuples, ans

    def rows_to_prov(self, res, prov_tbls, schema, proj, rule):
        """separate every result row into provenance tuples"""
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
        """get the provenance of each tuple"""
        for assign in assignments:
            if assign[0] not in self.provenance:
                self.provenance[assign[0]] = []
            self.provenance[assign[0]].append(assign[1:])  # add assignment to the prov of this tuple

    def convert_to_bool_formula(self):
        """build the boolean formula based on the provenance"""
        def random_string(string_length=10):
            # Generate a random string of fixed length
            letters = string.ascii_lowercase
            return ''.join(random.choice(letters) for i in range(string_length))

        bf = "(or "  # the boolean formula that will be evaluated
        for delta_tup in self.provenance:
            assignments = self.provenance[delta_tup]
            if len(assignments) > 1:
                bf += "(or "
            for assign in assignments:
                bf += "(and " if len(assign) > 1 else ""
                for tup in assign:
                    if tup not in self.prov_notations:
                        if "delta_" in tup[0] and (tup[0][6:], tup[1]) in self.prov_notations:  # tup is a delta tuple and its regular counterpart has an annotation
                            self.prov_notations[tup] = "(not " + self.prov_notations[(tup[0][6:], tup[1])] + ")"
                        elif ("delta_" + tup[0], tup[1]) in self.prov_notations:  # symmetric case
                            self.prov_notations[tup] = self.prov_notations[("delta_" + tup[0], tup[1])][5:-1]
                        else:
                            annotation = random_string()
                            annotation = "(not " + annotation + ")" if "delta_" in tup[0] else annotation
                            self.prov_notations[tup] = annotation
                    bf += self.prov_notations[tup] + " "
                bf = bf[:-1] + ") " if len(assign) > 1 else bf[:-1] + " "
            if len(assignments) > 1:
                bf = bf[:-1] + ") "

        return "(not " + bf[:-1] + ")) "

    def solve_boolean_formula_with_z3_smt2(self, bf):
        """Find minimum satisfying assignemnt for the boolean formula.
        # Example:
        # >>> bf = '(and (or a b) (not (and a c)))'
        # >>> appeared_symbol_list = ['a', 'b', 'c']
        # >>> solve_boolean_formula_with_z3_smt2(bf, appeared_symbol_list)
        # ([b = True, a = False, c = False, s = 1], 1)
        """
        # print(bf)
        appeared_symbol_list = list(set([a if "not " not in a else a[5:-1] for a in self.prov_notations.values()]))
        declaration_str = '\n'.join(list(map(lambda x: '(declare-const {} Bool)'.format(x), appeared_symbol_list)))
        declaration_str += '\n(declare-const s Int)'
        declaration_str += '\n(define-fun b2i ((x Bool)) Int (ite x 1 0))'

        size_str = '(+ {})'.format(' '.join(list(map(lambda x: '(b2i {})'.format(x), appeared_symbol_list))))
        assert_str = '(assert {})\n'.format(bf)
        assert_str += '(assert (= s {}))\n(assert (>= s 0))'.format(size_str)  # changed from (> s 0)

        z3_bf = parse_smt2_string(declaration_str + '\n' + assert_str)
        opt = Optimize()
        opt.add(z3_bf)
        s = Int('s')
        opt.maximize(s)  # changed from opt.minimize(s)

        if opt.check() == sat:
            best_model = opt.model()
            min_size = 0
            for cl in best_model:
                if isinstance(best_model[cl], BoolRef) and best_model[cl]:
                    min_size += 1
            return best_model, min_size
        else:
            return None, -1

    def convert_sat_sol_to_mss(self, sol):
        """include in mss all literals in solution that get the value False"""
        notations_mss = set()
        s = sol.sexpr().replace("\n", "").replace("()", "")
        literals = re.findall('\(([^)]+)', s)
        for literal_val in literals:
            if "Int" not in literal_val:
                mid_exp = literal_val.replace("define-fun ", "").replace(" Bool", "")
                literal, val = mid_exp.split("  ")
                if val == " false":
                    notations_mss.add(literal)
        mss = set([t for t in self.prov_notations if self.prov_notations[t] in notations_mss])
        return mss
