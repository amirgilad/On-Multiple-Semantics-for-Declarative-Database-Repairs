from Semantics.step_sem import StepSemantics
from database_generator.dba import DatabaseEngine
import unittest


class TestStepSemantics(unittest.TestCase):

    schema = {"author": ('aid',
                         'name',
                         'oid'),
              "publication": ('pid',
                              'title',
                              'year'),

              "writes": ('aid', 'pid'),

              "organization": ('oid',
                               'name'),

              "conference": ('cid',
                             'mas_id',
                             'name',
                             'full_name',
                             'homepage',
                             'paper_count',
                             'citation_count',
                             'importance'),

              "domain_conference" : ('cid', 'did'),

              "domain" : ('did',
                          'name',
                          'paper_count',
                          'importance')
              }

    def test_undefined_connection(self):
        """test no db connection"""
        rules = [("author", "SELECT * FROM author WHERE author.aid = 58525;")]
        tbl_names = ["organization", "author", "publication", "writes"]
        db = None
        self.assertRaises(AssertionError, StepSemantics, db, rules, tbl_names)

    def test_no_rules(self):
        """test no rules case. MSS supposed to be empty as db is stable"""
        rules = []
        tbl_names = ["organization", "author", "publication", "writes"]
        db = DatabaseEngine("cr")
        step_sem = StepSemantics(db, rules, tbl_names)
        mss = step_sem.find_mss(self.schema)

        self.assertEqual(mss, set(), "MSS supposed to be empty! Instead its " + str(mss))

    def test_gen_prov_rules(self):
        """test func that takes delta rules and turns them to rules that return provenance"""
        rules = [("author", "SELECT author.* FROM author, writes WHERE author.name LIKE '%m%' AND author.aid = writes.aid;")]
        tbl_names = ["organization", "author", "publication", "writes"]
        db = DatabaseEngine("cr")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        step_sem = StepSemantics(db, rules, tbl_names)
        prov_rule = step_sem.gen_prov_rules()[0][0][1]
        func_prov_results = db.execute_query(prov_rule)
        desired_prov_results = db.execute_query("SELECT author.*, writes.* FROM author, writes WHERE author.name LIKE '%m%' AND author.aid = writes.aid;")
        self.assertEqual(func_prov_results, desired_prov_results)

    def test_rows_to_prov(self):
        """test func that handles and stores the provenance of each tuple"""
        rules = [("author", "SELECT author.* FROM author, writes WHERE author.name LIKE '%m%' AND author.aid = writes.aid;")]
        tbl_names = ["organization", "author", "publication", "writes"]
        db = DatabaseEngine("cr")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        step_sem = StepSemantics(db, rules, tbl_names)
        prov_rules, prov_tbls, proj = step_sem.gen_prov_rules()
        cur_prov = db.execute_query(prov_rules[0][1])
        assignments = step_sem.rows_to_prov(cur_prov, prov_tbls[0], self.schema, proj, prov_rules[0])
        self.assertTrue(all(a[0][0] == 'delta_author' for a in assignments))

    # def test_solve_boolean_formula_with_z3_smt2_not(self):
    #     """test func that finds the minimum satisfying assignment to a boolean formula"""
    #     bf = '(not (or a b))'
    #     rules = []
    #     tbl_names = []
    #     db = DatabaseEngine("cr")
    #
    #     ind_sem = StepSemantics(db, rules, tbl_names)
    #     ind_sem.prov_notations = {'a': 'a', 'b': 'b'}
    #     sol = ind_sem.solve_boolean_formula_with_z3_smt2(bf)
    #     print(sol)
    #     self.assertTrue(all(assign in str(sol) for assign in ["a = False", "b = False"]))

    def test_gen_prov_graph(self):
        """test func that generate the provenance graph from the assignments"""
        rules = [("author", "SELECT author.* FROM author, writes WHERE author.name LIKE '%m%' AND author.aid = writes.aid;")]
        tbl_names = ["organization", "author", "publication", "writes"]
        db = DatabaseEngine("cr")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        step_sem = StepSemantics(db, rules, tbl_names)
        prov_rules, prov_tbls, proj = step_sem.gen_prov_rules()
        cur_prov = db.execute_query(prov_rules[0][1])
        assignments = step_sem.rows_to_prov(cur_prov, prov_tbls[0], self.schema, proj, prov_rules[0])
        step_sem.gen_prov_graph(assignments)
        results = db.execute_query("SELECT author.* FROM author, writes WHERE author.name LIKE '%m%' AND author.aid = writes.aid;")
        results += db.execute_query("SELECT writes.* FROM author, writes WHERE author.name LIKE '%m%' AND author.aid = writes.aid;")
        results = set(results)
        self.assertTrue(len([v for v in step_sem.prov_graph.nodes() if "delta_" not in v[0]]) == len(results))

    def test_compute_benefits(self):
        """test func that computes the benefit of every existential node in the provenance graph"""
        rules = [("author", "SELECT author.* FROM author, writes WHERE author.name LIKE '%m%' AND author.aid = writes.aid;")]
        tbl_names = ["organization", "author", "publication", "writes"]
        db = DatabaseEngine("cr")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        step_sem = StepSemantics(db, rules, tbl_names)
        prov_rules, prov_tbls, proj = step_sem.gen_prov_rules()
        cur_prov = db.execute_query(prov_rules[0][1])
        assignments = step_sem.rows_to_prov(cur_prov, prov_tbls[0], self.schema, proj, prov_rules[0])
        step_sem.gen_prov_graph(assignments)
        step_sem.compute_benefits()
        self.assertTrue(all([step_sem.prov_graph.node[v]['benefit'] >= -100000 for v in step_sem.prov_graph.nodes()]))

    def test_traverse_by_layer(self):
        """test func that traverses the graph by its layer"""
        rules = [("author", "SELECT author.* FROM author, writes WHERE author.name LIKE '%m%' AND author.aid = writes.aid;")]
        tbl_names = ["organization", "author", "publication", "writes"]
        db = DatabaseEngine("cr")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        step_sem = StepSemantics(db, rules, tbl_names)
        prov_rules, prov_tbls, proj = step_sem.gen_prov_rules()
        cur_prov = db.execute_query(prov_rules[0][1])
        assignments = step_sem.rows_to_prov(cur_prov, prov_tbls[0], self.schema, proj, prov_rules[0])
        step_sem.gen_prov_graph(assignments)
        step_sem.compute_benefits()
        mss = step_sem.traverse_by_layer()
        self.assertTrue(len(mss) == 255 and all(t[0] == "author" for t in mss))

    def test_mss_easy_case(self):
        """test case with two simple rules"""
        rules = [("author", "SELECT author.* FROM author WHERE author.name like '%m%';"), ("writes", "SELECT writes.* FROM writes WHERE writes.aid = 58525;")]
        tbl_names = ["organization", "author", "publication", "writes"]
        db = DatabaseEngine("cr")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        step_sem = StepSemantics(db, rules, tbl_names)

        results = db.execute_query("SELECT author.* FROM author WHERE author.name like '%m%';")
        results += db.execute_query("SELECT writes.* FROM writes WHERE writes.aid = 58525;")
        mss = step_sem.find_mss(self.schema)
        print("size of mss is ", len(mss), "and size of results is ", len(results))
        self.assertTrue(len(mss) == len(results))

    def test_mss_easy_case_2(self):
        """test case with one simple rule"""
        rules = [("author", "SELECT author.* FROM author WHERE lower(author.name) like 'zohar dvir';"), ("writes", "SELECT writes.* FROM writes WHERE writes.aid = 58525;")]
        tbl_names = ["organization", "author", "publication", "writes"]
        db = DatabaseEngine("cr")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        step_sem = StepSemantics(db, rules, tbl_names)

        results = db.execute_query("SELECT author.* FROM author WHERE lower(author.name) like 'zohar dvir';")
        results += db.execute_query("SELECT writes.* FROM writes WHERE writes.aid = 58525;")
        mss = step_sem.find_mss(self.schema)
        # print("size of mss is ", len(mss), "and size of results is ", len(results))
        self.assertTrue(len(mss) == len(results))

    def test_mss_hard_case(self):
        """test case with two rules with the same body"""
        rules = [("author", "SELECT author.* FROM author, writes WHERE author.aid = writes.aid AND author.aid = 100920;"), ("writes", "SELECT writes.* FROM author, writes WHERE author.aid = writes.aid AND author.aid = 100920;")]
        tbl_names = ["organization", "author", "publication", "writes"]
        db = DatabaseEngine("cr")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        step_sem = StepSemantics(db, rules, tbl_names)
        mss = step_sem.find_mss(self.schema)
        # MSS should only include the author tuple with aid = 100920
        self.assertTrue(len(mss) == 1 and '100920' == next(iter(mss))[1][1:7])

    def test_mss_recursive_case(self):
        """test case with one simple rule"""
        rules = [("author", "SELECT author.* FROM author WHERE author.aid = 100920;"), ("writes", "SELECT writes.* FROM writes, delta_author WHERE writes.aid = delta_author.aid;")]
        tbl_names = ["organization", "author", "publication", "writes"]
        db = DatabaseEngine("cr")

        # reset the database
        # db.delete_tables(tbl_names)
        # db.load_database_tables(tbl_names)

        step_sem = StepSemantics(db, rules, tbl_names)
        mss = step_sem.find_mss(self.schema)
        print(mss)
        self.assertTrue(len(mss) == 3 and all('100920' in t[1] for t in mss))