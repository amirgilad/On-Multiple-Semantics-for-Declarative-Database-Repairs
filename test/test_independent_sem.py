from Semantics.independent_sem import IndependentSemantics
from database_generator.dba import DatabaseEngine
import unittest


class TestIndependentSemantics(unittest.TestCase):

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
        # test no db connection
        rules = [("author", "SELECT * FROM author WHERE author.aid = 58525;")]
        tbl_names = ["organization", "author", "publication", "writes"]
        db = None
        self.assertRaises(AssertionError, IndependentSemantics, db, rules, tbl_names)

    def test_no_rules(self):
        # test no rules case. MSS supposed to be empty as db is stable
        rules = []
        tbl_names = ["organization", "author", "publication", "writes"]
        db = DatabaseEngine("cr")
        ind_sem = IndependentSemantics(db, rules, tbl_names)
        mss = ind_sem.find_mss(self.schema)

        self.assertEqual(mss, set(), "MSS supposed to be empty! Instead its " + str(mss))

    def test_gen_prov_rules(self):
        # test func that takes delta rules and turns them to rules that return provenance
        rules = [("author", "SELECT author.* FROM author, writes WHERE author.name LIKE '%m%' AND author.aid = writes.aid;")]
        tbl_names = ["organization", "author", "publication", "writes"]
        db = DatabaseEngine("cr")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        ind_sem = IndependentSemantics(db, rules, tbl_names)
        prov_rule = ind_sem.gen_prov_rules()[0][0][1]
        func_prov_results = db.execute_query(prov_rule)
        desired_prov_results = db.execute_query("SELECT author.*, writes.* FROM author, writes WHERE author.name LIKE '%m%' AND author.aid = writes.aid;")
        self.assertEqual(func_prov_results, desired_prov_results)

    def test_rows_to_prov(self):
        # test func that handles and stores the provenance of each tuple
        rules = [("author", "SELECT author.* FROM author, writes WHERE author.name LIKE '%m%' AND author.aid = writes.aid;")]
        tbl_names = ["organization", "author", "publication", "writes"]
        db = DatabaseEngine("cr")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        ind_sem = IndependentSemantics(db, rules, tbl_names)
        prov_rules, prov_tbls, proj = ind_sem.gen_prov_rules()
        cur_prov = db.execute_query(prov_rules[0][1])
        assignments = ind_sem.rows_to_prov(cur_prov, prov_tbls[0], self.schema, proj, prov_rules[0])
        self.assertTrue(all(a[0][0] == 'delta_author' for a in assignments))

    def test_solve_boolean_formula_with_z3_smt2(self):
        # test func that finds the minimum satisfying assignment to a boolean formula
        bf = '(and (or a b) (not (and a c)))'

        rules = []
        tbl_names = []
        db = DatabaseEngine("cr")

        ind_sem = IndependentSemantics(db, rules, tbl_names)
        ind_sem.prov_notations = {'a': 'a', 'b': 'b', 'c': 'c'}
        sol = ind_sem.solve_boolean_formula_with_z3_smt2(bf)
        self.assertTrue(all(assign in str(sol) for assign in ["a = False", "b = True", "c = False"]))

    def test_solve_boolean_formula_with_z3_smt2_not(self):
        # test func that finds the minimum satisfying assignment to a boolean formula
        bf = '(not (or a b))'
        rules = []
        tbl_names = []
        db = DatabaseEngine("cr")

        ind_sem = IndependentSemantics(db, rules, tbl_names)
        ind_sem.prov_notations = {'a': 'a', 'b': 'b'}
        sol = ind_sem.solve_boolean_formula_with_z3_smt2(bf)
        print(sol)
        self.assertTrue(all(assign in str(sol) for assign in ["a = False", "b = False"]))

    def test_process_provenance(self):
        # test func that converts the assignments into the provenance of each tuple
        rules = [("author", "SELECT author.* FROM author, writes WHERE author.name LIKE '%m%' AND author.aid = writes.aid;")]
        tbl_names = ["organization", "author", "publication", "writes"]
        db = DatabaseEngine("cr")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        ind_sem = IndependentSemantics(db, rules, tbl_names)
        prov_rules, prov_tbls, proj = ind_sem.gen_prov_rules()
        cur_prov = db.execute_query(prov_rules[0][1])
        assignments = ind_sem.rows_to_prov(cur_prov, prov_tbls[0], self.schema, proj, prov_rules[0])
        ind_sem.process_provenance(assignments)
        self.assertTrue(all("delta_" in k[0] for k in ind_sem.provenance))

    def test_convert_to_bool_formula(self):
        # test func that takes the provenance and converts it into a bool formula
        rules = [("author", "SELECT author.* FROM author, writes WHERE author.name LIKE '%m%' AND author.aid = writes.aid;")]
        tbl_names = ["organization", "author", "publication", "writes"]
        db = DatabaseEngine("cr")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        ind_sem = IndependentSemantics(db, rules, tbl_names)
        prov_rules, prov_tbls, proj = ind_sem.gen_prov_rules()
        cur_prov = db.execute_query(prov_rules[0][1])
        assignments = ind_sem.rows_to_prov(cur_prov, prov_tbls[0], self.schema, proj, prov_rules[0])
        ind_sem.process_provenance(assignments)
        bf = ind_sem.convert_to_bool_formula()
        assert (len(ind_sem.prov_notations.keys()) == len(set(ind_sem.prov_notations.values())))
        sol = ind_sem.solve_boolean_formula_with_z3_smt2(bf)
        print(bf)
        print(sol)

    def test_mss_easy_case(self):
        # test case with one simple rule
        rules = [("author", "SELECT author.* FROM author  WHERE author.name like '%m%';"), ("writes", "SELECT writes.* FROM writes WHERE writes.aid = 58525;")]
        tbl_names = ["organization", "author", "publication", "writes"]
        db = DatabaseEngine("cr")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        ind_sem = IndependentSemantics(db, rules, tbl_names)

        results = db.execute_query("SELECT author.* FROM author  WHERE author.name like '%m%';")
        results += db.execute_query("SELECT writes.* FROM writes WHERE writes.aid = 58525;")
        mss = ind_sem.find_mss(self.schema)
        # print("size of mss is ", len(mss), "and size of results is ", len(results))
        self.assertTrue(len(mss) == len(results))

    def test_mss_hard_case(self):
        # test case with two conflicting rules
        rules = [("author", "SELECT author.* FROM author, writes WHERE author.aid = writes.aid AND author.name LIKE '%m%';"), ("writes", "SELECT writes.* FROM author, writes WHERE author.aid = writes.aid AND author.name LIKE '%m%';")]
        tbl_names = ["organization", "author", "publication", "writes"]
        db = DatabaseEngine("cr")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        ind_sem = IndependentSemantics(db, rules, tbl_names)

        results1 = db.execute_query("SELECT author.* FROM author, writes WHERE author.aid = writes.aid AND author.name LIKE '%m%';")
        results2 = db.execute_query("SELECT writes.* FROM author, writes WHERE author.aid = writes.aid AND author.name LIKE '%m%';")
        mss = ind_sem.find_mss(self.schema)
        # print("size of mss is", len(mss), "and size of results1 is", len(results1), "and size of results2 is", len(results2))
        # 2-approximation
        self.assertTrue(len(mss) <= 2*min(len(results1), len(results2)))

    def test_mss_recursive_case(self):
        # test case with one simple rule
        rules = [("author", "SELECT author.* FROM author WHERE author.name like '%m%';"), ("writes", "SELECT writes.* FROM writes, delta_author WHERE writes.aid = delta_author.aid;")]
        tbl_names = ["organization", "author", "publication", "writes"]
        db = DatabaseEngine("cr")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        ind_sem = IndependentSemantics(db, rules, tbl_names)

        results = db.execute_query("SELECT author.* FROM author WHERE author.name like '%m%';")
        results += db.execute_query("SELECT writes.* FROM writes, delta_author WHERE writes.aid = delta_author.aid;")
        mss = ind_sem.find_mss()
        mss_no_rel = [e[1] for e in mss]
        self.assertTrue(all(t in mss_no_rel for t in results))