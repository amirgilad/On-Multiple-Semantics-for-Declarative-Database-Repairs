from Semantics.independent_sem import IndependentSemantics
from database_generator.dba import DatabaseEngine
import unittest


class TestIndependentSemantics(unittest.TestCase):

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
        mss = ind_sem.find_mss()

        self.assertEqual(mss, set(), "MSS supposed to be empty! Instead its " + str(mss))

    def test_gen_prov_rules(self):
        rules = [("author", "SELECT author.name FROM author, writes WHERE author.name LIKE '%m%' AND author.aid = writes.aid;")]
        tbl_names = ["organization", "author", "publication", "writes"]
        db = DatabaseEngine("cr")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        ind_sem = IndependentSemantics(db, rules, tbl_names)
        prov_rule = ind_sem.gen_prov_rules()[0][1]
        func_prov_results = db.execute_query(prov_rule)
        desired_prov_results = db.execute_query("SELECT author.*, writes.* FROM author, writes WHERE author.name LIKE '%m%' AND author.aid = writes.aid;")
        self.assertEqual(func_prov_results, desired_prov_results)

    def test_easy_case(self):
        # test case with one simple rule
        rules = [("author", "SELECT * FROM author WHERE author.aid = 58525;")]
        tbl_names = ["organization", "author", "publication", "writes"]
        db = DatabaseEngine("cr")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        ind_sem = IndependentSemantics(db, rules, tbl_names)

        results = db.execute_query("SELECT * FROM author WHERE author.aid = 58525;")
        mss = ind_sem.find_mss()
        mss_no_rel = [e[1] for e in mss]
        self.assertTrue(all(t in mss_no_rel for t in results))

    def test_hard_case(self):
        # test case with one simple rule
        rules = [("author", "SELECT * FROM author WHERE author.name like '%m%';"), ("writes", "SELECT * FROM writes WHERE pid = 1270038;")]
        tbl_names = ["organization", "author", "publication", "writes"]
        db = DatabaseEngine("cr")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        ind_sem = IndependentSemantics(db, rules, tbl_names)

        results = db.execute_query("SELECT * FROM author WHERE author.name like '%m%';")
        results += db.execute_query("SELECT * FROM writes WHERE pid = 1270038;")
        mss = ind_sem.find_mss()
        mss_no_rel = [e[1] for e in mss]
        self.assertTrue(all(t in mss_no_rel for t in results))

    def test_recursive_case(self):
        # test case with one simple rule
        rules = [("author", "SELECT * FROM author WHERE author.name like '%m%';"), ("writes", "SELECT writes.* FROM writes, delta_author WHERE writes.aid = delta_author.aid;")]
        tbl_names = ["organization", "author", "publication", "writes"]
        db = DatabaseEngine("cr")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        ind_sem = IndependentSemantics(db, rules, tbl_names)

        results = db.execute_query("SELECT * FROM author WHERE author.name like '%m%';")
        results += db.execute_query("SELECT writes.* FROM writes, delta_author WHERE writes.aid = delta_author.aid;")
        mss = ind_sem.find_mss()
        mss_no_rel = [e[1] for e in mss]
        self.assertTrue(all(t in mss_no_rel for t in results))