from Semantics.end_sem import EndSemantics
from database_generator.dba import DatabaseEngine
import unittest


class TestEndSemantics(unittest.TestCase):

    def test_undefined_connection(self):
        # test no db connection
        rules = [("author", "SELECT * FROM author WHERE author.aid = 58525;")]
        tbl_names = ["organization", "author", "publication", "writes"]
        db = None
        self.assertRaises(AssertionError, EndSemantics, db, rules, tbl_names)

    def test_no_rules(self):
        # test no rules case. MSS supposed to be empty as db is stable
        rules = []
        tbl_names = ["organization", "author", "publication", "writes"]
        db = DatabaseEngine("cr")
        end_sem = EndSemantics(db, rules, tbl_names)
        mss = end_sem.find_mss()

        self.assertEqual(mss, set(), "MSS supposed to be empty! Instead its " + str(mss))

    # def test_case\d_1_rules_sql_syntax(self):
    #     # no ";" at the end
    #     rules1 = ["SELECT * FROM author WHERE author.aid = 5"]
    #     db = DatabaseEngine("cr")
    #     tbl_names = ["organization", "author", "publication", "writes"]
    #     end_sem = EndSemantics(db, rules1, tbl_names)
    #     self.assertRaises(AssertionError, end_sem.find_mss())
    #
    # def test_case_2_rules_sql_syntax(self):
    #     # no table name after FROM
    #     rules2 = ["SELECT * FROM WHERE author.aid = 5;"]
    #     db = DatabaseEngine("cr")
    #     tbl_names = ["organization", "author", "publication", "writes"]
    #     end_sem = EndSemantics(db, rules2, tbl_names)
    #     self.assertRaises(AssertionError, end_sem.find_mss())
    #
    # def test_case_3_rules_sql_syntax(self):
    #     # no condition after WHERE
    #     rules3 = ["SELECT * FROM author WHERE ;"]
    #     db = DatabaseEngine("cr")
    #     tbl_names = ["organization", "author", "publication", "writes"]
    #     end_sem = EndSemantics(db, rules3, tbl_names)
    #     self.assertRaises(AssertionError, end_sem.find_mss())

    def test_easy_case(self):
        # test case with one simple rule
        rules = [("author", "SELECT * FROM author WHERE author.aid = 58525;")]
        tbl_names = ["organization", "author", "publication", "writes"]
        db = DatabaseEngine("cr")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        end_sem = EndSemantics(db, rules, tbl_names)

        results = db.execute_query("SELECT * FROM author WHERE author.aid = 58525;")
        mss = end_sem.find_mss()
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

        end_sem = EndSemantics(db, rules, tbl_names)

        results = db.execute_query("SELECT * FROM author WHERE author.name like '%m%';")
        results += db.execute_query("SELECT * FROM writes WHERE pid = 1270038;")
        mss = end_sem.find_mss()
        mss_no_rel = [e[1] for e in mss]
        self.assertTrue(all(t in mss_no_rel for t in results))