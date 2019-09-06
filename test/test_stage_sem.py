from Semantics.stage_sem import StageSemantics
from database_generator.dba import DatabaseEngine
import unittest


class TestStageSemantics(unittest.TestCase):

    def test_undefined_connection(self):
        # test no db connection
        rules = [("author", "SELECT * FROM author WHERE author.aid = 58525;")]
        tbl_names = ["organization", "author", "publication", "writes"]
        db = None
        self.assertRaises(AssertionError, StageSemantics, db, rules, tbl_names)

    def test_no_rules(self):
        # test no rules case. MSS supposed to be empty as db is stable
        rules = []
        tbl_names = ["organization", "author", "publication", "writes"]
        db = DatabaseEngine("cr")
        stage_sem = StageSemantics(db, rules, tbl_names)
        mss = stage_sem.find_mss()

        self.assertEqual(mss, set(), "MSS supposed to be empty! Instead its " + str(mss))

    def test_easy_case(self):
        # test case with one simple rule
        rules = [("author", "SELECT * FROM author WHERE author.aid = 58525;")]
        tbl_names = ["organization", "author", "publication", "writes"]
        db = DatabaseEngine("cr")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        stage_sem = StageSemantics(db, rules, tbl_names)

        results = db.execute_query("SELECT * FROM author WHERE author.aid = 58525;")
        mss = stage_sem.find_mss()
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

        stage_sem = StageSemantics(db, rules, tbl_names)

        results = db.execute_query("SELECT * FROM author WHERE author.name like '%m%';")
        results += db.execute_query("SELECT * FROM writes WHERE pid = 1270038;")
        mss = stage_sem.find_mss()
        mss_no_rel = [e[1] for e in mss]
        self.assertTrue(all(t in mss_no_rel for t in results))

    def test_hard_case_2(self):
        # test case with two dependent rules
        rules = [("author", "SELECT author.* FROM author WHERE author.aid = 100920;"), ("writes", "SELECT writes.* FROM writes, delta_author WHERE writes.aid = delta_author.aid;")]
        tbl_names = ["organization", "author", "publication", "writes"]
        db = DatabaseEngine("cr")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        stage_sem = StageSemantics(db, rules, tbl_names)

        results = db.execute_query("SELECT author.* FROM author WHERE author.aid = 100920;")
        results += db.execute_query("SELECT writes.* FROM writes, delta_author WHERE writes.aid = 100920;")
        mss = stage_sem.find_mss()
        print(mss)
        mss_no_rel = [e[1] for e in mss]
        self.assertTrue(all(t in mss_no_rel for t in results))

    def test_recursive_case(self):
        # test case with two dependent rules
        rules = [("author", "SELECT * FROM author WHERE author.name like '%m%';"), ("writes", "SELECT writes.* FROM writes, delta_author WHERE writes.aid = delta_author.aid;")]
        tbl_names = ["organization", "author", "publication", "writes"]
        db = DatabaseEngine("cr")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        stage_sem = StageSemantics(db, rules, tbl_names)

        results = db.execute_query("SELECT * FROM author WHERE author.name like '%m%';")
        results += db.execute_query("SELECT writes.* FROM writes, delta_author WHERE writes.aid = delta_author.aid;")
        mss = stage_sem.find_mss()
        mss_no_rel = [e[1] for e in mss]
        self.assertTrue(all(t in mss_no_rel for t in results))

    def test_recursive_case_3_rules(self):
        rules = [("author", "SELECT author.* FROM author WHERE author.aid = 100920;"),
                 ("writes", "SELECT writes.* FROM writes, delta_author WHERE writes.aid = delta_author.aid;"),
                 ("publication", "SELECT publication.* FROM publication, delta_writes, author WHERE publication.pid = delta_writes.pid AND delta_writes.aid = author.aid;")]
        tbl_names = ["organization", "author", "publication", "writes"]
        db = DatabaseEngine("cr")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        stage_sem = StageSemantics(db, rules, tbl_names)
        mss = stage_sem.find_mss()
        print(mss)
        self.assertTrue(len(mss) == 3 and len([t for t in mss if t[0] == 'writes']) == 2 and len([t for t in mss if t[0] == 'author']) == 1)