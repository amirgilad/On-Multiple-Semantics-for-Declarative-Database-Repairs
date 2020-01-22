from Semantics.stage_sem import StageSemantics
from database_generator.dba import DatabaseEngine
import unittest


class TestStageSemantics(unittest.TestCase):

    def test_undefined_connection(self):
        """test no db connection"""
        rules = [("author", "SELECT * FROM author WHERE author.aid = 58525;")]
        tbl_names = ["organization", "author", "publication", "writes"]
        db = None
        self.assertRaises(AssertionError, StageSemantics, db, rules, tbl_names)

    def test_no_rules(self):
        """test no rules case. MSS supposed to be empty as db is stable"""
        rules = []
        tbl_names = ["organization", "author", "publication", "writes"]
        db = DatabaseEngine("cr")
        stage_sem = StageSemantics(db, rules, tbl_names)
        mss = stage_sem.find_mss()

        self.assertEqual(mss, set(), "MSS supposed to be empty! Instead its " + str(mss))

    def test_easy_case(self):
        """test case with one simple rule"""
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
        """test case with two simple rules"""
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
        """test case with two dependent rules"""
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

    def test_mss_hard_case_3(self):
        """test case with two rules with the same body"""
        rules = [("author", "SELECT author.* FROM author, organization WHERE author.oid = organization.oid AND organization.oid = 16045;"),
                 ("organization", "SELECT organization.* FROM author, organization WHERE author.oid = organization.oid AND organization.oid = 16045;")]
        tbl_names = ["organization", "author", "publication", "writes"]
        db = DatabaseEngine("cr")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        stage_sem = StageSemantics(db, rules, tbl_names)
        mss = stage_sem.find_mss()
        print(mss)
        # MSS should only include the organization tuple with aid = 100920
        self.assertTrue(len(mss) > 1 and 16045 in next(iter(mss))[1])

    def test_large_mss_2(self):
        rules = [("organization", "SELECT organization.* FROM organization WHERE organization.oid = 16045;"),
                 ("author", "SELECT author.* FROM author, delta_organization WHERE author.oid = delta_organization.oid;"),
                 ("writes", "SELECT writes.* FROM writes, delta_author WHERE delta_author.aid = writes.aid;"),
                 ("publication", "SELECT publication.* FROM publication, delta_writes WHERE publication.pid = delta_writes.pid;"),
                 ("cite", "SELECT cite.* FROM cite, delta_publication WHERE cite.citing = delta_publication.pid AND cite.citing < 10000;")]

        tbl_names = ["organization", "author", "publication", "writes", "cite"]
        db = DatabaseEngine("cr")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        results = db.execute_query("SELECT DISTINCT organization.* FROM organization WHERE organization.oid = 16045;")
        results += db.execute_query("SELECT DISTINCT author.* FROM author WHERE author.oid = 16045;")
        results += db.execute_query("SELECT DISTINCT writes.* FROM writes, author WHERE author.aid = writes.aid AND author.oid = 16045;")
        results += db.execute_query("SELECT DISTINCT publication.* FROM publication, writes, author WHERE publication.pid = writes.pid AND author.aid = writes.aid AND  author.oid = 16045;")
        results += db.execute_query("SELECT DISTINCT cite.* FROM cite, publication, writes, author WHERE cite.citing = publication.pid AND publication.pid = writes.pid AND author.aid = writes.aid AND author.oid = 16045 AND cite.citing < 10000;")

        res_size = len(results)
        print("results size:", res_size)

        stage_sem = StageSemantics(db, rules, tbl_names)
        mss = stage_sem.find_mss()

        print("size of MSS should be the entire DB. Actual size:", len(mss), "results size:", res_size)
        self.assertTrue(len(mss) == res_size)

    def test_recursive_case(self):
        """test case with two dependent rules"""
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

    def test_mutually_recursive_2(self):
        rules = [("publication", "SELECT publication.* FROM publication WHERE publication.pid = 2352376;"),
                 ("cite", "SELECT cite.* FROM cite, delta_publication WHERE delta_publication.pid = cite.cited;"),
                 ("cite", "SELECT cite.* FROM cite WHERE cite.cited = 2352376;"),
                 ("publication", "SELECT publication.* FROM publication, delta_cite WHERE publication.pid = delta_cite.cited;")
                 ]
        tbl_names = ["organization", "author", "publication", "writes", "cite"]
        db = DatabaseEngine("cr")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        stage_sem = StageSemantics(db, rules, tbl_names)
        mss = stage_sem.find_mss()
        print(mss)
        self.assertTrue(len(mss) == 5 and all(2352376 in t[1] for t in mss))

    def test_dc_like_author(self):
        rules = [("hauthor", "SELECT hauthor1.* FROM hauthor AS hauthor1, hauthor AS hauthor2 WHERE hauthor1.aid = hauthor2.aid AND hauthor1.oid <> hauthor2.oid;"),
                 ("hauthor", "SELECT hauthor2.* FROM hauthor AS hauthor1, hauthor AS hauthor2 WHERE hauthor1.aid = hauthor2.aid AND hauthor1.oid <> hauthor2.oid;"),
                 ("hauthor", "SELECT hauthor1.* FROM hauthor AS hauthor1, hauthor AS hauthor2 WHERE hauthor1.aid = hauthor2.aid AND lower(hauthor1.name) <> lower(hauthor2.name);"),
                 ("hauthor", "SELECT hauthor2.* FROM hauthor AS hauthor1, hauthor AS hauthor2 WHERE hauthor1.aid = hauthor2.aid AND lower(hauthor1.name) <> lower(hauthor2.name);"),
                 ("hauthor", "SELECT hauthor1.* FROM hauthor AS hauthor1, hauthor AS hauthor2 WHERE hauthor1.aid = hauthor2.aid AND lower(hauthor1.organization) <> lower(hauthor2.organization);"),
                 ("hauthor", "SELECT hauthor2.* FROM hauthor AS hauthor1, hauthor AS hauthor2 WHERE hauthor1.aid = hauthor2.aid AND lower(hauthor1.organization) <> lower(hauthor2.organization);"),
                 ("hauthor", "SELECT hauthor1.* FROM hauthor AS hauthor1, hauthor AS hauthor2 WHERE hauthor1.oid = hauthor2.oid AND lower(hauthor1.organization) <> lower(hauthor2.organization);"),
                 ("hauthor", "SELECT hauthor2.* FROM hauthor AS hauthor1, hauthor AS hauthor2 WHERE hauthor1.oid = hauthor2.oid AND lower(hauthor1.organization) <> lower(hauthor2.organization);")
                 ]
        tbl_names = ["hauthor_100_errors"]
        db = DatabaseEngine("cr")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        stage_sem = StageSemantics(db, rules, ["hauthor"])
        mss = stage_sem.find_mss()

        print("size of MSS:", len(mss))
        self.assertTrue(True)

    def test_tpch(self):
        rules = [("nation", "SELECT nation.* FROM nation WHERE nation.N_REGIONKEY = 1;"),
                     ("customer", "SELECT customer.* FROM customer, nation WHERE nation.N_NATIONKEY = customer.C_NATIONKEY;"),
                     ("supplier", "SELECT supplier.* FROM supplier, nation WHERE nation.N_NATIONKEY = supplier.S_NATIONKEY;")]

        tbl_names = ["region", "nation", "supplier", "customer", "partsupp", "orders"]
        db = DatabaseEngine("tpch")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        stage_sem = StageSemantics(db, rules, tbl_names)
        mss = stage_sem.find_mss()
        print(mss)
        print(len(mss))