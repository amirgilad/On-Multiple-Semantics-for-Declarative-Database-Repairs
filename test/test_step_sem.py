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
                          'importance'),
              "cite" : ('citing', 'cited')
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
        step_sem.compute_benefits_and_removed_flags()
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
        step_sem.compute_benefits_and_removed_flags()
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
        """test case with two different simple rules"""
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
        self.assertTrue(len(mss) == 1 and 100920 in next(iter(mss))[1])

    def test_mss_hard_case(self):
        """test case with two rules with the same body"""
        rules = [("author", "SELECT author.* FROM author, organization WHERE author.oid = organization.oid AND organization.oid = 16045;"),
                 ("organization", "SELECT organization.* FROM author, organization WHERE author.oid = organization.oid AND organization.oid = 16045;")]
        tbl_names = ["organization", "author", "publication", "writes"]
        db = DatabaseEngine("cr")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        step_sem = StepSemantics(db, rules, tbl_names)
        mss = step_sem.find_mss(self.schema)
        print(mss)
        # MSS should only include the organization tuple with aid = 100920
        self.assertTrue(len(mss) == 1 and 16045 in next(iter(mss))[1])

    def test_mss_recursive_case(self):
        """test case with one rule relying on the other"""
        rules = [("author", "SELECT author.* FROM author WHERE author.aid = 100920;"), ("writes", "SELECT writes.* FROM writes, delta_author WHERE writes.aid = delta_author.aid;")]
        tbl_names = ["organization", "author", "publication", "writes"]
        db = DatabaseEngine("cr")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        step_sem = StepSemantics(db, rules, tbl_names)
        mss = step_sem.find_mss(self.schema)
        print(mss)
        self.assertTrue(len(mss) == 3 and all(100920 in t[1] for t in mss))

    def test_mss_three_layers(self):
        rules = [("author", "SELECT author.* FROM author WHERE author.aid = 100920;"),
        ("writes", "SELECT writes.* FROM writes, delta_author WHERE writes.aid = delta_author.aid;"),
        ("publication", "SELECT publication.* FROM publication, delta_writes, author WHERE publication.pid = delta_writes.pid AND delta_writes.aid = author.aid;")]

        tbl_names = ["organization", "author", "publication", "writes"]
        db = DatabaseEngine("cr")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        step_sem = StepSemantics(db, rules, tbl_names)
        mss = step_sem.find_mss(self.schema)
        print(mss)
        self.assertTrue(len(mss) == 3 and all(100920 in t[1] for t in mss))

    def test_separation_from_stage(self):
        rules = [("author", "SELECT author.* FROM author, writes WHERE author.aid = writes.aid AND author.aid = 100920;"),
        ("writes", "SELECT writes.* FROM author, writes WHERE author.aid = writes.aid AND author.aid = 100920;"),
        ("publication", "SELECT publication.* FROM publication, delta_writes, author WHERE publication.pid = delta_writes.pid AND delta_writes.aid = author.aid;"),
        ("publication", "SELECT publication.* FROM publication, writes, delta_author WHERE publication.pid = writes.pid AND writes.aid = delta_author.aid;")]

        tbl_names = ["organization", "author", "publication", "writes"]
        db = DatabaseEngine("cr")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        step_sem = StepSemantics(db, rules, tbl_names)
        mss = step_sem.find_mss(self.schema)
        print(mss)
        self.assertTrue(len(mss) == 3)

    def test_large_mss(self):
        rules = [("organization", "SELECT organization.* FROM organization WHERE organization.oid = 16045;"),
                    ("author", "SELECT author.* FROM author, delta_organization WHERE author.oid = delta_organization.oid;"),
                    ("writes", "SELECT writes.* FROM writes, delta_author WHERE delta_author.aid = writes.aid;"),
                    ("publication", "SELECT publication.* FROM publication, delta_writes WHERE publication.pid = delta_writes.pid;")]

        tbl_names = ["organization", "author", "publication", "writes"]
        db = DatabaseEngine("cr")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        step_sem = StepSemantics(db, rules, tbl_names)
        mss = step_sem.find_mss(self.schema)
        print("size of MSS should be the entire DB. Actual size:", len(mss))
        self.assertTrue(len(mss) > 27000)

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

        results1 = db.execute_query("SELECT organization.* FROM organization WHERE organization.oid = 16045;")
        print("num org:", len(results1))
        results2 = db.execute_query("SELECT author.* FROM author WHERE author.oid = 16045;")
        print("num author:", len(results2))
        results3 = db.execute_query("SELECT writes.* FROM writes, author WHERE author.aid = writes.aid AND author.oid = 16045;")
        print("num writes:", len(results3))
        results4 = db.execute_query("SELECT publication.* FROM publication, writes, author WHERE publication.pid = writes.pid AND author.aid = writes.aid AND  author.oid = 16045;")
        print("num pub:", len(results4))
        results5 = db.execute_query("SELECT cite.* FROM cite, publication, writes, author WHERE cite.citing = publication.pid AND publication.pid = writes.pid AND author.aid = writes.aid AND author.oid = 16045 AND cite.citing < 10000;")
        print("num cite:", len(results5))

        res_size = len(results1)+len(results2)+len(results3)+len(results4)+len(results5)
        print("results size:", res_size)

        step_sem = StepSemantics(db, rules, tbl_names)
        mss = step_sem.find_mss(self.schema)

        print("size of MSS should be the entire DB. Actual size:", len(mss), "results size:", res_size)
        self.assertTrue(len(mss) == res_size)

    def test_mutually_recursive_2(self):
        # DOES NOT WORK AS PROVENANCE GRAPH HAS A CYCLE!!!
        rules = [("publication", "SELECT publication.* FROM publication WHERE publication.pid = 2352376;"),
                 ("cite", "SELECT cite.* FROM cite, delta_publication WHERE delta_publication.pid = cite.cited AND cite.cited = 2352376;"),
                 ("cite", "SELECT cite.* FROM cite WHERE cite.cited = 2352376;"),
                 ("publication", "SELECT publication.* FROM publication, delta_cite WHERE publication.pid = delta_cite.cited AND delta_cite.cited = 2352376;")
                 ]
        tbl_names = ["organization", "author", "publication", "writes", "cite"]
        db = DatabaseEngine("cr")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        step_sem = StepSemantics(db, rules, tbl_names)
        mss = step_sem.find_mss(self.schema)
        print(mss)
        self.assertTrue(len(mss) == 5 and all(2352376 in t[1] for t in mss))

