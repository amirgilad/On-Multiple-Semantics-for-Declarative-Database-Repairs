from Semantics.step_sem import StepSemantics
from Semantics.independent_sem import IndependentSemantics
from Semantics.stage_sem import StageSemantics
from Semantics.end_sem import EndSemantics
from database_generator.dba import DatabaseEngine
import time
import unittest


class TestStepSemantics(unittest.TestCase):

    mas_schema = {"author": ('aid',
                         'name',
                         'oid'),
                  "hauthor": ('aid', 'name', 'oid', 'organization'),
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
    tpch_schema = {"customer": ('c_custkey',
                                'c_name',
                                'c_address',
                                'c_nationkey',
                                'c_phone',
                                'c_acctbal',
                                'C_MKTSEGMENT',
                                'c_comment'),
                   "lineitem": ('L_ORDERKEY',
                                'L_PARTKEY',
                                'L_SUPPKEY',
                                'L_LINENUMBER',
                                'L_QUANTITY',
                                'L_EXTENDEDPRICE',
                                'L_DISCOUNT',
                                'L_TAX',
                                'L_RETURNFLAG',
                                'L_LINESTATUS',
                                'L_SHIPDATE',
                                'L_COMMITDATE',
                                'L_RECEIPTDATE',
                                'L_SHIPINSTRUCT',
                                'L_SHIPMODE',
                                'L_COMMENT'),
                   "nation": ('N_NATIONKEY',
                              'N_NAME',
                              'N_REGIONKEY',
                              'N_COMMENT'),
                   "orders": ('O_ORDERKEY',
                              'O_CUSTKEY',
                              'O_ORDERSTATUS',
                              'O_TOTALPRICE',
                              'O_ORDERDATE',
                              'O_ORDERPRIORITY',
                              'O_CLERK',
                              'O_SHIPPRIORITY',
                              'O_COMMENT'),
                   "part": ('P_PARTKEY',
                            'P_NAME',
                            'P_MFGR',
                            'P_BRAND',
                            'P_TYPE',
                            'P_SIZE',
                            'P_CONTAINER',
                            'P_RETAILPRICE',
                            'P_COMMENT'),
                   "partsupp": ('PS_PARTKEY',
                                'PS_SUPPKEY',
                                'PS_AVAILQTY',
                                'PS_SUPPLYCOST',
                                'PS_COMMENT'),
                   "region": ('R_REGIONKEY',
                              'R_NAME',
                              'R_COMMENT'),
                   "supplier": ('S_SUPPKEY',
                                'S_NAME',
                                'S_ADDRESS',
                                'S_NATIONKEY',
                                'S_PHONE',
                                'S_ACCTBAL',
                                'S_COMMENT')
                   }
    holocomp_schema = {
        "hospital": ("ProviderNumber", "HospitalName", "Address1", "City", "State", "ZipCode", "CountyName", "PhoneNumber", "HospitalType", "HospitalOwner", "EmergencyService", "Condition", "MeasureCode", "MeasureName", "Score", "Sample", "Stateavg"),
        "hauthor": ('aid', 'name', 'oid', 'organization'),
        "flights": ('src', 'flight', 'scheduled_dept', 'actual_dept	dept_gate', 'scheduled_arrival', 'actual_arrival', 'arrival_gate')
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
        mss = step_sem.find_mss(self.mas_schema)

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
        assignments = step_sem.rows_to_prov(cur_prov, prov_tbls[0], self.mas_schema, proj, prov_rules[0])
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
        assignments = step_sem.rows_to_prov(cur_prov, prov_tbls[0], self.mas_schema, proj, prov_rules[0])
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
        assignments = step_sem.rows_to_prov(cur_prov, prov_tbls[0], self.mas_schema, proj, prov_rules[0])
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
        assignments = step_sem.rows_to_prov(cur_prov, prov_tbls[0], self.mas_schema, proj, prov_rules[0])
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
        mss = step_sem.find_mss(self.mas_schema)
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
        mss = step_sem.find_mss(self.mas_schema)
        # print("size of mss is ", len(mss), "and size of results is ", len(results))
        self.assertTrue(len(mss) == len(results))

    def test_mss_hard_case(self):
        """test case with two rules with the same body"""
        rules = [("author", "SELECT author.* FROM author, writes WHERE author.aid = writes.aid AND author.aid = 100920;"),
                 ("writes", "SELECT writes.* FROM author, writes WHERE author.aid = writes.aid AND author.aid = 100920;")]
        tbl_names = ["organization", "author", "publication", "writes"]
        db = DatabaseEngine("cr")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        step_sem = StepSemantics(db, rules, tbl_names)
        mss = step_sem.find_mss(self.mas_schema)
        # MSS should only include the author tuple with aid = 100920
        self.assertTrue(len(mss) == 1 and 100920 in next(iter(mss))[1])

    def test_mss_hard_case_2(self):
        """test case with two rules with the same body"""
        rules = [("author", "SELECT author.* FROM author, organization WHERE author.oid = organization.oid AND organization.oid = 16045;"),
                 ("organization", "SELECT organization.* FROM author, organization WHERE author.oid = organization.oid AND organization.oid = 16045;")]
        tbl_names = ["organization", "author", "publication", "writes"]
        db = DatabaseEngine("cr")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        step_sem = StepSemantics(db, rules, tbl_names)
        mss = step_sem.find_mss(self.mas_schema)
        print(mss)
        # MSS should only include the organization tuple with aid = 100920
        self.assertTrue(len(mss) == 1 and 16045 in next(iter(mss))[1])

    def test_mss_hard_case_3(self):
        rules = [
            ("author", "SELECT author.* FROM author, writes WHERE author.aid = writes.aid AND author.aid = 100920;"),
            ("writes", "SELECT writes.* FROM author, writes WHERE author.aid = writes.aid AND author.aid = 100920;"),
            ("publication", "SELECT publication.* FROM publication, delta_writes, author WHERE publication.pid = delta_writes.pid AND delta_writes.aid = author.aid;"),
            ("publication", "SELECT publication.* FROM publication, writes, delta_author WHERE publication.pid = writes.pid AND writes.aid = delta_author.aid;")
            ]
        tbl_names = ["organization", "author", "publication", "writes", "cite"]
        db = DatabaseEngine("cr")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        ind_sem = IndependentSemantics(db, rules, tbl_names)
        mss_ind = ind_sem.find_mss(self.mas_schema)

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        step_sem = StepSemantics(db, rules, tbl_names)
        mss_step = step_sem.find_mss(self.mas_schema)
        mss_step_strs = set([(t[0], '('+','.join(str(x) for x in t[1])+')') for t in mss_step])


        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        stage_sem = StageSemantics(db, rules, tbl_names)
        mss_stage = stage_sem.find_mss()
        mss_stage_strs = set([(t[0], '('+','.join(str(x) for x in t[1])+')') for t in mss_stage])

        print(mss_ind)
        print(mss_step)
        print(mss_stage)

        # mss according to end should be equal to mss according to ind
        self.assertTrue(len(mss_stage_strs) == len(mss_ind))

    def test_mss_recursive_case(self):
        """test case with one rule relying on the other"""
        rules = [("author", "SELECT author.* FROM author WHERE author.aid = 100920;"), ("writes", "SELECT writes.* FROM writes, delta_author WHERE writes.aid = delta_author.aid;")]
        tbl_names = ["organization", "author", "publication", "writes"]
        db = DatabaseEngine("cr")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        step_sem = StepSemantics(db, rules, tbl_names)
        mss = step_sem.find_mss(self.mas_schema)
        print(mss)
        self.assertTrue(len(mss) == 3 and all(100920 in t[1] for t in mss))

    def test_mss_prog_6(self):
        rules = [("author","SELECT author.* FROM author WHERE author.aid = 100920;"),
                  ("writes","SELECT writes.* FROM writes, delta_author WHERE writes.aid = delta_author.aid;"),
        ("publication","SELECT publication.* FROM publication, delta_writes, author WHERE publication.pid = delta_writes.pid AND delta_writes.aid = author.aid;")]

        tbl_names = ["organization", "author", "publication", "writes"]
        db = DatabaseEngine("cr")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        step_sem = StepSemantics(db, rules, tbl_names)
        mss = step_sem.find_mss(self.mas_schema)
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
        mss = step_sem.find_mss(self.mas_schema)
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
        mss = step_sem.find_mss(self.mas_schema)
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
        mss = step_sem.find_mss(self.mas_schema)
        print("size of MSS should be the entire DB. Actual size:", len(mss))
        self.assertTrue(len(mss) == 24798)

    def test_large_mss_2(self):
        rules = [("organization", "SELECT organization.* FROM organization WHERE organization.oid = 16045;"),
                 ("author", "SELECT author.* FROM author, delta_organization WHERE author.oid = delta_organization.oid;"), # AND author.aid < 400000
                 ("writes", "SELECT writes.* FROM writes, delta_author WHERE delta_author.aid = writes.aid;"),
                 ("publication", "SELECT publication.* FROM publication, delta_writes WHERE publication.pid = delta_writes.pid;"),
                 ("cite", "SELECT cite.* FROM cite, delta_publication WHERE cite.citing = delta_publication.pid;")]

        tbl_names = ["organization", "author", "publication", "writes", "cite"]
        db = DatabaseEngine("cr")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        results = db.execute_query("SELECT DISTINCT organization.* FROM organization WHERE organization.oid = 16045;")
        results += db.execute_query("SELECT DISTINCT author.* FROM author WHERE author.oid = 16045;")
        results += db.execute_query("SELECT DISTINCT writes.* FROM writes, author WHERE author.aid = writes.aid AND author.oid = 16045;")
        results += db.execute_query("SELECT DISTINCT publication.* FROM publication, writes, author WHERE publication.pid = writes.pid AND author.aid = writes.aid AND  author.oid = 16045;")
        results += db.execute_query("SELECT DISTINCT cite.* FROM cite, publication, writes, author WHERE cite.citing = publication.pid AND publication.pid = writes.pid AND author.aid = writes.aid AND author.oid = 16045;")

        res_size = len(results)
        print("results size:", res_size)

        step_sem = StepSemantics(db, rules, tbl_names)
        start = time.time()
        mss = step_sem.find_mss(self.mas_schema)
        end = time.time()
        time_mss = end-start
        print("time to find MSS by step semantics:", time_mss)
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
        mss = step_sem.find_mss(self.mas_schema)
        print(mss)
        self.assertTrue(len(mss) == 5 and all(2352376 in t[1] for t in mss))

    def test_dc_like_author(self):
        rules = [("hauthor", "SELECT hauthor1.* FROM hauthor AS hauthor1, hauthor AS hauthor2 WHERE hauthor1.aid = hauthor2.aid AND hauthor1.oid <> hauthor2.oid;"),
                 # ("hauthor", "SELECT hauthor2.* FROM hauthor AS hauthor1, hauthor AS hauthor2 WHERE hauthor1.aid = hauthor2.aid AND hauthor1.oid <> hauthor2.oid;"),
                 ("hauthor", "SELECT hauthor1.* FROM hauthor AS hauthor1, hauthor AS hauthor2 WHERE hauthor1.aid = hauthor2.aid AND lower(hauthor1.name) <> lower(hauthor2.name);"),
                 # ("hauthor", "SELECT hauthor2.* FROM hauthor AS hauthor1, hauthor AS hauthor2 WHERE hauthor1.aid = hauthor2.aid AND lower(hauthor1.name) <> lower(hauthor2.name);"),
                 ("hauthor", "SELECT hauthor1.* FROM hauthor AS hauthor1, hauthor AS hauthor2 WHERE hauthor1.aid = hauthor2.aid AND lower(hauthor1.organization) <> lower(hauthor2.organization);"),
                 # ("hauthor", "SELECT hauthor2.* FROM hauthor AS hauthor1, hauthor AS hauthor2 WHERE hauthor1.aid = hauthor2.aid AND lower(hauthor1.organization) <> lower(hauthor2.organization);"),
                 ("hauthor", "SELECT hauthor1.* FROM hauthor AS hauthor1, hauthor AS hauthor2 WHERE hauthor1.oid = hauthor2.oid AND lower(hauthor1.organization) <> lower(hauthor2.organization);")
                 # ("hauthor", "SELECT hauthor2.* FROM hauthor AS hauthor1, hauthor AS hauthor2 WHERE hauthor1.oid = hauthor2.oid AND lower(hauthor1.organization) <> lower(hauthor2.organization);")
                 ]
        tbl_names = ["hauthor_500_errors"]
        db = DatabaseEngine("cr")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        step_sem = StepSemantics(db, rules, ["hauthor"])
        mss = step_sem.find_mss(self.mas_schema, suffix="_500_errors")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)
        db.delete(["hauthor"], set([t[1] for t in mss]))
        results = db.execute_query("SELECT hauthor1.* FROM hauthor AS hauthor1, hauthor AS hauthor2 WHERE hauthor1.aid = hauthor2.aid AND hauthor1.oid <> hauthor2.oid;")
        results += db.execute_query("SELECT hauthor1.* FROM hauthor AS hauthor1, hauthor AS hauthor2 WHERE hauthor1.aid = hauthor2.aid AND lower(hauthor1.name) <> lower(hauthor2.name);")
        results += db.execute_query("SELECT hauthor1.* FROM hauthor AS hauthor1, hauthor AS hauthor2 WHERE hauthor1.aid = hauthor2.aid AND lower(hauthor1.organization) <> lower(hauthor2.organization);")
        results += db.execute_query("SELECT hauthor1.* FROM hauthor AS hauthor1, hauthor AS hauthor2 WHERE hauthor1.oid = hauthor2.oid AND lower(hauthor1.organization) <> lower(hauthor2.organization);")

        db.delete_tables(tbl_names)
        print(len(mss))
        self.assertTrue(len(results) == 0)


    def test_tpch(self):
        rules = [("nation", "SELECT nation.* FROM nation WHERE nation.N_REGIONKEY = 1;"),
                 ("customer", "SELECT customer.* FROM customer, nation WHERE nation.N_NATIONKEY = customer.C_NATIONKEY;"),
                 ("supplier", "SELECT supplier.* FROM supplier, nation WHERE nation.N_NATIONKEY = supplier.S_NATIONKEY;")]

        tbl_names = ["region", "nation", "supplier", "customer"]
        db = DatabaseEngine("tpch")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        step_sem = StepSemantics(db, rules, tbl_names)
        mss = step_sem.find_mss(self.tpch_schema)
        # print(mss)
        print(len(mss))

    def test_containment_tpch(self):
        # pogram 1
        # rules = [("partsupp","SELECT partsupp.* FROM partsupp, supplier  WHERE supplier.s_suppkey < 1000 AND supplier.s_suppkey = partsupp.PS_SUPPKEY;"),
        # ("lineitem", "SELECT lineitem.* FROM lineitem, delta_partsupp WHERE delta_partsupp.PS_SUPPKEY = lineitem.l_SUPPKEY AND delta_partsupp.PS_SUPPKEY < 1000;")]

        # pogram 2
        rules = [("partsupp", "SELECT partsupp.* FROM partsupp WHERE partsupp.ps_suppkey < 1000;"),
                 ("lineitem", "SELECT lineitem.* FROM lineitem, delta_partsupp WHERE delta_partsupp.PS_SUPPKEY = lineitem.l_SUPPKEY AND delta_partsupp.PS_SUPPKEY < 1000;")]
        tbl_names = ["lineitem", "partsupp", "supplier"]
        db = DatabaseEngine("tpch")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        stage_sem = StageSemantics(db, rules, tbl_names)
        stage_mss = stage_sem.find_mss()

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        step_sem = StepSemantics(db, rules, tbl_names)
        step_mss = step_sem.find_mss(self.tpch_schema)


        ordkey_step = [(t[0], tuple(t[1][:3])) for t in step_mss]
        ordkey_end = [(t[0], tuple(t[1][:3])) for t in stage_mss]
        print(len([p for p in ordkey_step if p not in ordkey_end]))

