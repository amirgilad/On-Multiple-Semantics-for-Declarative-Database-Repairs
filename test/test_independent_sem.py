from Semantics.independent_sem import IndependentSemantics
from Semantics.end_sem import EndSemantics
from database_generator.dba import DatabaseEngine
import unittest


class TestIndependentSemantics(unittest.TestCase):

    mas_schema = {"author": ('aid',
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

    def test_undefined_connection(self):
        """test no db connection"""
        rules = [("author", "SELECT * FROM author WHERE author.aid = 58525;")]
        tbl_names = ["organization", "author", "publication", "writes"]
        db = None
        self.assertRaises(AssertionError, IndependentSemantics, db, rules, tbl_names)

    def test_no_rules(self):
        """test no rules case. MSS supposed to be empty as db is stable"""
        rules = []
        tbl_names = ["organization", "author", "publication", "writes"]
        db = DatabaseEngine("cr")
        ind_sem = IndependentSemantics(db, rules, tbl_names)
        mss = ind_sem.find_mss(self.mas_schema)

        self.assertEqual(mss, set(), "MSS supposed to be empty! Instead its " + str(mss))

    def test_gen_prov_rules(self):
        """test func that takes delta rules and turns them to rules that return provenance"""
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
        """test func that handles and stores the provenance of each tuple"""
        rules = [("author", "SELECT author.* FROM author, writes WHERE author.name LIKE '%m%' AND author.aid = writes.aid;")]
        tbl_names = ["organization", "author", "publication", "writes"]
        db = DatabaseEngine("cr")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        ind_sem = IndependentSemantics(db, rules, tbl_names)
        prov_rules, prov_tbls, proj = ind_sem.gen_prov_rules()
        cur_prov = db.execute_query(prov_rules[0][1])
        assignments = ind_sem.rows_to_prov(cur_prov, prov_tbls[0], self.mas_schema, proj, prov_rules[0])
        self.assertTrue(all(a[0][0] == 'delta_author' for a in assignments))

    def test_solve_boolean_formula_with_z3_smt2(self):
        """test func that finds the minimum satisfying assignment to a boolean formula"""
        bf = '(and (or a b) (not (and a c)))'

        rules = []
        tbl_names = []
        db = DatabaseEngine("cr")

        ind_sem = IndependentSemantics(db, rules, tbl_names)
        ind_sem.prov_notations = {'a': 'a', 'b': 'b', 'c': 'c'}
        sol = ind_sem.solve_boolean_formula_with_z3_smt2(bf)
        self.assertTrue(all(assign in str(sol) for assign in ["a = True", "b = True", "c = False"]))

    def test_solve_boolean_formula_with_z3_smt2_not(self):
        """test func that finds the minimum satisfying assignment to a boolean formula"""
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
        """test func that converts the assignments into the provenance of each tuple"""
        rules = [("author", "SELECT author.* FROM author, writes WHERE author.name LIKE '%m%' AND author.aid = writes.aid;")]
        tbl_names = ["organization", "author", "publication", "writes"]
        db = DatabaseEngine("cr")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        ind_sem = IndependentSemantics(db, rules, tbl_names)
        prov_rules, prov_tbls, proj = ind_sem.gen_prov_rules()
        cur_prov = db.execute_query(prov_rules[0][1])
        assignments = ind_sem.rows_to_prov(cur_prov, prov_tbls[0], self.mas_schema, proj, prov_rules[0])
        ind_sem.process_provenance(assignments)
        self.assertTrue(all("delta_" in k[0] for k in ind_sem.provenance))

    def test_convert_to_bool_formula(self):
        """test func that takes the provenance and converts it into a bool formula"""
        rules = [("author", "SELECT author.* FROM author, writes WHERE author.name LIKE '%m%' AND author.aid = writes.aid;")]
        tbl_names = ["organization", "author", "publication", "writes"]
        db = DatabaseEngine("cr")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        ind_sem = IndependentSemantics(db, rules, tbl_names)
        prov_rules, prov_tbls, proj = ind_sem.gen_prov_rules()
        cur_prov = db.execute_query(prov_rules[0][1])
        assignments = ind_sem.rows_to_prov(cur_prov, prov_tbls[0], self.mas_schema, proj, prov_rules[0])
        ind_sem.process_provenance(assignments)
        bf = ind_sem.convert_to_bool_formula()
        assert (len(ind_sem.prov_notations.keys()) == len(set(ind_sem.prov_notations.values())))
        sol = ind_sem.solve_boolean_formula_with_z3_smt2(bf)
        print(bf)
        print(sol)

    def test_mss_easy_case(self):
        """test case with two simple rules"""
        rules = [("author", "SELECT author.* FROM author WHERE author.name like '%m%';"), ("writes", "SELECT writes.* FROM writes WHERE writes.aid = 58525;")]
        tbl_names = ["organization", "author", "publication", "writes"]
        db = DatabaseEngine("cr")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        ind_sem = IndependentSemantics(db, rules, tbl_names)

        results = db.execute_query("SELECT author.* FROM author WHERE author.name like '%m%';")
        results += db.execute_query("SELECT writes.* FROM writes WHERE writes.aid = 58525;")
        mss = ind_sem.find_mss(self.mas_schema)
        # print("size of mss is ", len(mss), "and size of results is ", len(results))
        self.assertTrue(len(mss) == len(results))

    def test_mss_easy_case_2(self):
        """test case with two simple rules"""
        rules = [("author", "SELECT author.* FROM author WHERE lower(author.name) like 'zohar dvir';"), ("writes", "SELECT writes.* FROM writes WHERE writes.aid = 58525;")]
        tbl_names = ["organization", "author", "publication", "writes"]
        db = DatabaseEngine("cr")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        ind_sem = IndependentSemantics(db, rules, tbl_names)

        results = db.execute_query("SELECT author.* FROM author WHERE lower(author.name) like 'zohar dvir';")
        results += db.execute_query("SELECT writes.* FROM writes WHERE writes.aid = 58525;")
        mss = ind_sem.find_mss(self.mas_schema)
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

        ind_sem = IndependentSemantics(db, rules, tbl_names)
        mss = ind_sem.find_mss(self.mas_schema)
        # MSS should only include the author tuple with aid = 100920
        self.assertTrue(len(mss) == 1 and '100920' == next(iter(mss))[1][1:7])

    def test_mss_hard_case_2(self):
        """test case with two rules with the same body"""
        rules = [("author", "SELECT author.* FROM author WHERE author.aid = 100920;"),
                 ("writes", "SELECT writes.* FROM writes, delta_author WHERE writes.aid = delta_author.aid;"),
                 ("publication", "SELECT publication.* FROM publication, delta_writes, author WHERE publication.pid = delta_writes.pid AND delta_writes.aid = author.aid;")]
        tbl_names = ["organization", "author", "publication", "writes"]
        db = DatabaseEngine("cr")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        ind_sem = IndependentSemantics(db, rules, tbl_names)
        mss = ind_sem.find_mss(self.mas_schema)
        print(mss)
        # MSS should only include the author tuple with aid = 100920
        # self.assertTrue(len(mss) == 1 and '100920' == next(iter(mss))[1][1:7])

    def test_mss_hard_case_3(self):
        """test case with two rules with the same body"""
        rules = [("publication", "SELECT publication.* FROM publication WHERE publication.pid = 2352376;"),
                 ("cite", "SELECT cite.* FROM cite, delta_publication WHERE cite.citing = delta_publication.pid AND cite.citing < 2700;"),
                 ("cite", "SELECT cite.* FROM cite, delta_publication WHERE cite.cited = delta_publication.pid AND cite.cited < 2700;")]
        tbl_names = ["organization", "author", "publication", "writes"]
        db = DatabaseEngine("cr")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        ind_sem = IndependentSemantics(db, rules, tbl_names)
        mss = ind_sem.find_mss(self.mas_schema)
        print(mss)
        # MSS should only include the author tuple with aid = 100920
        # self.assertTrue(len(mss) == 1 and '100920' == next(iter(mss))[1][1:7])

    def test_mss_hard_case_4(self):
        """test case with two dependent rules
        SHOWS THAT CONVERTING TO STRING FORMAT DOES NOT WORK FOR AUTHORS, SO EVEN THOUGH EXPERIMENTS SHOW THAT MSS_IND IS
        NOT CONTAINED IN MSS_END, IT IS IF IT IS OF THE SAME SIZE"""
        rules = [("organization", "SELECT organization.* FROM organization WHERE organization.oid = 16045;"),
                 ("author", "SELECT author.* FROM author, delta_organization WHERE author.oid = delta_organization.oid;")]
        tbl_names = ["organization", "author", "publication", "writes", "cite"]
        db = DatabaseEngine("cr")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        ind_sem = IndependentSemantics(db, rules, tbl_names)
        mss_ind = ind_sem.find_mss(self.mas_schema)
        end_sem = EndSemantics(db, rules, tbl_names)
        mss_end = end_sem.find_mss()
        mss_end_strs = set([(t[0], '('+','.join(str(x) for x in t[1])+')') for t in mss_end])
        # mss according to end should be equal to mss according to ind
        self.assertTrue(len(mss_end_strs) ==  len(mss_ind))

    def test_mss_hard_case_5(self):
        """test case with two rules with the same body"""
        rules = [("author", "SELECT author.* FROM author, organization WHERE author.oid = organization.oid AND organization.oid = 16045;"),
                 ("organization", "SELECT organization.* FROM author, organization WHERE author.oid = organization.oid AND organization.oid = 16045;")]
        tbl_names = ["organization", "author", "publication", "writes"]
        db = DatabaseEngine("cr")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        ind_sem = IndependentSemantics(db, rules, tbl_names)
        mss = ind_sem.find_mss(self.mas_schema)
        print(mss)
        # MSS should only include the organization tuple with aid = 100920
        self.assertTrue(len(mss) == 1 and '16045' in next(iter(mss))[1])

    def test_mss_recursive_case(self):
        """test case with one simple rule"""
        rules = [("author", "SELECT author.* FROM author WHERE author.aid = 100920;"), ("writes", "SELECT writes.* FROM writes, delta_author WHERE writes.aid = delta_author.aid;")]
        tbl_names = ["organization", "author", "publication", "writes"]
        db = DatabaseEngine("cr")

        # reset the database
        # db.delete_tables(tbl_names)
        # db.load_database_tables(tbl_names)

        ind_sem = IndependentSemantics(db, rules, tbl_names)
        mss = ind_sem.find_mss(self.mas_schema)
        print(mss)
        self.assertTrue(len(mss) == 3 and all('100920' in t[1] for t in mss))

    def test_mutually_recursive(self):
        rules = [("publication", "SELECT publication.* FROM publication WHERE publication.pid = 2352376;"),
                 ("cite", "SELECT cite.* FROM cite, delta_publication WHERE delta_publication.pid = cite.cited AND cite.cited = 2352376;"),
                 ("cite", "SELECT cite.* FROM cite WHERE cite.cited = 2352376;"),
                 ("publication", "SELECT publication.* FROM publication, delta_cite WHERE publication.pid = delta_cite.cited AND delta_cite.cited = 2352376;")
                 ]
        tbl_names = ["organization", "author", "publication", "writes", "cite"]
        db = DatabaseEngine("cr")

        ind_sem = IndependentSemantics(db, rules, tbl_names)
        mss = ind_sem.find_mss(self.mas_schema)
        print(mss)
        self.assertTrue(len(mss) == 5 and all('2352376' in t[1] for t in mss))

    def test_tpch(self):
        rules = [("nation", "SELECT nation.* FROM nation WHERE nation.N_REGIONKEY = 1;"),
                 ("customer", "SELECT customer.* FROM customer, nation WHERE nation.N_NATIONKEY = customer.C_NATIONKEY;"),
                 ("supplier", "SELECT supplier.* FROM supplier, nation WHERE nation.N_NATIONKEY = supplier.S_NATIONKEY;")]

        tbl_names = ["region", "nation", "supplier", "customer"]
        db = DatabaseEngine("tpch")

        # reset the database
        db.delete_tables(tbl_names)
        db.load_database_tables(tbl_names)

        ind_sem = IndependentSemantics(db, rules, tbl_names)
        mss = ind_sem.find_mss(self.tpch_schema)
        # print(mss)
        print(len(mss))
