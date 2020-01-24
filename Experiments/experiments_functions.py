from database_generator.dba import DatabaseEngine
from Semantics.end_sem import EndSemantics
from Semantics.stage_sem import StageSemantics
from Semantics.step_sem import StepSemantics
from Semantics.independent_sem import IndependentSemantics
import time
import csv


class Experiments:
    """All experiment functions for a given database and rules. Includes: size comparison, runtime, and containment"""
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

    def __init__(self, rule_file):
        subfolder = ""
        if "tpch" not in rule_file:
            self.tbl_names = ["organization", "author", "publication", "writes", "cite"]
            self.db = DatabaseEngine("cr")
            subfolder = "mas\\"
        else:
            self.tbl_names = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
            self.db = DatabaseEngine("tpch")
            subfolder = "tpch\\"
        self.programs = self.read_rules("..\\data\\" + subfolder + rule_file)
        self.filename = rule_file

    def database_reset(self):
        """reset the database"""
        res = self.db.execute_query("select current_database();")
        db_name = res[0][0]
        self.db.delete_tables(self.tbl_names)
        self.db.close_connection()
        self.db = DatabaseEngine(db_name)
        self.db.load_database_tables(self.tbl_names)

    def find_mss_all_semantics(self, rules):
        """finds the mss for all semantics"""
        schema = self.mas_schema if all(name in self.mas_schema for name in self.tbl_names) else self.tpch_schema

        # self.database_reset()
        # ind_sem = IndependentSemantics(self.db, rules, self.tbl_names)
        # end_sem = EndSemantics(self.db, rules, self.tbl_names)
        # stage_sem = StageSemantics(self.db, rules, self.tbl_names)
        # step_sem = StepSemantics(self.db, rules, self.tbl_names)

        # find mss for end semantics
        self.database_reset()
        # end_sem = EndSemantics(self.db, rules, self.tbl_names)
        start = time.time()
        # mss_end = end_sem.find_mss()
        mss_end = set()
        end = time.time()
        runtime_end = end - start
        print("done with end")
        print("mss size by end:", len(mss_end))

        # find mss for stage semantics
        self.database_reset()
        # stage_sem = StageSemantics(self.db, rules, self.tbl_names)
        start = time.time()
        # mss_stage = stage_sem.find_mss()
        mss_stage = set()
        end = time.time()
        runtime_stage = end - start
        print("done with stage")

        # find mss for step semantics
        self.database_reset()
        step_sem = StepSemantics(self.db, rules, self.tbl_names)
        start = time.time()
        mss_step = step_sem.find_mss(schema)
        end = time.time()
        runtime_step = end - start
        print("done with step")

        # find mss for independent semantics
        self.database_reset()
        # ind_sem = IndependentSemantics(self.db, rules, self.tbl_names)
        start = time.time()
        # mss_ind = ind_sem.find_mss(schema)
        mss_ind = set()
        end = time.time()
        runtime_ind = end - start
        print("done with ind")

        return mss_end, mss_stage, mss_step, mss_ind, runtime_end, runtime_stage, runtime_step, runtime_ind

    def runtime_breakdown_step(self, rules):
        """breakdown of the runtime for step semantics based on the components of the algorithm"""
        self.database_reset()
        step_sem = StepSemantics(self.db, rules, self.tbl_names)

        if not step_sem.rules:   # verify more than 0 rules
            return set()

        # convert the rules so they will store the provenance
        start = time.time()
        prov_rules, prov_tbls, proj = step_sem.gen_prov_rules()

        # evaluate the program and update delta tables
        assignments = step_sem.eval(self.mas_schema, prov_rules, prov_tbls, proj)

        end = time.time()
        runtime_eval = end - start

        # process provenance into a graph
        start = time.time()
        prov_dict = step_sem.gen_prov_dict(assignments)
        step_sem.gen_prov_graph(assignments)
        step_sem.compute_benefits_and_removed_flags()
        end = time.time()
        runtime_process_prov = end - start

        # the "heart" of the algorithm. Traverse the prov. graph by layer
        # and greedily find for each layer the nodes whose derivation will
        # be most beneficial to stabilizing the database
        start = time.time()
        mss = step_sem.traverse_by_layer(prov_dict)
        end = time.time()
        runtime_graph_traversal = end - start

        return runtime_eval, runtime_process_prov, runtime_graph_traversal

    def runtime_breakdown_independent(self, rules):
        """breakdown of the runtime for independent semantics based on the components of the algorithm"""
        ind_sem = IndependentSemantics(self.db, rules, self.tbl_names)

        if not ind_sem.rules:   # verify more than 0 rules
            return set()

        # delete database and reload with all possible and impossible delta tuples
        ind_sem.db.delete_tables(ind_sem.delta_tuples.keys())
        ind_sem.db.load_database_tables(ind_sem.delta_tuples.keys(), is_delta=True)

        # convert the rules so they will store the provenance
        start = time.time()
        prov_rules, prov_tbls, proj = ind_sem.gen_prov_rules()

        # evaluate the program
        assignments = ind_sem.eval(self.mas_schema, prov_rules, prov_tbls, proj)

        end = time.time()
        runtime_eval = end - start

        # process provenance into a formula
        start = time.time()
        ind_sem.process_provenance(assignments)
        bf = ind_sem.convert_to_bool_formula()
        end = time.time()
        runtime_process_prov = end - start

        # bf_size = len(ind_sem.prov_notations.values())

        # find minimum satisfying assignment
        start = time.time()
        sol, size = ind_sem.solve_boolean_formula_with_z3_smt2(bf)

        # process solution to mss
        mss = ind_sem.convert_sat_sol_to_mss(sol)
        end = time.time()
        runtime_solve = end - start

        return runtime_eval, runtime_process_prov, runtime_solve

    def run_experiments(self):
        """"runs size, containment, and runtime comparison between the semantics for all programs"""
        size_results = [["Program Number", "End Size", "Stage Size", "Step Size", "Independent Size"]]
        runtime_results = [["Program Number", "End Runtime", "Stage Runtime", "Step Runtime", "Independent Runtime"]]
        containment_results = [["Program Number", "Stage in End", "Step in End", "Step in Stage", "Stage in Step", "Independent in End",
                                "Independent in Stage", "Independent in Step"]]

        for i in range(len(self.programs)):
            idx = i+1
            rules = self.programs[i]
            mss_end, mss_stage, mss_step, mss_ind, runtime_end, runtime_stage, runtime_step, runtime_ind = \
                self.find_mss_all_semantics(rules)
            size_results.append([idx, len(mss_end), len(mss_stage), len(mss_step), len(mss_ind)])
            runtime_results.append([idx, runtime_end, runtime_stage, runtime_step, runtime_ind])

            # change tuples to strings to comply with independent semantics format
            mss_end_strs = set([(t[0], '('+','.join(str(x) for x in t[1])+')') for t in mss_end])
            mss_stage_strs = set([(t[0], '('+','.join(str(x) for x in t[1])+')') for t in mss_stage])
            mss_step_strs = set([(t[0], '('+','.join(str(x) for x in t[1])+')') for t in mss_end])
            # containment_results.append([idx, mss_stage <= mss_end, mss_step <= mss_end, mss_step <= mss_stage,
            #                             mss_stage <= mss_step, mss_ind <= mss_end_strs, mss_ind <= mss_stage_strs,
            #                             mss_ind <= mss_step_strs])

            mss_end_short = set([(t[0], tuple(t[1][:3])) for t in mss_end])
            mss_stage_short = set([(t[0], tuple(t[1][:3])) for t in mss_stage])
            mss_step_short = set([(t[0], tuple(t[1][:3])) for t in mss_step])

            containment_results.append([idx, mss_stage_short <= mss_end_short, mss_step_short <= mss_end_short, mss_step_short <= mss_stage_short,
                                        mss_stage_short <= mss_step_short, mss_ind <= mss_end_strs, mss_ind <= mss_stage_strs,
                                        mss_ind <= mss_step_strs])

            print("Program ", idx, "/", len(self.programs), "completed")

        prefix = "" if all(name in self.mas_schema for name in self.tbl_names) else "tpch_"
        self.write_to_csv(prefix + "AFTER_FIX_STEP_size_experiments_" + self.filename[:-4] + ".csv", size_results)
        self.write_to_csv(prefix + "AFTER_FIX_STEP_containment_experiments_" + self.filename[:-4] + ".csv", containment_results)
        self.write_to_csv(prefix + "AFTER_FIX_STEP_runtime_experiments_" + self.filename[:-4] + ".csv", runtime_results)

    def run_experiments_breakdown(self, sem):
        """runs the runtime breakdown experiments for step and independent semantics"""
        last = "Traverse" if sem == "step" else "Solve"
        breakdown_results = [["Program Number", "Eval", "Process", last]]
        for i in range(len(self.programs)):
            idx = i+1
            rules = self.programs[i]
            if sem == "step":
                runtime_eval, runtime_process_prov, runtime_last = self.runtime_breakdown_step(rules)
            else:
                runtime_eval, runtime_process_prov, runtime_last = self.runtime_breakdown_independent(rules)
            total = runtime_eval + runtime_process_prov + runtime_last
            breakdown_results.append([idx, runtime_eval / total, runtime_process_prov / total, runtime_last / total])

            print("Program ", idx, "/", len(self.programs), "completed")
        self.write_to_csv("AFTER_FIX_runtime_breakdown_" + sem + "_" + self.filename[:-4] + ".csv", breakdown_results)


    def write_to_csv(self, fname, data):
        """write rows to CSV file"""
        with open("..\\reports\\" + fname, 'w', newline='') as csvFile:
            writer = csv.writer(csvFile)
            writer.writerows(data)
        csvFile.close()

    def read_rules(self, rule_file):
        """read programs from txt file"""
        all_programs = []
        with open(rule_file) as f:
            rules = []
            for line in f:
                if line.strip():
                    tbl, r = line.split("|")
                    rules.append((tbl, r[:-2]))
                else:
                    all_programs.append([r for r in rules])
                    rules = []
            all_programs.append(rules)
        return all_programs


# first set with general assortment of programs
# ex = Experiments("..\\data\\mas\\programs.txt")
# ex.run_experiments()
# ex.run_experiments_breakdown("step")
# ex.run_experiments_breakdown("independent")


# third set with increasing number of rules relying on each other
# ex = Experiments("num_rules_programs.txt")
# ex.run_experiments()
# ex.run_experiments_breakdown("step")
# ex.run_experiments_breakdown("independent")
#
# # first set with general assortment of programs
# ex = Experiments("tpch_programs.txt")
# ex.run_experiments()

# ex = Experiments("..\\data\\tpch\\programs_test_tpch.txt")
# ex.run_experiments()

# # second set with increasing number of joins in a rule
# ex = Experiments("..\\data\\mas\\join_programs.txt")
# ex.run_experiments()
# ex.run_experiments_breakdown("step")
# ex.run_experiments_breakdown("independent")


ex = Experiments("tpch_programs.txt")
ex.run_experiments()