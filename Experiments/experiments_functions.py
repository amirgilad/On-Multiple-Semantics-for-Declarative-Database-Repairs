from database_generator.dba import DatabaseEngine
from Semantics.end_sem import EndSemantics
from Semantics.stage_sem import StageSemantics
from Semantics.step_sem import StepSemantics
from Semantics.independent_sem import IndependentSemantics
import time
import csv


class Experiments:
    """All experiment functions for a given database and rules. Includes: size comparison, runtime, and containment"""
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

    def __init__(self, rule_file):
        self.tbl_names = ["organization", "author", "publication", "writes"]
        self.db = DatabaseEngine("cr")
        self.programs = self.read_rules(rule_file)

    def database_reset(self):
        """reset the database"""
        self.db.delete_tables(self.tbl_names)
        self.db.load_database_tables(self.tbl_names)

    def find_mss_all_semantics(self, rules):
        """finds the mss for all semantics"""
        self.database_reset()
        ind_sem = IndependentSemantics(self.db, rules, self.tbl_names)
        end_sem = EndSemantics(self.db, rules, self.tbl_names)
        stage_sem = StageSemantics(self.db, rules, self.tbl_names)
        step_sem = StepSemantics(self.db, rules, self.tbl_names)

        # find mss for independent semantics
        self.database_reset()
        start = time.time()
        mss_ind = ind_sem.find_mss(self.schema)
        end = time.time()
        runtime_ind = end - start

        # find mss for end semantics
        self.database_reset()
        start = time.time()
        mss_end = end_sem.find_mss()
        end = time.time()
        runtime_end = end - start

        # find mss for stage semantics
        self.database_reset()
        start = time.time()
        mss_stage = stage_sem.find_mss()
        end = time.time()
        runtime_stage = end - start

        # find mss for step semantics
        self.database_reset()
        start = time.time()
        mss_step = step_sem.find_mss(self.schema)
        end = time.time()
        runtime_step = end - start

        return mss_end, mss_stage, mss_step, mss_ind, runtime_end, runtime_stage, runtime_step, runtime_ind

    def run_experiments(self):
        """"runs size, containment, and runtime comparison between the semantics for all programs"""
        size_results = [["Program Number", "End Size", "Stage Size", "Step Size", "Independent Size"]]
        runtime_results = [["Program Number", "End Runtime", "Stage Runtime", "Step Runtime", "Independent Runtime"]]
        containment_results = [["Program Number", "Stage in End", "Step in End", "Step in Stage", "Independent in End",
                                "Independent in Stage", "Independent in Step"]]

        for i in range(len(self.programs)):
            idx = i+1
            rules = self.programs[i]
            mss_end, mss_stage, mss_step, mss_ind, runtime_end, runtime_stage, runtime_step, runtime_ind = \
                self.find_mss_all_semantics(rules)
            size_results.append([idx, len(mss_end), len(mss_stage), len(mss_step), len(mss_ind)])
            runtime_results.append([idx, runtime_end, runtime_stage, runtime_step, runtime_ind])

            # change tuples to strings to comply with independent semantics format
            mss_end_strs = set([(t[0], str(t[1]).replace("'", "").replace(", ", ",")) for t in mss_end])
            mss_stage_strs = set([(t[0], str(t[1]).replace("'", "").replace(", ", ",")) for t in mss_stage])
            mss_step_strs = set([(t[0], str(t[1]).replace("'", "").replace(", ", ",")) for t in mss_step])
            containment_results.append([idx, mss_stage <= mss_end, mss_step <= mss_end, mss_step <= mss_stage,
                                        mss_ind <= mss_end_strs, mss_ind <= mss_stage_strs, mss_ind <= mss_step_strs])

            print("Program ", idx, "/", len(self.programs), "completed")

        self.write_to_csv("size_experiments.csv", size_results)
        self.write_to_csv("containment_experiments.csv", containment_results)
        self.write_to_csv("runtime_experiments.csv", runtime_results)

    def write_to_csv(self, fname, data):
        """write rows to CSV file"""
        with open(fname, 'w', newline='') as csvFile:
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


ex = Experiments("programs.txt")
ex.run_experiments()
