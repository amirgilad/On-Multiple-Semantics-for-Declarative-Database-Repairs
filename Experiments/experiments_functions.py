from database_generator.dba import DatabaseEngine
from Semantics.end_sem import EndSemantics
from Semantics.stage_sem import StageSemantics
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

    def __init__(self):
        self.tbl_names = ["organization", "author", "publication", "writes"]
        self.db = DatabaseEngine("cr")

    def database_reset(self):
        # reset the database
        self.db.delete_tables(self.tbl_names)
        self.db.load_database_tables(self.tbl_names)

    def find_mss_all_semantics(self, rules):
        """finds the mss for all semantics"""
        self.database_reset()
        ind_sem = IndependentSemantics(self.db, rules, self.tbl_names)
        end_sem = EndSemantics(self.db, rules, self.tbl_names)
        stage_sem = StageSemantics(self.db, rules, self.tbl_names)
        # TODO: add algorithm for step here
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

        return mss_end, mss_stage, mss_ind, runtime_end, runtime_stage, runtime_ind

    def run_experiments(self, rules):
        size_results = [["End Size", "Stage Size", "Independent Size"]]
        containment_results = [["Stage in End", "Independent in End", "Independent in Stage"]]
        runtime_results = [["End Runtime", "Stage Runtime", "Independent Runtime"]]
        mss_end, mss_stage, mss_ind, runtime_end, runtime_stage, runtime_ind = self.find_mss_all_semantics(rules)
        size_results.append([len(mss_end), len(mss_stage), len(mss_ind)])
        containment_results.append([mss_stage <= mss_end, mss_ind <= mss_end, mss_ind <= mss_stage])
        runtime_results.append([runtime_end, runtime_stage, runtime_ind])

    def write_to_csv(fname, data):
        with open(fname, 'w') as csvFile:
            writer = csv.writer(csvFile)
            writer.writerows(data)
        csvFile.close()
