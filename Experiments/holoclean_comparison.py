from database_generator.dba import DatabaseEngine
from Semantics.end_sem import EndSemantics
from Semantics.stage_sem import StageSemantics
from Semantics.step_sem import StepSemantics
from Semantics.independent_sem import IndependentSemantics
import time
import csv


class HoloCompare:
    holocomp_schema = {
        "hospital": ("ProviderNumber", "HospitalName", "Address1", "City", "State", "ZipCode", "CountyName", "PhoneNumber", "HospitalType", "HospitalOwner", "EmergencyService", "Condition", "MeasureCode", "MeasureName", "Score", "Sample", "Stateavg"),
        "hauthor": ('aid', 'name', 'oid', 'organization'),
        "flights": ('src', 'flight', 'scheduled_dept', 'actual_dept	dept_gate', 'scheduled_arrival', 'actual_arrival', 'arrival_gate')
    }

    def __init__(self, rule_file, tics=[0], isErrors=True):
        if "hauthor" in rule_file and tics != []:
            self.tbl_names = ["hauthor"]
            if isErrors:
                self.tbl_files = [["hauthor_"+str(err)+"_errors"] for err in tics]
            else:
                self.tbl_files = [["hauthor_"+str(err)+"_rows"] for err in tics]
        else:
            self.tbl_names = ["hospital"]
            self.tbl_files = [["hospital"]]
        self.db = DatabaseEngine("holocomp")
        self.programs = self.read_rules(rule_file)
        self.filename = rule_file

    def database_reset(self, db_idx):
        """reset the database"""
        res = self.db.execute_query("select current_database();")
        db_name = res[0][0]
        self.db.delete_tables(self.tbl_names)
        self.db.close_connection()
        self.db = DatabaseEngine(db_name)
        self.db.load_database_tables(self.tbl_files[db_idx])

    def find_mss_all_semantics(self, rules, db_idx=0):
        """finds the mss for all semantics"""
        schema = self.holocomp_schema

        self.database_reset(db_idx)
        ind_sem = IndependentSemantics(self.db, rules, self.tbl_names)
        end_sem = EndSemantics(self.db, rules, self.tbl_names)
        stage_sem = StageSemantics(self.db, rules, self.tbl_names)
        step_sem = StepSemantics(self.db, rules, self.tbl_names)

        # find mss for end semantics
        self.database_reset(db_idx)
        end_sem = EndSemantics(self.db, rules, self.tbl_names)
        start = time.time()
        mss_end = end_sem.find_mss()
        end = time.time()
        runtime_end = end - start
        self.db.save_tbl_as_csv(self.tbl_names[0], "fixed_end.csv")
        self.check_zero_violations_semantics()
        print("done with end")
        print("mss size by end:", len(mss_end))

        # find mss for stage semantics
        self.database_reset(db_idx)
        stage_sem = StageSemantics(self.db, rules, self.tbl_names)
        start = time.time()
        mss_stage = stage_sem.find_mss()
        end = time.time()
        runtime_stage = end - start
        self.check_zero_violations_semantics()
        self.db.save_tbl_as_csv(self.tbl_names[0], "fixed_stage.csv")
        print("done with stage")

        # find mss for step semantics
        self.database_reset(db_idx)
        step_sem = StepSemantics(self.db, rules, self.tbl_names)
        start = time.time()
        mss_step = step_sem.find_mss(schema)
        end = time.time()
        runtime_step = end - start
        self.db.delete(self.tbl_names, set([t[1] for t in mss_step]))
        self.check_zero_violations_semantics()
        self.db.save_tbl_as_csv(self.tbl_names[0], "fixed_step.csv")
        print("done with step")

        # find mss for independent semantics
        self.database_reset(db_idx)
        ind_sem = IndependentSemantics(self.db, rules, self.tbl_files[db_idx])
        start = time.time()
        suf = self.tbl_files[db_idx][0][7:] if self.tbl_names[0] == "hauthor" else ""
        mss_ind = ind_sem.find_mss(schema, suffix=suf)
        end = time.time()
        runtime_ind = end - start
        set_to_delete = [tuple(map(str, t[1][1:-1].split(','))) for t in mss_ind]
        self.db.delete(self.tbl_names, set_to_delete)
        self.db.save_tbl_as_csv(self.tbl_names[0], "fixed_ind.csv")
        self.check_zero_violations_semantics()
        print("done with ind")

        return mss_end, mss_stage, mss_step, mss_ind, runtime_end, runtime_stage, runtime_step, runtime_ind

    def change_programs(self):
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

        prefix = self.tbl_names[0] + "_"
        self.write_to_csv(prefix + "size_experiments_" + self.filename[:-4] + ".csv", size_results)
        self.write_to_csv(prefix + "containment_experiments_" + self.filename[:-4] + ".csv", containment_results)
        self.write_to_csv(prefix + "runtime_experiments_" + self.filename[:-4] + ".csv", runtime_results)

    def change_databases(self, isErrors=True):
        """"runs size, containment, and runtime comparison between the semantics for all programs"""
        var = "Errors" if isErrors else "Rows"
        size_results = [[var, "End Size", "Stage Size", "Step Size", "Independent Size"]]
        runtime_results = [[var, "End Runtime", "Stage Runtime", "Step Runtime", "Independent Runtime"]]
        containment_results = [[var, "Stage in End", "Step in End", "Step in Stage", "Stage in Step", "Independent in End",
                                "Independent in Stage", "Independent in Step"]]

        for i in range(len(self.tbl_files)):
            rules = self.programs[0]
            mss_end, mss_stage, mss_step, mss_ind, runtime_end, runtime_stage, runtime_step, runtime_ind = \
                self.find_mss_all_semantics(rules, db_idx=i)
            size_results.append([self.tbl_files[i], len(mss_end), len(mss_stage), len(mss_step), len(mss_ind)])
            runtime_results.append([self.tbl_files[i], runtime_end, runtime_stage, runtime_step, runtime_ind])

            # change tuples to strings to comply with independent semantics format
            mss_end_strs = set([(t[0], '('+','.join(str(x) for x in t[1])+')') for t in mss_end])
            mss_stage_strs = set([(t[0], '('+','.join(str(x) for x in t[1])+')') for t in mss_stage])
            mss_step_strs = set([(t[0], '('+','.join(str(x) for x in t[1])+')') for t in mss_end])
            containment_results.append([self.tbl_files[i][0], mss_stage <= mss_end, mss_step <= mss_end, mss_step <= mss_stage,
                                        mss_stage <= mss_step, mss_ind <= mss_end_strs, mss_ind <= mss_stage_strs,
                                        mss_ind <= mss_step_strs])

            # mss_end_short = set([(t[0], tuple(t[1][:3])) for t in mss_end])
            # mss_stage_short = set([(t[0], tuple(t[1][:3])) for t in mss_stage])
            # mss_step_short = set([(t[0], tuple(t[1][:3])) for t in mss_step])

            # containment_results.append([self.tbl_files[i], mss_stage_short <= mss_end_short, mss_step_short <= mss_end_short, mss_step_short <= mss_stage_short,
            #                             mss_stage_short <= mss_step_short, mss_ind <= mss_end_strs, mss_ind <= mss_stage_strs,
            #                             mss_ind <= mss_step_strs])

            print("Database number", i+1, "/", len(self.tbl_files), "completed")

        prefix = var + "_changing_" + self.tbl_names[0] + "_"
        self.write_to_csv(prefix + "size_experiments_" + self.filename[:-4] + ".csv", size_results)
        self.write_to_csv(prefix + "containment_experiments_" + self.filename[:-4] + ".csv", containment_results)
        self.write_to_csv(prefix + "runtime_experiments_" + self.filename[:-4] + ".csv", runtime_results)

    def get_conflicting_tuples(self, fixed, clean):
        with open(fixed, 'r', newline='\n') as t1, open(clean, 'r') as t2:
            reader_fixed = csv.reader(t1, delimiter=',', quotechar='|')
            reader_clean = csv.reader(t2, delimiter=',', quotechar='|')

            fixed_lines = []
            for line in reader_fixed:
                fixed_lines.append(line)
            clean_lines = []
            for line in reader_clean:
                clean_lines.append([val for val in line if val])

            num_rows_fixed_not_in_clean = 0
            for line in fixed_lines:
                if line not in clean_lines:
                    num_rows_fixed_not_in_clean += 1
            print("number of rows in fixed file that are not in the clean file:", num_rows_fixed_not_in_clean)

    def count_violations(self):
        err = [100, 200, 300, 500, 700, 1000]
        for e in err:
            self.count_violations_datbase("hauthor_"+str(e)+"_errors")

    def count_violations_datbase(self, fixed_name):
        '''Get the fixed database of Holoclean and count the nubmer of violating tuples for each constraints'''
        res = self.db.execute_query("select current_database();")
        db_name = res[0][0]
        self.db.delete_tables(self.tbl_names)
        self.db.close_connection()
        self.db = DatabaseEngine(db_name)
        self.db.load_database_tables([fixed_name])

        data = []
        total_set = set()
        dcs = self.programs[0]
        for dc in dcs:
            total_set.update(set(self.db.execute_query(dc[1])))
            num_violations = len(set(self.db.execute_query(dc[1])))
            data.append([dc, num_violations])
        data.append(["Total", len(total_set)])
        self.write_to_csv("holoclean_remaining_violations_"+ fixed_name +".csv", data)

    def check_zero_violations_semantics(self):
        cnt = 0
        dcs = self.programs[0]
        for dc in dcs:
            cnt += len(set(self.db.execute_query(dc[1])))
        print("num violations:", cnt)

    def write_to_csv(self, fname, data):
        """write rows to CSV file"""
        with open("..\\reports\\" + fname, 'w', newline='') as csvFile:
            writer = csv.writer(csvFile)
            writer.writerows(data)
        csvFile.close()

    def read_rules(self, rule_file):
        """read programs from txt file"""
        all_programs = []
        with open("..\\data\\holocomp\\"+rule_file) as f:
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


err_num = [100, 200, 300, 500, 700, 1000]
ex = HoloCompare("holoclean_hauthor_programs.txt", err_num)
# ex.change_databases()
ex.count_violations()
# ex.count_violations_datbase("hauthor_100_errors")

# row_num = [2000, 3000, 4000, 5000, 6000, 7000, 8000]
# ex = HoloCompare("holoclean_hauthor_programs.txt", row_num, isErrors=False)
# ex.change_databases(isErrors=False)

# ex = HoloCompare("holoclean_hospital_programs.txt")
# ex.change_programs()