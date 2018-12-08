from dba import DatabaseEngine, Rule


class Semantics():
    def __init__(self):
        self.db_conn = DatabaseEngine()
        self.prog = []
        self.table_names = []


    # def initdb(self, names, schemas, inserts):
    #     db_tbl_names = names + ['Delta_'+ names[i] for i in range[len(names)]]
    #     # table_schemas = ['(X INT PRIMARY KEY NOT NULL)' for i in range(6)]
    #     self.dba.init_database(db_tbl_names, schemas, inserts)
    #     # inserts = ['floor(random() * 10 + 1)::int' for i in range(6)]
    #     self.table_names = [names[i] for i in range(len(names))]


    def initRules(self, names, conds):
        for i in range(len(names)):
            r = Rule(self.db_conn, 'Delta'+names[i], ' '+conds[i])
            self.prog.append(r)


    def end_semantics(self):
        """implementation of end semantics where updates
        to the rules are at the end of the evaluation"""
        changed = True
        while changed:
            changed = False
            for i in range(len(self.prog)):
                cur_status = self.prog[i].fire()
                changed = changed or cur_status
        # update original tables at the end of the evaluation
        for i in range(len(self.prog)):
            self.db_conn.delta_update(self.table_names[i])


    def step_semantics(self):
        """implementation of step semantics where updates to
        the rules are done at the end of each step of evaluation"""
        changed = True
        while changed:
            changed = False
            for i in range(len(self.prog)):
                cur_status = self.prog[i].fire()
                changed = changed or cur_status
            # update original tables at the end of each evaluation step
            for i in range(len(self.prog)):
                self.db_conn.delta_update(self.table_names[i])