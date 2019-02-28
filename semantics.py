from database_generator.dba import Rule


class Semantics():
    def __init__(self, conn, names):
        self.db_conn = conn
        self.prog = []
        self.table_names = [names[i] for i in range(len(names))]


    def initRules(self, names, conds):
        for i in range(len(conds)):
            r = Rule(self.db_conn, 'Delta_' + names[i], ' ' + conds[i])
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