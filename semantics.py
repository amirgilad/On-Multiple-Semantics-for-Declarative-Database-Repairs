from dba import DatabaseEngine

class Semantics():
    def __init__(self):
        self.dba = DatabaseEngine()


    def initdb(self, names, schemas, inserts):
        table_names = names + ['Delta_'+ names[i] for i in range[len(names)]]
        # table_schemas = ['(X INT PRIMARY KEY NOT NULL)' for i in range(6)]
        self.dba.init_database(table_names, schemas, inserts)
        # inserts = ['floor(random() * 10 + 1)::int' for i in range(6)]



    def end_semantics(self):
        pass
        # update all delta tables according to the rules while there are rules to be satisfied
        # update positive tables


    def step_semantics(self):
        pass
        # while there are rules to be satisfied,
        # 1. update all delta tables according to the rules
        # 2. update positive tables minus deltas