from Semantics.abs_sem import *


class EndSemantics(AbsSemantics):
    """This class implements end semantics. This is standard datalog semantics of deriving all delta tuples, \Delta(t),
    and deleting their regular counterparts, t, at the end of the evaluation process"""

    def __init__(self, db_conn, rules, tbl_names):
        super(EndSemantics, self).__init__(db_conn, rules, tbl_names)

    def find_mss(self):
        """implementation of end semantics where updates
        to the rules are at the end of the evaluation"""
        mss = set()
        changed = True
        prev_len = 0
        while changed:
            for i in range(len(self.rules)):
                results = self.db.execute_query(self.rules[i][1])
                self.delta_tuples[self.rules[i][0]].update(results)
                mss.update([(self.rules[i][0], row) for row in results])
                self.db.delta_update(self.rules[i][0], results)   # update delta table in db
            changed = prev_len != len(mss)
            prev_len = len(mss)
        # update original tables at the end of the evaluation
        for i in range(len(self.rules)):
            self.db.delete(self.rules[i], self.delta_tuples[self.rules[i][0]])
        return mss
