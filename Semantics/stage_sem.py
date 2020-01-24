from Semantics.abs_sem import *
import time

class StageSemantics(AbsSemantics):
    """This class implements stage semantics. This is similar to seminaive
    semantics of deriving the delta tuples, \Delta(t), and deleting their
    regular counterparts, t, at the end of every stage of the evaluation process"""

    def __init__(self, db_conn, rules, tbl_names):
        super(StageSemantics, self).__init__(db_conn, rules, tbl_names)

    def find_mss(self):
        """implementation of end semantics where updates
        to the tables are at the end of each stage of the evaluation"""
        mss = set()
        changed = True
        prev_len = 0

        cnt = 1

        while changed:
            for i in range(len(self.rules)):
                results = self.db.execute_query(self.rules[i][1])
                self.delta_tuples[self.rules[i][0]].update(results)
                mss.update([(self.rules[i][0], row) for row in results])
            changed = prev_len != len(mss)
            prev_len = len(mss)
            # update original tables at the end of each evaluation step
            for i in range(len(self.rules)):
                self.db.delete(self.rules[i], self.delta_tuples[self.rules[i][0]])
                self.db.delta_update(self.rules[i][0], self.delta_tuples[self.rules[i][0]]) # update delta table in db
            cnt += 1
        return mss
