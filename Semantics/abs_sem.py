from abc import ABC, abstractmethod
from database_generator.dba import DatabaseEngine


class AbsSemantics(ABC):
    """Abstract class for all semantics. Enforces the method find_mss"""

    def __init__(self, db_conn, rules, tbl_names):
        assert isinstance(rules, list)
        assert isinstance(tbl_names, list)
        assert isinstance(db_conn, DatabaseEngine)

        self.db = db_conn # database connector
        self.rules = rules # delta rules (SQL queries)
        self.delta_tuples = {k: set() for k in tbl_names} # a dict storing the tuples to be updated in each table
        super().__init__()

    @abstractmethod
    def find_mss(self):
        pass
