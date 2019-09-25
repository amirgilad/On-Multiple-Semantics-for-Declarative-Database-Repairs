from database_generator.dba import DatabaseEngine
import json


def db_reset(db):
    # simple db reset
    tbl_names = ["organization", "author", "publication", "writes", "cite"]
    db.delete_tables(tbl_names)
    db.load_database_tables(tbl_names)


def drop_triggers(db):
    db.execute_query("select strip_all_triggers();")

def read_triggers(db, filename):
    funcs = []
    triggers = []
    with open(filename) as json_file:
        data = json.load(json_file)
        funcs_triggers = data["triggers"]
        for f_t in funcs_triggers:
            func = f_t["func"]
            trigger = f_t["trigger"]
            funcs.append(func)
            triggers.append(trigger)
    for f in funcs:
        db.execute_query(f)
    for t in triggers:
        db.execute_query(t)

def gen_event(db):
    """a simple deletion event to make triggers work"""
    db.execute_query("insert into author values (1,'amir',1);")
    db.execute_query("delete from author where aid=1;")


db = DatabaseEngine("cr")
drop_triggers(db)
db_reset(db)
# read_triggers(db, "C:\\Users\\user\\git\\causal-rules\\Experiments\\triggers\\p3_x.json")
# gen_event(db)

# select count(*) from delta_author;