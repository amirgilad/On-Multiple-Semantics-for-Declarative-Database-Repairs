from dba import DatabaseEngine
from semantics import Semantics
import logging

logging.basicConfig(filename='log.log',level=logging.DEBUG)

def test_semantics_basic(is_end = True):
    db_conn = DatabaseEngine()

    # drop DB tables
    db_conn.drop_table('R')
    db_conn.drop_table('P')
    db_conn.drop_table('Delta_R')

    # init database
    names = ['R', 'P', 'Delta_R']
    schemas = ['(ID INT PRIMARY KEY NOT NULL, Y INT NOT NULL, Z INT NOT NULL)' for i in range(3)]
    inserts = [' generate_series(1,10) AS ID, floor(random() * 10 + 1)::int AS Y, floor(random() * 10 + 1)::int AS Z', \
               ' generate_series(1,2) AS ID, floor(random() * 10 + 1)::int AS Y, floor(random() * 10 + 1)::int AS Z', '']
    db_conn.init_database(names, schemas, inserts)

    # print table R
    res = db_conn.execute_query('select * from R')
    print('R IS: ', res)

    # init program and semantics
    sem = Semantics(db_conn, names)
    sem.initRules(names, [' R.ID, R.Y, R.Z FROM R INNER JOIN P ON P.id=R.id'])

    if is_end:
        sem.end_semantics()
    else:
        sem.step_semantics()

    # print table R
    res = db_conn.execute_query('select * from R')
    print('R IS: ', res)


    db_conn.close_connection()


def test_semantics_diff(is_end = True):
    db_conn = DatabaseEngine()

    # drop DB tables
    db_conn.drop_table('R')
    db_conn.drop_table('P')
    db_conn.drop_table('Q')

    # init database
    names = ['R', 'P', 'Q']
    schemas = ['(ID INT PRIMARY KEY NOT NULL)' for i in range(3)]
    inserts = [' 1 AS ID', ' 1 AS ID', ' 1 AS ID']
    db_conn.init_database(names, schemas, inserts)

    # print table R
    res = db_conn.execute_query('select * from P')
    print('P IS: ', res)

    # init program and semantics
    sem = Semantics(db_conn, names)
    sem.initRules(names, [' R.ID FROM R', ' P.ID FROM P INNER JOIN R ON P.id=R.id INNER JOIN Delta_Q ON Delta_Q.id=R.id',
                          ' Q.ID FROM Q INNER JOIN Delta_R ON Q.id=Delta_R.id'])

    if is_end:
        sem.end_semantics()
    else:
        sem.step_semantics()

    # print table R
    res = db_conn.execute_query('select * from P')
    print('P IS: ', res)


    db_conn.close_connection()


# test_semantics_basic()
# test_semantics_basic(is_end=False)

# example where the two semantics don't agree
test_semantics_diff()
test_semantics_diff(is_end=False)