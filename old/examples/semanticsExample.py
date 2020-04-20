from database_generator.dba import DatabaseEngine
from old.semantics import Semantics
import logging
from old import provenanceHandler as ph

# from provFormula import solve

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
    db_conn = DatabaseEngine("cr")

    # drop DB tables
    db_conn.drop_table('R')
    db_conn.drop_table('P')
    db_conn.drop_table('Q')
    db_conn.drop_table('Delta_R1')

    # init database
    names = ['R', 'P', 'Q']
    schemas = ['(ID INT PRIMARY KEY NOT NULL)' for i in range(3)]
    inserts = [' 1 AS ID', ' 1 AS ID', ' 1 AS ID']
    db_conn.init_database(names, schemas, inserts)

    # init program and semantics
    sem = Semantics(db_conn, names)
    # sem.initRules(names, [' R.ID FROM R', ' P.ID FROM P INNER JOIN R ON P.id=R.id INNER JOIN Delta_Q ON Delta_Q.id=R.id',
    #                       ' Q.ID FROM Q INNER JOIN Delta_R ON Q.id=Delta_R.id'])
    sem.initRules(names,
                  [' *, where_provenance(provenance()) FROM (SELECT R.ID, R.provsql FROM R) t', ' *, where_provenance(provenance()) FROM (SELECT Delta_Q.ID FROM Delta_Q INNER JOIN R ON Delta_Q.id=R.id INNER JOIN P ON P.id=R.id) t',
                   ' *, where_provenance(provenance()) FROM (SELECT Q.ID FROM Q INNER JOIN Delta_R ON Q.id=Delta_R.id) t'])

    # ' *, where_provenance(provenance()) FROM (SELECT P.ID FROM P INNER JOIN R ON P.id=R.id INNER JOIN Delta_Q ON Delta_Q.id=R.id) t'

    if is_end:
        sem.end_semantics()
    else:
        sem.step_semantics()

    # print table P
    res = db_conn.execute_query('select * from P')
    print('P IS: ', res)

    res = db_conn.execute_query('select * from delta_P')
    print('delta_P IS: ', res)
    # res1 = db_conn.execute_query('select * from delta_Q')
    # print('delta_Q IS: ', res1)
    res += db_conn.execute_query('select * from delta_R')
    res += db_conn.execute_query('select * from delta_Q')
    # print(res)
    prov_map = ph.make_prov_map(res)
    # print(prov_map)

    # print(makeDNF(prov_map))
    # solve(prov_map)


    db_conn.close_connection()


# test_semantics_basic()
# test_semantics_basic(is_end=False)

# example where the two semantics don't agree
test_semantics_diff()
test_semantics_diff(is_end=False)