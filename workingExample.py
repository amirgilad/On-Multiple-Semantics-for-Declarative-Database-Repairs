"""Hard coded for now"""
from dba import DatabaseEngine, Rule
import logging
db_conn = DatabaseEngine()
logging.basicConfig(filename='log.log',level=logging.DEBUG)


names = ['R', 'P', 'Delta_R']
schemas = ['(ID INT PRIMARY KEY NOT NULL, Y INT NOT NULL, Z INT NOT NULL)' for i in range(3)]
inserts = [' generate_series(1,10) AS ID, floor(random() * 10 + 1)::int AS Y, floor(random() * 10 + 1)::int AS Z',\
           ' generate_series(1,2) AS ID, floor(random() * 10 + 1)::int AS Y, floor(random() * 10 + 1)::int AS Z', '']
db_conn.init_database(names, schemas, inserts)

res = db_conn.execute_query('select * from R')
print('R IS: ', res)

res = db_conn.execute_query('select * from P')
print('P IS: ', res)

# define the rule \Delta_R(x) :- R(x), P(x) and fire it
r = Rule(db_conn, 'Delta_R', ' R.ID, R.Y, R.Z FROM R INNER JOIN P ON P.id=R.id')
is_changed = r.fire()
print("Has rule '\Delta_R(x) :- R(x), P(x)' changed table Delta_R? ", is_changed)
# update_delta_r = 'INSERT INTO Delta_R (ID, Y, Z) (SELECT R.ID, R.Y, R.Z FROM R INNER JOIN P ON P.id=R.id);'
# dba.execute_query(update_delta_r)

# update R
rowcount = db_conn.delta_update('R')
print('Rows deleted from R during delta update: ', rowcount)
res = db_conn.execute_query('select * from R')
print('R is now: ', res)

# drop DB tables
db_conn.drop_table('R')
db_conn.drop_table('P')
db_conn.drop_table('Delta_R')

db_conn.close_connection()