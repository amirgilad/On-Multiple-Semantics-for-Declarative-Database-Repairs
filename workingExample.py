"""Hard coded for now"""
from dba import DatabaseEngine, Rule
db_conn = DatabaseEngine()

# drop DB tables
db_conn.drop_table('R')
db_conn.drop_table('P')
db_conn.drop_table('Delta_R')

names = ['R', 'P', 'Delta_R']
schemas = ['(UNIQUE ID INT PRIMARY KEY NOT NULL, Y INT NOT NULL, Z INT NOT NULL)' for i in range(3)]
inserts = [' generate_series(1,10) AS ID, floor(random() * 10 + 1)::int AS Y, floor(random() * 10 + 1)::int AS Z',\
           ' generate_series(1,2) AS ID, floor(random() * 10 + 1)::int AS Y, floor(random() * 10 + 1)::int AS Z', '']
db_conn.init_database(names, schemas, inserts)

res = db_conn.execute_query('select * from R')
print('R IS: ', res)

res = db_conn.execute_query('select * from P')
print('P IS: ', res)

# \Delta_R(x) :- R(x), P(x)
r = Rule(db_conn, 'Delta_R', ' R.ID, R.Y, R.Z FROM R INNER JOIN P ON P.id=R.id')
r.fire()
# update_delta_r = 'INSERT INTO Delta_R (ID, Y, Z) (SELECT R.ID, R.Y, R.Z FROM R INNER JOIN P ON P.id=R.id);'
# dba.execute_query(update_delta_r)

res = db_conn.execute_query('select * from Delta_R')
print('delta_R IS: ', res)
# update R
rowcount = db_conn.delta_update('R', 'R.id = Delta_R.id')
# dba.execute_query('DELETE FROM R USING Delta_R WHERE R.id = Delta_R.id')

res = db_conn.execute_query('select * from R')
print('R is now: ', res)
print('rows affected: ', rowcount)


db_conn.close_connection()