"""Hard coded for now"""
from database_generator.dba import DatabaseEngine, Rule
import logging
db_conn = DatabaseEngine()
logging.basicConfig(filename='log.log',level=logging.DEBUG)


# drop DB tables
db_conn.drop_table('R')
db_conn.drop_table('P')
db_conn.drop_table('first_r')
db_conn.drop_table('last_r')
db_conn.drop_table('first_p')
db_conn.drop_table('last_p')


names = ['R', 'P']
# schemas = ['(ID INT PRIMARY KEY NOT NULL, Y INT NOT NULL, Z INT NOT NULL)' for i in range(2)]
schemas = ['(ID SERIAL PRIMARY KEY, first varchar NOT NULL, last varchar NOT NULL)' for i in range(2)]
inserts = ['', '']

# inserts = [' generate_series(1,10) AS ID, floor(random() * 10 + 1)::int AS Y, floor(random() * 10 + 1)::int AS Z',\
#            ' generate_series(1,2) AS ID, floor(random() * 10 + 1)::int AS Y, floor(random() * 10 + 1)::int AS Z']
db_conn.init_database(names, schemas, inserts)


db_conn.execute_query("INSERT INTO R (first,last) VALUES ('amir', 'gilad'), ('iris', 'gilad'), ('alon', 'gilad');")
db_conn.execute_query("INSERT INTO P (first,last) VALUES ('eli', 'a'), ('or', 'b'), ('a', 'gilad');")


res = db_conn.execute_query('select * from R')
print('R IS: ', res)

res = db_conn.execute_query('select * from P')
print('P IS: ', res)


# define the rule \Delta_R(x) :- R(x), P(x) and fire it
# r = Rule(db_conn, 'Delta_R', ' R.ID, R.Y, R.Z FROM R INNER JOIN P ON P.id=R.id')
# db_conn.execute_query("SELECT *, formula(provenance(), 'first') FROM (SELECT R.ID, R.first, R.last FROM R INNER JOIN P ON P.last=R.last) t;")
# db_conn.execute_query("SELECT create_provenance_mapping('last_r','R','last');")
# db_conn.execute_query("SELECT create_provenance_mapping('last_p','P','last');")
# db_conn.execute_query("SELECT create_provenance_mapping('first_r','R','first');")
# db_conn.execute_query("SELECT create_provenance_mapping('first_p','P','first');")
r = Rule(db_conn, 'Delta_R', "*, where_provenance(provenance()) FROM (SELECT DISTINCT R.ID, R.first, R.last FROM R INNER JOIN P ON P.last=R.last) t;")
is_changed = r.fire()
# print("Has rule '\Delta_R(x) :- R(x), P(x)' changed table Delta_R? ", is_changed)
# update_delta_r = 'INSERT INTO Delta_R (ID, Y, Z) (SELECT R.ID, R.Y, R.Z FROM R INNER JOIN P ON P.id=R.id);'
# dba.execute_query(update_delta_r)

res = db_conn.execute_query('select * from delta_R')
print('delta_R is now: ', res)

# update R
rowcount = db_conn.delta_update('R')
print('Rows deleted from R during delta update: ', rowcount)
res = db_conn.execute_query('select * from R')
print('R is now: ', res)

# drop DB tables
db_conn.drop_table('R')
db_conn.drop_table('P')

db_conn.close_connection()