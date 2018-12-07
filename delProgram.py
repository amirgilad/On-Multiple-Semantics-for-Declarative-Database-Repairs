"""Hard coded for now"""
from dba import DatabaseEngine


drop_r = 'DROP TABLE R;'
drop_p = 'DROP TABLE P;'
drop_delta_r = 'DROP TABLE Delta_R;'
create_r = 'CREATE TABLE R (ID INT PRIMARY KEY NOT NULL, Y INT NOT NULL, Z INT NOT NULL);'
populate_r = 'INSERT INTO R SELECT generate_series(1,10) AS ID, floor(random() * 10 + 1)::int AS Y, floor(random() * 10 + 1)::int AS Z;'
create_p = 'CREATE TABLE P (ID INT PRIMARY KEY NOT NULL, Y INT NOT NULL, Z INT NOT NULL);'
populate_p = 'INSERT INTO P SELECT generate_series(1,6) AS ID, floor(random() * 10 + 1)::int AS Y, floor(random() * 10 + 1)::int AS Z;'

dba = DatabaseEngine()

# drop DB tables
dba.execute_query(drop_r)
dba.execute_query(drop_p)
dba.execute_query(drop_delta_r)

# create and populate r
dba.execute_query(create_r)
dba.execute_query(populate_r)

# create and populate p
dba.execute_query(create_p)
dba.execute_query(populate_p)


"""\Delta_R(x) :- R(x), P(x)"""
create_delta_r = 'CREATE TABLE Delta_R AS SELECT * FROM R;'
update_delta_r = 'INSERT INTO Delta_R SELECT R.ID, R.Y, R.Z FROM R, P WHERE R.id=P.id;'
dba.execute_query(create_delta_r)
dba.execute_query(update_delta_r)

dba.execute_query('DELETE FROM R USING Delta_R WHERE R.id = Delta_R.id')

res = dba.execute_query('select * from R')
print(res)

dba.close_connection()