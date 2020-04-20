from database_generator.dba import DatabaseEngine
from old.semantics import Semantics
from old import DependecyRemover as dr

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

# init program and semantics
sem = Semantics(db_conn, names)
# for name in names:
#     rel_name = "ID_" + name
#     # db_conn.drop_table(rel_name)
#     db_conn.execute_query("drop table " + rel_name + ";")
#     db_conn.execute_query("SELECT create_provenance_mapping('" + rel_name + "','" + name + "','ID');")
    # db_conn.commit()

# sem.initRules(names,
#               [" *, formula(provenance(), 'ID_R') FROM (SELECT R.ID, R.provsql FROM R) t",
#                " *, formula(provenance(), 'ID_P') FROM (SELECT P.ID FROM P INNER JOIN R ON P.id=R.id INNER JOIN Delta_Q ON Delta_Q.id=R.id) t",
#                " *, formula(provenance(), 'ID_Q') FROM (SELECT Q.ID FROM Q INNER JOIN Delta_R ON Q.id=Delta_R.id) t"])

sem.initRules(names, [' R.ID FROM R', ' P.ID, R.provsql as R_id, delta_Q.provsql as delta_Q_id FROM P, R, Delta_Q where P.id=R.id and Delta_Q.id=R.id',
                      ' Q.ID, delta_R.id as delta_R_id FROM Q, delta_R where Q.id=Delta_R.id'])
# sem.initRules(names, [' R.ID, R.provsql FROM R', 'Q.ID FROM Q', 'P.ID FROM P'])

g = dr.DependencyGraph(sem.prog)
print(g)
g.remove_dependencies()
db_conn.close_connection()