import psycopg2
import logging
import io
from psycopg2._psycopg import IntegrityError


class DatabaseEngine():

    def __init__(self, db_name):
        # create a connection to the database cr
        self.connection = None
        try:
            self.connection = psycopg2.connect(user = "postgres",
                                               password = "Amiris1",
                                               host = "127.0.0.1",
                                               port = "5432",
                                               database = db_name)
            self.connection.set_session(readonly=False, autocommit=True)
            # self.execute_query('SET search_path TO public, provsql;')

        except (Exception, psycopg2.DatabaseError) as error :
            logging.info("Error while creating PostgreSQL table", error)

        # self.create_deltas()


    def create_semiring_functions(self):
        with self.connection as conn:
            cursor = conn.cursor()
            cursor.execute(open("functions.sql", "r").read())
            cursor.close()

    def close_connection(self):
        """close connection to the database cr"""
        if(self.connection):
            self.connection.close()
            logging.info("PostgreSQL connection is closed")



    def init_database(self, names, schemas, inserts):
        assert len(names) == len(schemas)
        for i in range(len(names)):
            self.create_table(names[i], schemas[i])
            self.create_table('Delta_' + names[i], schemas[i])
            if len(inserts[i]) > 0:
                self.insert_into_table(names[i], inserts[i])


    def create_table(self, name, schema):
        """create a table database cr"""
        query = 'CREATE TABLE ' + name + ' ' + schema + ';'
        cursor = self.connection.cursor()
        cursor.execute(query)
        self.connection.commit()
        self.execute_query("SELECT add_provenance('" + name + "');")
        cursor.close()
        logging.info("Table " + name + " created successfully in PostgreSQL ")


    def insert_into_table(self, name, insert):
        """create a table database cr"""
        query = 'INSERT INTO ' + name + ' SELECT ' + insert + ';'
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            rows_affected = cursor.rowcount
        except IntegrityError:
            rows_affected = 0

        self.connection.commit()
        cursor.close()
        logging.info("Insert into table " + name + " successfully in PostgreSQL ")
        return rows_affected

    def delete(self, rule, rows):
        # Hard Coded for DBLP
        name = rule[0]
        prefix = ""
        if name == 'cite':
            if 'citing' in rule[1]:
                prefix = 'citing'
            else:
                prefix = 'cited'
        elif name[0] != 'w':
            prefix = name[0] + "id"
        else:
            if 'aid' in rule[1]:
                prefix = 'aid'
            else:
                prefix = 'pid'

        prefix2 = "" if name[0] != 'w' else 'p'

        ps_delete_query = "DELETE FROM " + name + " WHERE " + prefix + " = %s"
        if prefix2:
            ps_delete_query += " AND " + prefix2 + "id = %s"
            rows_to_delete = [(row[0], row[1]) for row in rows]
        else:
            rows_to_delete = [(row[0], ) for row in rows]
        cursor = self.connection.cursor()
        cursor.executemany(ps_delete_query, rows_to_delete)
        rows_affected = cursor.rowcount
        self.connection.commit()
        cursor.close()
        return rows_affected

    def delta_update(self, name, rows):
        attr_num = 0 if len(rows) == 0 else len(next(iter(rows)))
        row_shape = "(" + "%s,"*attr_num
        row_shape = row_shape[:-1] + ")"
        sql_insert_query = "INSERT INTO " + "Delta_" + name + " VALUES " + row_shape
        cursor = self.connection.cursor()
        rows_affected = 0
        if len(rows) > 0:
            cursor.executemany(sql_insert_query, rows)
            rows_affected = cursor.rowcount
            self.connection.commit()
        cursor.close()
        return rows_affected

    def create_deltas(self):
        # hard coded for dblp
        create_queries_prov = [
            'CREATE TABLE Delta_author (aid int, name varchar(60), oid int);',
            'CREATE TABLE Delta_publication (pid int, title varchar(200), year int);',
            'CREATE TABLE Delta_writes (aid int, pid int);',
            'CREATE TABLE Delta_cite (citing int, cited int);',
            'CREATE TABLE Delta_organization (oid int, name varchar(150));'
        ]
        cursor = self.connection.cursor()
        for cq in create_queries_prov:
            cursor.execute(cq)
        self.connection.commit()
        cursor.close()

    def delete_tables(self, lst_names):
        for name in lst_names:
            self.execute_query("DELETE FROM " + name + ";")
            self.execute_query("DELETE FROM " + "delta_" + name + ";")

    def load_database_tables(self, lst_names, is_delta=False):
        # hard coded for dblp
        schema = {
            "author": "(aid, name, oid)",
            "writes": "(aid, pid)",
            "publication": "(pid, title, year)",
            "organization": "(oid, name)",
            "cite": "(citing, cited)"
        }
        cursor = self.connection.cursor()
        for name in lst_names:
            with open("C:\\Users\\user\\git\\causal-rules\\database_generator\\"+name+".csv") as f:
                cursor.copy_expert("COPY " + name + schema[name] + " FROM STDIN DELIMITER ',' CSV HEADER;", f)
            if is_delta:
                with open("C:\\Users\\user\\git\\causal-rules\\database_generator\\"+name+".csv") as f:
                    cursor.copy_expert("COPY delta_" + name + schema[name] + " FROM STDIN DELIMITER ',' CSV HEADER;", f)



    # def delta_update(self, name):
    #     sql_delete_query = 'DELETE FROM ' + name + ' USING Delta_' + name + ' WHERE ' + name + '.ID = ' + 'Delta_' + name + '.ID' + ';'
    #     cursor = self.connection.cursor()
    #     cursor.execute(sql_delete_query)
    #     rows_affected = cursor.rowcount
    #     self.connection.commit()
    #     logging.info("Deleted from table successfully in PostgreSQL ")
    #     cursor.close()
    #     return rows_affected


    def drop_table(self, name):
        res = self.execute_query("SELECT to_regclass('" + name + "');")
        if res[0][0] != None:
            self.execute_query('DROP TABLE ' + name)
        res = self.execute_query("SELECT to_regclass('Delta_" + name + "');")
        if res[0][0] != None:
            self.execute_query('DROP TABLE Delta_' + name)
        logging.info("Deleted table " + name + " successfully in PostgreSQL ")



    def execute_query(self, query):
        """execute a query on the database cr"""
        results = None
        cursor = self.connection.cursor()
        cursor.execute(query)
        lowerq = query.lower()
        if 'insert' in lowerq or 'drop' in lowerq or 'create' in lowerq:
            self.connection.commit()
        if cursor.description != None:
            results = cursor.fetchall()

        cursor.close()
        return results


# class Rule():
#
#     def __init__(self, conn, table_name, conds):
#         self.head = table_name
#         self.body = conds
#         self.dba = conn
#         self.i = 0
#
#     def fire(self):
#         if self.i == 0:
#             changed = self.fire_start()
#         else:
#             changed = self.fire_cont()
#         self.i += 1
#         # if self.head.lower() == 'delta_p':
#         #     prov = self.dba.execute_query('select R.id from R inner join delta_P on R.provsql=delta_P.R_id')
#         #     prov += self.dba.execute_query('select delta_Q.id from delta_Q inner join delta_P on delta_Q.provsql=delta_P.delta_Q_id')
#         #     print('prov of delta_P IS: ', prov)
#         return changed
#
#     def fire_cont(self):
#         self.dba.execute_query("CREATE TABLE " + self.head + str(self.i) + " AS SELECT " + self.body + ";")
#         changed_rows = self.dba.execute_query("INSERT INTO " + self.head + " SELECT * FROM " + self.head + str(self.i) + ";")
#         self.dba.execute_query('DROP TABLE ' + self.head + str(self.i) + ';')
#         return changed_rows != None and changed_rows > 0
#
#     def fire_start(self):
#         self.dba.execute_query('DROP TABLE ' + self.head + ';')
#         self.dba.execute_query("CREATE TABLE " + self.head + " AS SELECT " + self.body + ";")
#         self.is_first_time = False
#         return True