import psycopg2
import logging

from psycopg2._psycopg import IntegrityError


class DatabaseEngine():

    def __init__(self):
        """create a connection to the database cr"""
        self.connection = None
        try:
            self.connection = psycopg2.connect(user = "postgres",
                                               password = "Amiris1",
                                               host = "127.0.0.1",
                                               port = "5432",
                                               database = "postgres")
            self.connection.set_session(readonly=False, autocommit=True)
            self.execute_query('SET search_path TO public, provsql;')

        except (Exception, psycopg2.DatabaseError) as error :
            logging.info("Error while creating PostgreSQL table", error)



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


    def delta_update(self, name):
        sql_delete_query = 'DELETE FROM ' + name + ' USING Delta_' + name + ' WHERE ' + name + '.ID = ' + 'Delta_' + name + '.ID' + ';'
        cursor = self.connection.cursor()
        cursor.execute(sql_delete_query)
        rows_affected = cursor.rowcount
        self.connection.commit()
        logging.info("Deleted from table successfully in PostgreSQL ")
        cursor.close()
        return rows_affected


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


class Rule():

    def __init__(self, conn, table_name, conds):
        self.head = table_name
        self.body = conds
        self.dba = conn
        self.i = 0

    def fire(self):
        if self.i == 0:
            changed = self.fire_start()
        else:
            changed = self.fire_cont()
        self.i += 1
        # if self.head.lower() == 'delta_p':
        #     prov = self.dba.execute_query('select R.id from R inner join delta_P on R.provsql=delta_P.R_id')
        #     prov += self.dba.execute_query('select delta_Q.id from delta_Q inner join delta_P on delta_Q.provsql=delta_P.delta_Q_id')
        #     print('prov of delta_P IS: ', prov)
        return changed

    def fire_cont(self):
        self.dba.execute_query("CREATE TABLE " + self.head + str(self.i) + " AS SELECT " + self.body + ";")
        changed_rows = self.dba.execute_query("INSERT INTO " + self.head + " SELECT * FROM " + self.head + str(self.i) + ";")
        self.dba.execute_query('DROP TABLE ' + self.head + str(self.i) + ';')
        return changed_rows != None and changed_rows > 0

    def fire_start(self):
        self.dba.execute_query('DROP TABLE ' + self.head + ';')
        self.dba.execute_query("CREATE TABLE " + self.head + " AS SELECT " + self.body + ";")
        self.is_first_time = False
        return True