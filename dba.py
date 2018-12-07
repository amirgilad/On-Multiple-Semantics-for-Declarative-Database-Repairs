import psycopg2

class DatabaseEngine():

    def __init__(self):
        """create a connection to the database cr"""
        self.connection = None
        try:
            self.connection = psycopg2.connect(user = "postgres",
                                               password = "Amiris1",
                                               host = "127.0.0.1",
                                               port = "5432",
                                               database = "cr")

        except (Exception, psycopg2.DatabaseError) as error :
            print("Error while creating PostgreSQL table", error)



    def close_connection(self):
        """close connection to the database cr"""
        if(self.connection):
            self.connection.close()
            print("PostgreSQL connection is closed")



    def init_database(self, names, schemas, inserts):
        assert len(names) == len(schemas)
        for i in range(len(names)):
            self.create_table(names[i], schemas[i])
            if len(inserts[i]) > 0:
                self.insert_into_table(names[i], inserts[i])


    def create_table(self, name, schema):
        """create a table database cr"""
        query = 'CREATE TABLE ' + name + schema + ';'
        cursor = self.connection.cursor()
        cursor.execute(query)
        self.connection.commit()
        cursor.close()
        print("Table " + name + " created successfully in PostgreSQL ")


    def insert_into_table(self, name, insert):
        """create a table database cr"""
        query = 'INSERT INTO ' + name + ' SELECT ' + insert + ';'
        cursor = self.connection.cursor()
        cursor.execute(query)
        self.connection.commit()
        cursor.close()
        print("Insert into table " + name + " successfully in PostgreSQL ")


    def delta_update(self, name, conds):
        sql_delete_query = 'DELETE FROM ' + name + ' USING Delta_' + name + ' WHERE ' + conds + ';'
        cursor = self.connection.cursor()
        cursor.execute(sql_delete_query)
        rows_affected = cursor.rowcount
        self.connection.commit()
        print("Deleted from table successfully in PostgreSQL ")
        cursor.close()
        return rows_affected


    def drop_table(self, name):
        res = self.execute_query("SELECT to_regclass('" + name + "');")
        if res[0][0] != None:
            self.execute_query('DROP TABLE ' + name)
        print("Deleted table " + name + " successfully in PostgreSQL ")



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

    def fire(self):
        self.dba.insert_into_table(self.head, self.body)

    # def is_different(self):
    #     query = 'select * from ' + name_before + ' minus select * from ' + self.head + ';'