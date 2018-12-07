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



    def create_table(self, name):
        """create a table database cr"""
        query = 'CREATE TABLE ' + name + ';'
        cursor = self.connection.cursor()
        cursor.execute(query)
        self.connection.commit()
        print("Table created successfully in PostgreSQL ")


    def insert_into_table(self, query):
        """create a table database cr"""
        query = 'INSERT INTO ' + name + ';'
        cursor = self.connection.cursor()
        cursor.execute(query)
        self.connection.commit()
        print("Table created successfully in PostgreSQL ")


    def delete_from_table(self, query):
        sql_delete_query = 'DELETE FROM ' + query + ';'
        cursor = self.connection.cursor()
        cursor.execute(sql_delete_query)
        self.connection.commit()


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

        return results