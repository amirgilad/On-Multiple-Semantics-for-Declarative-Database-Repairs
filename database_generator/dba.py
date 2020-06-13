import psycopg2
import logging
import io
from psycopg2._psycopg import IntegrityError
import sys
sys.path.append('..\\')


class DatabaseEngine():

    mas_schema = {
        "author": "(aid, name, oid)",
        "hauthor": "(aid, name, oid, organization)", # for HoloClean comparison
        "writes": "(aid, pid)",
        "publication": "(pid, title, year)",
        "organization": "(oid, name)",
        "cite": "(citing, cited)"
    }
    tpc_h_schema = {"customer": "(c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, C_MKTSEGMENT, c_comment)",
                    "lineitem": "(L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT)",
                    "nation": "(N_NATIONKEY, N_NAME, N_REGIONKEY, N_COMMENT)",
                    "orders": "(O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT)",
                    "part": "(P_PARTKEY, P_NAME, P_MFGR, P_BRAND, P_TYPE, P_SIZE, P_CONTAINER, P_RETAILPRICE, P_COMMENT)",
                    "partsupp": "(PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY, PS_SUPPLYCOST, PS_COMMENT)",
                    "region": "(R_REGIONKEY, R_NAME, R_COMMENT)",
                    "supplier": "(S_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY, S_PHONE, S_ACCTBAL, S_COMMENT)"

                    }
    # for HoloClean comparison
    holocomp_schema = {
        "hospital": "(providernumber, HospitalName, Address1, City, State, ZipCode, CountyName, PhoneNumber, HospitalType, HospitalOwner, EmergencyService, Condition, MeasureCode, MeasureName, Score, Sample, Stateavg)",
        "hauthor": "(aid, name, oid, organization)",
        "flights": "(src, flight, scheduled_dept, actual_dept	dept_gate, scheduled_arrival, actual_arrival, arrival_gate)"
    }

    def __init__(self, db_name):
        # create a connection to the database cr
        self.connection = None
        try:
            self.connection = psycopg2.connect(user = "postgres",
                                               password = "Your Password Here",
                                               host = "127.0.0.1",
                                               port = "5432",
                                               database = db_name)
            self.connection.set_session(readonly=False, autocommit=True)

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

    # def delete(self, rule, rows):
    #     # Hard Coded for DBLP
    #     name = rule[0]
    #     prefix = ""
    #     if name == 'cite':
    #         if 'citing' in rule[1]:
    #             prefix = 'citing'
    #         else:
    #             prefix = 'cited'
    #     elif name[0] != 'w':
    #         prefix = name[0] + "id"
    #     else:
    #         if 'aid' in rule[1]:
    #             prefix = 'aid'
    #         else:
    #             prefix = 'pid'
    #
    #     prefix2 = "" if name[0] != 'w' else 'p'
    #
    #     ps_delete_query = "DELETE FROM " + name + " WHERE " + prefix + " = %s"
    #     if prefix2:
    #         ps_delete_query += " AND " + prefix2 + "id = %s"
    #         rows_to_delete = [(row[0], row[1]) for row in rows]
    #     else:
    #         rows_to_delete = [(row[0], ) for row in rows]
    #     cursor = self.connection.cursor()
    #     cursor.executemany(ps_delete_query, rows_to_delete)
    #     rows_affected = cursor.rowcount
    #     self.connection.commit()
    #     cursor.close()
    #     return rows_affected

    def delete(self,rule,rows):
        def convert_str_tup(s):
            t = s.split(", ")
            t[0] = t[0][1:]
            t[-1] = t[-1][:-1]
            t = tuple(t)
            return t

        name = rule[0]
        if name in self.mas_schema:
            schema = self.mas_schema
        elif name in self.tpc_h_schema:
            schema = self.mas_schema
        else:
            schema = self.holocomp_schema
        prefix = convert_str_tup(schema[name])[0]
        prefix2 = "" if name[0] != 'w' else 'pid'
        ps_delete_query = "DELETE FROM " + name + " WHERE " + prefix + " = %s"
        if prefix2:
            ps_delete_query += " AND " + prefix2 + " = %s"
            rows_to_delete = [(row[0], row[1]) for row in rows]
        else:
            rows_to_delete = [(row[0], ) for row in rows]
            if prefix == 'providernumber':
                rows_to_delete = [(str(row[0]), ) for row in rows]
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

    def create_tables(self, schema):
        # hard coded for dblp, tpch and holoclean comparison
        create_queries_prov_mas = [
            'CREATE TABLE IF NOT EXISTS author (aid int, name varchar(60), oid int);',
            'CREATE TABLE IF NOT EXISTS publication (pid int, title varchar(200), year int);',
            'CREATE TABLE IF NOT EXISTS writes (aid int, pid int);',
            'CREATE TABLE IF NOT EXISTS cite (citing int, cited int);',
            'CREATE TABLE IF NOT EXISTS organization (oid int, name varchar(150));',
            'CREATE TABLE IF NOT EXISTS Delta_author (aid int, name varchar(60), oid int);',
            'CREATE TABLE IF NOT EXISTS Delta_publication (pid int, title varchar(200), year int);',
            'CREATE TABLE IF NOT EXISTS Delta_writes (aid int, pid int);',
            'CREATE TABLE IF NOT EXISTS Delta_cite (citing int, cited int);',
            'CREATE TABLE IF NOT EXISTS Delta_organization (oid int, name varchar(150));'
        ]
        create_queries_prov_tpch = [
            "CREATE TABLE IF NOT EXISTS delta_customer (C_CUSTKEY int NOT NULL, C_NAME varchar(25) NOT NULL, C_ADDRESS varchar(40) NOT NULL, C_NATIONKEY int NOT NULL, C_PHONE char(15) NOT NULL, C_ACCTBAL decimal(15,2) NOT NULL, C_MKTSEGMENT char(10) NOT NULL, C_COMMENT varchar(117) NOT NULL);",
            "CREATE TABLE IF NOT EXISTS delta_lineitem (L_ORDERKEY int NOT NULL, L_PARTKEY int NOT NULL, L_SUPPKEY int NOT NULL, L_LINENUMBER int NOT NULL, L_QUANTITY decimal(15,2) NOT NULL, L_EXTENDEDPRICE decimal(15,2) NOT NULL, L_DISCOUNT decimal(15,2) NOT NULL, L_TAX decimal(15,2) NOT NULL, L_RETURNFLAG char(1) NOT NULL, L_LINESTATUS char(1) NOT NULL, L_SHIPDATE date NOT NULL, L_COMMITDATE date NOT NULL, L_RECEIPTDATE date NOT NULL, L_SHIPINSTRUCT char(25) NOT NULL, L_SHIPMODE char(10) NOT NULL, L_COMMENT varchar(44) NOT NULL);",
            "CREATE TABLE IF NOT EXISTS delta_nation (N_NATIONKEY int NOT NULL, N_NAME char(25) NOT NULL, N_REGIONKEY int NOT NULL, N_COMMENT varchar(152) DEFAULT NULL);",
            "CREATE TABLE IF NOT EXISTS delta_orders (O_ORDERKEY int NOT NULL, O_CUSTKEY int NOT NULL, O_ORDERSTATUS char(1) NOT NULL, O_TOTALPRICE decimal(15,2) NOT NULL, O_ORDERDATE date NOT NULL, O_ORDERPRIORITY char(15) NOT NULL, O_CLERK char(15) NOT NULL, O_SHIPPRIORITY int NOT NULL, O_COMMENT varchar(79) NOT NULL);",
            "CREATE TABLE IF NOT EXISTS delta_part (P_PARTKEY int NOT NULL, P_NAME varchar(55) NOT NULL, P_MFGR char(25) NOT NULL, P_BRAND char(10) NOT NULL, P_TYPE varchar(25) NOT NULL, P_SIZE int NOT NULL, P_CONTAINER char(10) NOT NULL, P_RETAILPRICE decimal(15,2) NOT NULL, P_COMMENT varchar(23) NOT NULL);",
            "CREATE TABLE IF NOT EXISTS delta_partsupp (PS_PARTKEY int NOT NULL, PS_SUPPKEY int NOT NULL, PS_AVAILQTY int NOT NULL, PS_SUPPLYCOST decimal(15,2) NOT NULL, PS_COMMENT varchar(199) NOT NULL);",
            "CREATE TABLE IF NOT EXISTS delta_region (R_REGIONKEY int NOT NULL, R_NAME char(25) NOT NULL, R_COMMENT varchar(152) DEFAULT NULL);",
            "CREATE TABLE IF NOT EXISTS delta_supplier (S_SUPPKEY int NOT NULL, S_NAME char(25) NOT NULL, S_ADDRESS varchar(40) NOT NULL, S_NATIONKEY int NOT NULL, S_PHONE char(15) NOT NULL, S_ACCTBAL decimal(15,2) NOT NULL, S_COMMENT varchar(101) NOT NULL);",
            "CREATE TABLE IF NOT EXISTS customer (C_CUSTKEY int NOT NULL, C_NAME varchar(25) NOT NULL, C_ADDRESS varchar(40) NOT NULL, C_NATIONKEY int NOT NULL, C_PHONE char(15) NOT NULL, C_ACCTBAL decimal(15,2) NOT NULL, C_MKTSEGMENT char(10) NOT NULL, C_COMMENT varchar(117) NOT NULL);",
            "CREATE TABLE IF NOT EXISTS lineitem (L_ORDERKEY int NOT NULL, L_PARTKEY int NOT NULL, L_SUPPKEY int NOT NULL, L_LINENUMBER int NOT NULL, L_QUANTITY decimal(15,2) NOT NULL, L_EXTENDEDPRICE decimal(15,2) NOT NULL, L_DISCOUNT decimal(15,2) NOT NULL, L_TAX decimal(15,2) NOT NULL, L_RETURNFLAG char(1) NOT NULL, L_LINESTATUS char(1) NOT NULL, L_SHIPDATE date NOT NULL, L_COMMITDATE date NOT NULL, L_RECEIPTDATE date NOT NULL, L_SHIPINSTRUCT char(25) NOT NULL, L_SHIPMODE char(10) NOT NULL, L_COMMENT varchar(44) NOT NULL);",
            "CREATE TABLE IF NOT EXISTS nation (N_NATIONKEY int NOT NULL, N_NAME char(25) NOT NULL, N_REGIONKEY int NOT NULL, N_COMMENT varchar(152) DEFAULT NULL);",
            "CREATE TABLE IF NOT EXISTS orders (O_ORDERKEY int NOT NULL, O_CUSTKEY int NOT NULL, O_ORDERSTATUS char(1) NOT NULL, O_TOTALPRICE decimal(15,2) NOT NULL, O_ORDERDATE date NOT NULL, O_ORDERPRIORITY char(15) NOT NULL, O_CLERK char(15) NOT NULL, O_SHIPPRIORITY int NOT NULL, O_COMMENT varchar(79) NOT NULL);",
            "CREATE TABLE IF NOT EXISTS part (P_PARTKEY int NOT NULL, P_NAME varchar(55) NOT NULL, P_MFGR char(25) NOT NULL, P_BRAND char(10) NOT NULL, P_TYPE varchar(25) NOT NULL, P_SIZE int NOT NULL, P_CONTAINER char(10) NOT NULL, P_RETAILPRICE decimal(15,2) NOT NULL, P_COMMENT varchar(23) NOT NULL);",
            "CREATE TABLE IF NOT EXISTS partsupp (PS_PARTKEY int NOT NULL, PS_SUPPKEY int NOT NULL, PS_AVAILQTY int NOT NULL, PS_SUPPLYCOST decimal(15,2) NOT NULL, PS_COMMENT varchar(199) NOT NULL);",
            "CREATE TABLE IF NOT EXISTS region (R_REGIONKEY int NOT NULL, R_NAME char(25) NOT NULL, R_COMMENT varchar(152) DEFAULT NULL);",
            "CREATE TABLE IF NOT EXISTS supplier (S_SUPPKEY int NOT NULL, S_NAME char(25) NOT NULL, S_ADDRESS varchar(40) NOT NULL, S_NATIONKEY int NOT NULL, S_PHONE char(15) NOT NULL, S_ACCTBAL decimal(15,2) NOT NULL, S_COMMENT varchar(101) NOT NULL);"
        ]
        create_queries_prov_holo = [
            "CREATE TABLE IF NOT EXISTS hauthor (aid int, name varchar(60), oid int, organization varchar(100));",
            "CREATE TABLE IF NOT EXISTS hospital (ProviderNumber varchar(10), HospitalName varchar(40), Address1 varchar(40), City varchar(20), State char(5), Zipcode char(7), CountyName char(20), PhoneNumber char(10), HospitalType char(25), HospitalOwner char(100), EmergencyService char(7), Condition varchar(100), MeasureCode varchar(25), MeasureName varchar(200), Score varchar(5), Sample varchar(20), Stateavg varchar(20));",
            "CREATE TABLE IF NOT EXISTS flights (src char(40), flight varchar(60), scheduled_dept varchar(60), actual_dept varchar(60), dept_gate varchar(50), scheduled_arrival  varchar(60), arrival_gate varchar(50));",
            "CREATE TABLE IF NOT EXISTS Delta_hospital (ProviderNumber varchar(10), HospitalName varchar(40), Address1 varchar(40), City varchar(20), State char(5), Zipcode char(7), CountyName char(20), PhoneNumber char(10), HospitalType char(25), HospitalOwner char(100), EmergencyService char(7), Condition varchar(100), MeasureCode varchar(25), MeasureName varchar(200), Score varchar(5), Sample varchar(20), Stateavg varchar(20));"
            "CREATE TABLE IF NOT EXISTS Delta_hauthor (aid int, name varchar(60), oid int, organization varchar(100));",
            "CREATE TABLE IF NOT EXISTS Delta_flights (src char(40), flight varchar(60), scheduled_dept varchar(60), actual_dept varchar(60), dept_gate varchar(50), scheduled_arrival  varchar(60), arrival_gate varchar(50));"
        ]
        cursor = self.connection.cursor()
        queries = []
        if schema == self.mas_schema:
            queries = create_queries_prov_mas
        elif schema == self.tpc_h_schema:
            queries = create_queries_prov_tpch
        else:
            queries = create_queries_prov_holo
        for cq in queries:
            cursor.execute(cq)
        self.connection.commit()
        cursor.close()

    def delete_tables(self, lst_names):
        for name in lst_names:
            # specific for HoloClean experiments
            if any(char.isdigit() for char in name):
                name = name.split("_")[0]
            # self.execute_query("DELETE FROM " + name + ";")
            # self.execute_query("DELETE FROM " + "delta_" + name + ";")
            self.execute_query("DROP TABLE IF EXISTS " + name + ";")
            self.execute_query("DROP TABLE IF EXISTS " + "delta_" + name + ";")

    def load_database_tables(self, lst_names, is_delta=False):

        schema = None
        path = ""
        s_name = ""
        if all(name in self.mas_schema for name in lst_names):
            schema = self.mas_schema
            path = "..\\data\\mas\\"
        elif all(name in self.tpc_h_schema for name in lst_names):
            schema = self.tpc_h_schema
            path = "..\\data\\tpch\\"
        else:
            schema = self.holocomp_schema
            path = "..\\data\\holocomp\\"
        self.create_tables(schema) # create tables if they do not exists
        cursor = self.connection.cursor()
        for name in lst_names:
            # specific for HoloClean experiments
            if any(char.isdigit() for char in name):
                s_name = name.split("_")[0]
            else:
                s_name = name
            with open(path+name+".csv") as f:
                # cursor.copy_expert("COPY " + name + schema[name] + " FROM STDIN DELIMITER ',' CSV HEADER;", f)
                cursor.copy_expert("COPY " + s_name + schema[s_name] + " FROM STDIN DELIMITER ',' CSV;", f)
            if is_delta:
                with open(path+name+".csv") as f:
                    # cursor.copy_expert("COPY delta_" + name + schema[name] + " FROM STDIN DELIMITER ',' CSV HEADER;", f)
                    cursor.copy_expert("COPY delta_" + s_name + schema[s_name] + " FROM STDIN DELIMITER ',' CSV;", f)


    def drop_table(self, name):
        res = self.execute_query("SELECT to_regclass('" + name + "');")
        if res[0][0] != None:
            self.execute_query('DROP TABLE ' + name)
        res = self.execute_query("SELECT to_regclass('Delta_" + name + "');")
        if res[0][0] != None:
            self.execute_query('DROP TABLE Delta_' + name)
        logging.info("Deleted table " + name + " successfully in PostgreSQL ")

    def save_tbl_as_csv(self, tbl_name, f_name):
        save_query = "COPY " + tbl_name + " to 'C:\\Users\\Public\\" + f_name + "' csv header;"
        self.execute_query(save_query)

    def execute_query(self, query):
        """execute a query on the database"""
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


