import psycopg2

conn = psycopg2.connect(dbname='dblp_plus', user='postgres',
                            password="Amiris1", host="127.0.0.1", port="5432")
cursor = conn.cursor()

create_queries_prov = [
        'CREATE TABLE author (aid int(11), name varchar(60), oid int(11), papar_count int(11), citation_count int(11));',
        'CREATE TABLE publication (wid int(11), cid int(11), title varchar(100), year int(11));',
        'CREATE TABLE writes (aid int(11), wid int(11));',
        'CREATE TABLE cite (citing int(11), cited int(11));',
        'CREATE TABLE organization (oid int(11), name varchar(100));'
        ]