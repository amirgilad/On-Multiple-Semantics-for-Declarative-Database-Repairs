import psycopg2

conn = psycopg2.connect(dbname='dblp_plus', user='postgres',
                            password="Amiris1", host="127.0.0.1", port="5432")
cursor = conn.cursor()

drop_queries = [
        'DROP TABLE IF EXISTS author;',
        'DROP TABLE IF EXISTS publication;',
        'DROP TABLE IF EXISTS writes;',
        'DROP TABLE IF EXISTS cite;',
        'DROP TABLE IF EXISTS organization;',
    ]

create_queries = [
        'CREATE TABLE author (aid int, name varchar(60), oid int);',
        'CREATE TABLE publication (pid int, title varchar(200), year int);',
        'CREATE TABLE writes (aid int, pid int);',
        'CREATE TABLE cite (citing int, cited int);',
        'CREATE TABLE organization (oid int, name varchar(150));'
        ]

for dq in drop_queries:
        cursor.execute(dq)
conn.commit()
for cq in create_queries:
        cursor.execute(cq)
conn.commit()


is_laptop = False

usr = 'amirgilad' if is_laptop else 'agilad'
path = "/home/"+usr+"/PycharmProjects/causal-rules/database_generator/"

# LAPTOP
# insert_queries = ["COPY author(aid,name,oid)  FROM '/home/amirgilad/PycharmProjects/causal-rules/database_generator/author.csv' DELIMITER ',' CSV HEADER;",
#                   "COPY publication(pid,title,year)  FROM '/home/amirgilad/PycharmProjects/causal-rules/database_generator/publication.csv' DELIMITER ',' CSV HEADER;",
#                   "COPY writes(aid,pid)  FROM '/home/amirgilad/PycharmProjects/causal-rules/database_generator/writes.csv' DELIMITER ',' CSV HEADER;",
#                   "COPY cite(citing,cited)  FROM '/home/amirgilad/PycharmProjects/causal-rules/database_generator/cite.csv' DELIMITER ',' CSV HEADER;",
#                   "COPY organization(oid,name)  FROM '/home/amirgilad/PycharmProjects/causal-rules/database_generator/organization.csv' DELIMITER ',' CSV HEADER;"
#                   ]

insert_queries = ["COPY author(aid,name,oid)  FROM '"+ path + "author.csv' DELIMITER ',' CSV HEADER;",
                  "COPY publication(pid,title,year)  FROM '"+ path + "publication.csv' DELIMITER ',' CSV HEADER;",
                  "COPY writes(aid,pid)  FROM '"+ path + "writes.csv' DELIMITER ',' CSV HEADER;",
                  "COPY cite(citing,cited)  FROM '"+ path + "cite.csv' DELIMITER ',' CSV HEADER;",
                  "COPY organization(oid,name)  FROM '"+ path + "organization.csv' DELIMITER ',' CSV HEADER;"
                  ]

for iq in insert_queries:
        cursor.execute(iq)
conn.commit()

cursor.close()
conn.close()