import psycopg2

conn = psycopg2.connect(dbname='cr', user='postgres',
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

insert_queries = [
                    # "\copy publication(pid,title,year) FROM 'C:\\Users\\user\\git\\causal-rules\\database_generator\\publication.csv' DELIMITER ',' CSV HEADER;"
                  "\copy author(aid,name,oid)  FROM 'C:\\Users\\user\\git\\causal-rules\\database_generator\\author.csv' DELIMITER ',' CSV HEADER;",
                  "\copy publication(pid,title,year)  FROM 'C:\\Users\\user\\git\\causal-rules\\database_generator\\publication.csv' DELIMITER ',' CSV HEADER;",
                  "\copy writes(aid,pid)  FROM 'C:\\Users\\user\\git\\causal-rules\\database_generator\\writes.csv' DELIMITER ',' CSV HEADER;",
                  "\copy cite(citing,cited)  FROM 'C:\\Users\\user\\git\\causal-rules\\database_generator\\cite.csv' DELIMITER ',' CSV HEADER;",
                  "\copy organization(oid,name)  FROM 'C:\\Users\\user\\git\\causal-rules\\database_generator\\organization.csv' DELIMITER ',' CSV HEADER;"
                  ]

# for iq in insert_queries:
#         cursor.execute(iq)
# conn.commit()
#
# cursor.close()
# conn.close()
import csv
def write_to_csv(fname, data):
    """write rows to CSV file"""
    with open(fname, 'w', newline='') as csvFile:
        writer = csv.writer(csvFile, delimiter=',')
        writer.writerows(data)
    csvFile.close()

def read_csv(fname):
    rows = []
    with open(fname, newline='') as csvFile:
        reader = csv.reader(csvFile, delimiter=',')
        for row in reader:
            if row not in rows:
                rows.append(row)
    csvFile.close()
    return rows

rows = read_csv("C:\\Users\\user\\git\\causal-rules\\database_generator\\cite.csv")
write_to_csv("C:\\Users\\user\\git\\causal-rules\\database_generator\\cite2.csv", rows)
