import MySQLdb
import csv



def fill_tables(org_names, conn, orig_conn):

    relation_list = ['organization', 'author', 'publication', 'writes', 'cite', 'cite']

    header_list = [['oid', 'name'], ['aid', 'name', 'oid'], ['pid', 'title', 'year'], ['aid', 'pid'], ['citing', 'cited'], ['citing', 'cited']]

    insert_queries = ["SELECT organization.oid, organization.name FROM organization WHERE organization.name like '%",

                      "select author.aid, author.name, author.oid from author, organization where " \
                      "author.oid = organization.oid and organization.name like '%",

                      "select publication.pid, publication.title, publication.year from publication, writes, author, organization where " \
                      "publication.pid = writes.pid and author.aid = writes.aid and " \
                      "author.oid = organization.oid and organization.name like '%",

                      "select writes.aid, writes.pid from writes, publication, author, organization where publication.pid = writes.pid and " \
                      "author.aid = writes.aid and author.oid = organization.oid and " \
                      "organization.name like '%",

                      "select cite.* from cite, writes, author, organization where cite.citing = writes.pid and author.aid = writes.aid and " \
                      "author.oid = organization.oid and organization.name like '%",

                      "select cite.* from cite, writes, author, organization where cite.cited = writes.pid and author.aid = writes.aid and " \
                      "author.oid = organization.oid and organization.name like '%"
                      ]

    for name in org_names:
        for i in range(len(relation_list)):
            select_query = insert_queries[i] + name + "%';"
            orig_conn.execute(select_query)
            tuple_list = orig_conn.fetchall()

            with open('experiment_dbs\\' + relation_list[i]+'.csv', 'w', newline='') as writeFile:
                writer = csv.writer(writeFile)
                writer.writerow(header_list[i])
                writer.writerows(tuple_list)
                writeFile.close()

            # delete_query = "DELETE FROM organization;"
            # conn.execute(delete_query)
            # conn.commit()

            # conn.insert_into_table(relation_list[i], tuple_list)


db = MySQLdb.connect("localhost","root","Amiris1","dblp_plus")
cursor = db.cursor()

fill_tables(['Tel Aviv'], cursor, cursor)

db.close()
