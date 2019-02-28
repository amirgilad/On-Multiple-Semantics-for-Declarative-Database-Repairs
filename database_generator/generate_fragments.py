from database_generator.dba import DatabaseEngine

def generate(orgs_names):
    orig_conn = DatabaseEngine("dblp_plus")
    conn = DatabaseEngine("dblp_plus_fragment")
    create_tables(conn)
    fill_tables(orgs_names, conn, orig_conn)



def create_tables(conn):

    create_queries_prov = [
        'CREATE TABLE author (aid int(11), name varchar(60), oid int(11), papar_count int(11), citation_count int(11));',
        'CREATE TABLE publication (wid int(11), cid int(11), title varchar(100), citeid int(11), refid int(11));',
        'CREATE TABLE writes (aid int(11), wid int(11));',
        'CREATE TABLE cite (citeid int(11), refid int(11));',
        'CREATE TABLE organization (oid int(11), name varchar(100));'
        ]
    for cq in create_queries_prov:
        conn.execute_query(cq)
    conn.close_connection()



def fill_tables(org_names, conn, orig_conn):

    relation_list = ['organization', 'author', 'publication', 'writes', 'cite', 'cite']

    insert_queries = ["SELECT * FROM organization WHERE' + 'organization.name like '%",

                      "select author. * from author, organization where " \
                      "author.oid = organization.oid and organization.name like'%",

                      "select publication. * from publication, writes, author, organization where " \
                      "publication.pid = writes.pid and author.aid = writes.aid and " \
                      "author.oid = organization.oid and organization.name like '%",

                      "select * from writes where publication.pid = writes.pid and " \
                      "author.aid = writes.aid and author.oid = organization.oid and " \
                      "organization.name like '%",

                      "select * from cite where cite.citing = writes.pid and author.aid = writes.aid and " \
                      "author.oid = organization.oid and organization.name like '%",

                      "select * from cite where cite.cited = writes.pid and author.aid = writes.aid and " \
                      "author.oid = organization.oid and organization.name like '%"
                      ]


    for name in org_names:
        for i in range(len(relation_list)):
            select_query = insert_queries[i] + name + "%';"
            orig_conn.execute(select_query)
            tuple_list = orig_conn.fetchall()

            delete_query = "DELETE FROM organization;"
            conn.execute(delete_query)
            conn.commit()

            conn.insert_into_table(relation_list[i], tuple_list)
