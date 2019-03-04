from radb.ra_tracker import RATracker
import tempfile
import sys
import re
import json
# import pymssql
import psycopg2
import datetime
import logging
from tt import BooleanExpression, to_cnf
import pyparsing  # make sure you have this installed
from z3 import *

from query_rewriter import add_prov

logger = logging.getLogger('Runner')


def rewrite_query_prov(sql, smt2=False):
    lines = sql.split('\n')
    prov_sql_str = ''
    new_sql_str = ''
    for line in lines:
        line = line.strip()
        if line == '':
            continue
        new_line, prov_line = add_prov(line, smt2)
        if line[0:4] == 'WITH':
            prov_sql_str += prov_line
            new_sql_str += new_line
        elif line[0:6] == 'SELECT':
            prov_sql_str += '\n' + prov_line
            new_sql_str += '\n' + new_line
        else:
            prov_sql_str += ',\n' + prov_line
            new_sql_str += ',\n' + new_line
    return new_sql_str, prov_sql_str


class ra_prov_runner:

    def __init__(self, ra_query, db_raw, db_prov):
        self.ra_query = ra_query
        self.sql_query = None
        self.error_message = None
        self.cur_raw = db_raw
        self.cur_prov = db_prov
        self.time_ra_start = None
        self.time_ra_finish = None
        self.time_prov_start = None
        self.time_prov_finish = None

    def evaluate_sql(self):
        self.time_prov_start = datetime.datetime.now()
        q, q_prov = rewrite_query_prov(self.sql_query, smt2=True)
        # logger.warning(q_prov)
        self.cur_prov.execute(q_prov)
        res = self.cur_prov.fetchall()
        self.time_prov_finish = datetime.datetime.now()
        res_prov = list(map(lambda x: x[-1], res))
        return res_prov

    def evaluate_ra(self):
        ra_infile = tempfile.NamedTemporaryFile()
        # logger.warning(self.ra_query)
        ra_infile.write(self.ra_query.encode())
        ra_infile.seek(0)
        ra_outfile = tempfile.NamedTemporaryFile()
        # out_file = open('temp.txt', 'w+b')
        self.time_ra_start = datetime.datetime.now()
        try:
            RATracker(configfile='psql.ini', inputfile=ra_infile.name, outputfile=ra_outfile.name,
                      # RATracker(configfile='mssql.ini', inputfile=ra_infile.name, outputfile=ra_outfile.name,
                      debug=False, verbose=True)
        except Exception as e:
            self.error_message = e
            sys.stdout = sys.__stdout__
            ra_infile.close()
            ra_outfile.close()
            return
        self.time_ra_finish = datetime.datetime.now()
        sys.stdout = sys.__stdout__

        ra_outfile.seek(0)
        self.sql_query = ra_outfile.read().decode()
        # logger.warning(self.sql_query)
        if self.sql_query.startswith('ERROR:') or self.sql_query.find('ERROR:') != -1:
            # self.error_message = self.sql_query
            logger.error(self.sql_query)
            ra_infile.close()
            ra_outfile.close()
            return
        elif len(self.sql_query) == 0:
            logger.error('Empty query!')
            ra_infile.close()
            ra_outfile.close()
            return
        self.sql_query = self.sql_query[self.sql_query.rfind('WITH rat0'):]
        # logger.warning(self.sql_query)
        ra_outfile.seek(0)
        ra_infile.close()
        ra_outfile.close()

        return self.evaluate_sql()



def copy_prov_database(conn_raw, cur_raw, conn_prov, cur_prov):
    drop_queries = [
        'DROP TABLE IF EXISTS author;',
        'DROP TABLE IF EXISTS publication;',
        'DROP TABLE IF EXISTS writes;',
        'DROP TABLE IF EXISTS cite;',
        'DROP TABLE IF EXISTS organization;'
    ]

    create_queries_prov = [
        'CREATE TABLE author (aid int, name varchar(60), oid int, prov varchar);',
        'CREATE TABLE publication (pid int, title varchar(200), year int, prov varchar);',
        'CREATE TABLE writes (aid int, pid int, prov varchar);',
        'CREATE TABLE cite (citing int, cited int, prov varchar);',
        'CREATE TABLE organization (oid int, name varchar(150), prov varchar);'
    ]

    for dq in drop_queries:
        cur_prov.execute(dq)
    conn_prov.commit()
    for cq in create_queries_prov:
        cur_prov.execute(cq)
    conn_prov.commit()


    relation_list = ['author', 'publication', 'writes', 'cite', 'organization']

    for r in relation_list:

        tuple_prov_list = []
        select_query = 'SELECT * FROM {}'.format(r)
        cur_raw.execute(select_query)
        tuple_list = cur_raw.fetchall()
        cnt = 0
        for arr in tuple_list:
            tuple_prov_list.append(
                ','.join(map(lambda x: "'" + str(x).replace("'", "") + "'", arr)) + ",'{}'".format(r[0:2] + str(cnt)))
            cnt += 1

        delete_query = "DELETE FROM {};".format(r)
        cur_prov.execute(delete_query)
        conn_prov.commit()

        insert_query = 'INSERT INTO {} VALUES {}'
        n = 0
        while n < len(tuple_prov_list):
            insert_str = ','.join(map(lambda x: '(' + str(x) + ')', tuple_prov_list[n:n + 100]))
            n += 100

            cur_prov.execute(insert_query.format(r, insert_str))
            conn_prov.commit()


def parse_prov_tt(parsed, parens):
    res = ''
    is_last_layer = not any(isinstance(e, list) for e in parsed)
    if is_last_layer == True:
        if isinstance(parsed, str):
            return parsed
        else:
            return parsed[1] + ' ' + parsed[0] + ' ' + parsed[2]

    for i in range(1, len(parsed)):
        res += '(' + parse_prov_tt(parsed[i], parens) + ') ' + parsed[0] + ' '
    res = res[:-4]
    return res

def parse_all_prov_tt(prov):
    thecontent = pyparsing.Word(pyparsing.alphanums) | 'and' | 'or'
    parens = pyparsing.nestedExpr('(', ')', content=thecontent)
    res = ''
    for p in prov:
        parsed = parens.parseString(p).asList()
        res += '(' + parse_prov_tt(parsed[0], parens) + ')' + ' and '
    return res[:-4]


def parse_prov_z3(parsed, parens):
    res = ''
    is_last_layer = not any(isinstance(e, list) for e in parsed)
    if is_last_layer == True:
        if isinstance(parsed, str):
            return "d['"+parsed+"']"
        else:
            op = "And" if "and" in parsed[0] else "Or"
            return op + "(d['" + parsed[1] + "'], d['" + parsed[2] + "'])"

    res += "And" if "and" in parsed[0] else "Or"
    for i in range(1, len(parsed)):
        res += '(' + parse_prov_z3(parsed[i], parens)
    res = res[:-2]+ '), '
    return res

def parse_all_prov_z3(prov):
    thecontent = pyparsing.Word(pyparsing.alphanums) | 'and' | 'or'
    parens = pyparsing.nestedExpr('(', ')', content=thecontent)
    res = 'And('
    for p in prov:
        parsed = parens.parseString(p).asList()
        res += '(' + parse_prov_z3(parsed[0], parens) + '), '
    return res[:-2] + ')'


def find_min_assignment(formula):
    formula = 'not (' + formula + ')'
    b = BooleanExpression(formula)
    b = to_cnf(b)
    total_max = 0
    sol = None
    # Amir: in the future, add condition that all all True tuples have to be derivable
    for sat_solution in b.sat_all():
        cur_max = str(sat_solution).count('=0')
        if total_max < cur_max:
            sol = sat_solution
            total_max = cur_max
    print(sol)
    return sol


if __name__ == '__main__':
    logging.getLogger('Runner').setLevel(logging.INFO)
    # logging.getLogger('Runner').setLevel(logging.DEBUG)
    logger_handler = logging.StreamHandler(sys.stdout)
    logger_handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
    logger.addHandler(logger_handler)

    # // Find names and addresses of drinkers who like some beer served at The Edge.
    ra_query = '''
    \\project_{aid, name, oid} (
        author \\join_{oid = oid} (
            organization \\join
             \\select_{name = 'Tel Aviv University'} organization
        )
    );'''

    conn_raw = psycopg2.connect(dbname='dblp_plus', user='postgres', password="Amiris1",
                                host="127.0.0.1",
                                port="5432")
    conn_prov = psycopg2.connect(dbname='dblp_plus_prov', user='postgres', password="Amiris1",
                                 host="127.0.0.1",
                                 port="5432")
    cur_raw = conn_raw.cursor()
    cur_prov = conn_prov.cursor()

    copy_prov_database(conn_raw, cur_raw, conn_prov, cur_prov)

    rr = ra_prov_runner(ra_query, cur_raw, cur_prov)

    logger.info(rr.evaluate_ra())

    rr.sql_query = '''WITH rat0(a0, a1, a2) AS (SELECT * FROM author),
rat1(a0, a1) AS (SELECT * FROM organization),
rat2(a0, a1) AS (SELECT * FROM rat1 WHERE rat1.a1 = 'Tel Aviv University'),
rat3(a0, a1, a2) AS (SELECT rat0.a0, rat0.a1, rat0.a2 FROM rat0, rat2 WHERE rat0.a2 = rat2.a0)
SELECT * FROM rat3'''

    # logger.info(rr.evaluate_sql())

    prov = rr.evaluate_sql()
    print(prov)
    # res = parse_all_prov(prov)
    # b = to_cnf(res)
    # print(b)
    # find_min_assignment(res)

    # prov = ["""(or (and a b) (and c d))"""]
    # prov_parsed = parse_all_prov_z3(prov)

    # var_lst = []
    # for exp in prov:
    #     var_lst += exp.replace('(', '').replace(')', '').replace('and', '').replace('or', '').split(' ')
    # d = {x : Bool(x) for x in var_lst if x != ''}
    #
    print(prov[0])
    f = Z3_parse_smtlib2_string(0, '(= a b)', 0, 0, 0, 0, 0, 0)
    # f = prov_parsed
    print(f)



    # au8, or1 = Bools('au8 or1')
    # d = {au8: Bool('au8'), or1: Bool('or1')}
    # abc = """And(d[au8], d[or1]), """* 3
    # abc = """And(""" + abc[:-2] + """)"""
    # f = eval(abc)
    # print(f)
    # f = eval(prov_test[1:-1])

    # s = Solver()
    # s.add(Not(f))
    # print (s.check())
    # print (s.model())







