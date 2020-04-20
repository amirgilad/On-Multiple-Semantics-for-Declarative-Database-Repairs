import itertools


def extract_prov(db_conn):
    query = "SELECT table_name FROM information_schema.tables WHERE  table_type='BASE TABLE';"
    tbls = db_conn.execute_query(query)
    tbls = [tbls[i][0] for i in range(len(tbls)) if 'delta' in tbls[i][0]]
    prov = []
    for tbl in tbls:
        res = db_conn.execute_query('select * from ' + tbl)
        res = [tuple([tbl +'_' + str(res[i][0])] + list(res[i][1:])) for i in range(len(res))]
        prov.extend(res)

    return prov


def make_prov_map(table_rows):
    prov_map = {}
    for row in table_rows:
        # tup = eval(row)
        pre = process(row)
        prov = get_prov(pre)
        if row[:-1] not in prov_map:
            prov_map[row[:-1]] = prov
        else:
            prov_map[row[:-1]].extend(prov)
        # remove duplicate provenance monomials
        prov_map[row[:-1]].sort()
        prov_map[row[:-1]] = list(i for i, _ in itertools.groupby(prov_map[row[:-1]]))
    return prov_map



def process(tup):
    # tup = eval(str_tup)
    prov_lsts = tup[-1].split(',')
    prov_monoms = []
    for m in prov_lsts:
        prov = m.strip("[]{}")
        prov_monoms.append(prov)
    return prov_monoms


def get_prov(prov_monoms):
    dnf_prov = []
    for monom1 in prov_monoms:
        m1 = monom1.split(';')
        m1 = [t.rsplit(':',1)[0] for t in m1 if len(t) > 0]
        flag = True
        for monom2 in prov_monoms:
            m2 = monom2.split(';')
            m2 = [t.rsplit(':',1)[0] for t in m2 if len(t) > 0]

            if sublist(m1, m2):
                flag = False
                break
        if flag:
            dnf_prov.append(m1)
    return dnf_prov


def sublist(lst1, lst2):
    ls1 = [element for element in lst1 if element in lst2]
    ls2 = [element for element in lst2 if element in lst1]
    is_equal = sorted(lst1) != sorted(lst2) and len(lst1) < len(lst2)
    return ls1 == ls2 and is_equal


# p = process("(2, 'iris', 'gilad', '{[r:2f9c5b36-e122-41a4-bcb3-f10a5b084f7b:1],[r:2f9c5b36-e122-41a4-bcb3-f10a5b084f7b:2],[p:83d4cf1b-7bf3-463a-94bc-16c6305db423:3;r:2f9c5b36-e122-41a4-bcb3-f10a5b084f7b:3],[]}', '51cc0056-dc43-5a3e-b81c-15ee4e43dfa6')")
# prov = get_prov(p)
# print(prov)

