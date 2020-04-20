import sys
import json
import random
import argparse
import time
import os
import psycopg2
import re
# import sqlparse

rat_width = dict()

agg_func_list = ['max', 'min', 'count', 'sum', 'avg', 'string_agg']
operator_list = ['>', '<', '>=', '<=', '=', '<>']

def eliminate_agg_sub_query(sq):
    if sq['select_ids'] is None or sq['from_ids'] is None:
        print('Invalid sql statement!')
        return None

    select_attr_in_agg = []
    select_attr_not_in_agg = []
    for select_id in sq['select_ids']:
        if isinstance(select_id, str):
            continue
        in_agg = False
        for agg_func in agg_func_list:
            if (select_id.value[0:len(agg_func)].lower() == agg_func and
                    len(select_id.value) > len(agg_func) + 2 and
                    select_id.value[len(agg_func)] == '(' and select_id.value[-1] == ')'):
                select_attr_in_agg.append(select_id.value[len(agg_func) + 1:-1])
                in_agg = True
                break
        if not in_agg:
            select_attr_not_in_agg.append(select_id.value)
    from_relation_list = []
    for from_id in sq['from_ids']:
        from_relation_list.append(from_id.value)
    groupby_attr = []
    eliminate_agg_str = 'SELECT '
    eliminate_agg_prov_str = 'SELECT '
    need_group_by = False
    if sq['distinct']:
        eliminate_agg_str += 'DISTINCT '
        need_group_by = True
    if sq['groupby_ids'] is not None:
        for groupby_id in sq['groupby_ids']:
            if groupby_id.value not in select_attr_in_agg:
                groupby_attr.append(groupby_id.value)
        eliminate_agg_str += ','.join(groupby_attr + select_attr_in_agg)
        select_str = ','.join(groupby_attr + select_attr_in_agg)
    else:
        eliminate_agg_str += ','.join(select_attr_not_in_agg + select_attr_in_agg)
        select_str = ','.join(select_attr_not_in_agg + select_attr_in_agg)
    eliminate_agg_str += ' FROM ' + ','.join(from_relation_list)

    if len(from_relation_list) > 1:
        prov_str = "'&(' || {} || ')'".format(".prov || ',' || ".join(from_relation_list))
        from_str = ','.join(from_relation_list)
    else:
        prov_str = from_relation_list[0] + '.prov'
        from_str = from_relation_list[0]
    if sq['where_cl'] is not None:
        eliminate_agg_str += ' ' + sq['where_cl']
        where_str = sq['where_cl']

    return eliminate_agg_str


def add_prov(line, smt2=False):
    # print(line)
    if line[0:6] == 'SELECT':
        return line, line
    prov_line = ''
    if line[0:4] == 'WITH':
        prov_line = 'WITH '
    # prov_line += '('
    p_rat_as = re.compile(r'\brat\d+\(.*\) AS')
    rat_as_str = p_rat_as.findall(line)[0]
    p_rat = re.compile(r'rat\d+')
    rat_str = p_rat.findall(rat_as_str)[0]
    p_cols = re.compile(r"\((.*)\)")
    rat_cols_str = p_cols.findall(rat_as_str)[0]
    rat_cols = rat_cols_str.split(', ')
    rat_cols.append('a' + str(len(rat_cols)))
    rat_width[rat_str] = len(rat_cols)

    p_sub_query = re.compile(r'\((SELECT.*)\)')
    sub_query_str = p_sub_query.findall(line)[0]
    arr_except = sub_query_str.split(' EXCEPT ')

    p_select = re.compile(r'SELECT (.*) FROM')
    # p_from = re.compile(r' FROM (.*)(\bWHERE\b|\bGROUP\b|\bUNION\b|\bEXCEPT\b|\bINTERSECT\b)')
    p_from = re.compile(
        r' FROM (.*)(\sWHERE\s.*\sGROUP BY\s|\sWHERE\s|\sGROUP BY\s|\sUNION\s|\sEXCEPT\s|\sINTERSECT\s).*')
    p_from_single = re.compile(r' FROM (.*)$')
    p_where = re.compile(r' WHERE (.*)')
    p_groupby = re.compile(r' GROUP BY (.*)')
    p_having = re.compile(r' HAVING (.*)')

    if len(arr_except) > 1:
        # Handle EXCEPT

        select_str = [p_select.findall(arr_except[i])[0] for i in range(2)]

        from_str = []
        from_match_res = [p_from.findall(arr_except[i]) for i in range(2)]

        for i in range(2):
            if len(from_match_res[i]) == 0:
                from_match_res[i] = p_from_single.findall(arr_except[i])
            from_str.append(from_match_res[i][0])
            select_str[i] = select_str[i].replace('*', ', '.join(
                [from_str[i] + '.a' + str(j) for j in range(rat_width[from_str[i]] - 1)]))

        prov_where_str = ' and '.join(map(lambda x: '(' + '='.join(x) + ')',
                                          zip([from_str[0] + '.a' + str(j) for j in range(rat_width[from_str[0]] - 1)],
                                              [from_str[1] + '.a' + str(j) for j in
                                               range(rat_width[from_str[1]] - 1)])))

        group_by_str = select_str[0].replace('DISTINCT ', '')
        prov_line += rat_str + '({})'.format(', '.join(rat_cols)) + ' AS ('
        prov_line += '(SELECT ' + select_str[0]
        # prov_line += ", string_agg('And(' || {}.a{} || '), (Not(' || {}.a{} || '))', ' or ') ".format(
        if smt2:
            prov_line += ", '(and ' || {}.a{} || ' (not ' || {}.a{} || '))'".format(
                from_str[0], str(rat_width[from_str[0]] - 1),
                from_str[1], str(rat_width[from_str[1]] - 1))
        else:
            prov_line += ", '&(' || {}.a{} || ',~(' || {}.a{} || '))'".format(
                from_str[0], str(rat_width[from_str[0]] - 1),
                from_str[1], str(rat_width[from_str[1]] - 1))
        prov_line += ' FROM {} '.format(','.join(from_str))
        prov_line += 'WHERE {} '.format(prov_where_str)
        # prov_line += 'GROUP BY {}'.format(group_by_str)
        prov_line += ') UNION ('
        prov_line += 'SELECT ' + select_str[0]
        # prov_line += ", string_agg('('|| {}.a{} ||')', ' or ') ".format(from_str[0], str(rat_width[from_str[0]]-1))
        prov_line += ", {}.a{}".format(from_str[0], str(rat_width[from_str[0]] - 1))
        prov_line += ' FROM ' + from_str[0]
        # prov_line += ' WHERE ' + select_str[0] + ' NOT IN ('
        prov_line += ' WHERE NOT EXISTS ('
        prov_line += 'SELECT {} '.format(', '.join(['a' + str(j) for j in range(rat_width[from_str[1]] - 1)]))
        prov_line += 'FROM {} '.format(from_str[1])
        prov_line += 'WHERE {}) '.format(prov_where_str)
        # prov_line += 'GROUP BY {}'.format(group_by_str)
        prov_line += '))'

    else:
        arr_union = sub_query_str.split(' UNION ')
        if len(arr_union) > 1:
            # Handle UNION
            select_str = [p_select.findall(arr_union[i])[0] for i in range(2)]
            from_str = []
            from_match_res = [p_from.findall(arr_union[i]) for i in range(2)]

            for i in range(2):
                if len(from_match_res[i]) == 0:
                    from_match_res[i] = p_from_single.findall(arr_union[i])
                from_str.append(from_match_res[i][0])
                select_str[i] = select_str[i].replace('*', ', '.join(
                    [from_str[i] + '.a' + str(j) for j in range(rat_width[from_str[i]] - 1)]))

            prov_where_str = ' and '.join(map(lambda x: '(' + '='.join(x) + ')',
                                              zip([from_str[0] + '.a' + str(j) for j in
                                                   range(rat_width[from_str[0]] - 1)],
                                                  [from_str[1] + '.a' + str(j) for j in
                                                   range(rat_width[from_str[1]] - 1)])))

            group_by_str = [select_str[i].replace('DISTINCT ', '') for i in range(2)]
            prov_line += rat_str + '({})'.format(', '.join(rat_cols)) + ' AS ('
            prov_line += '(SELECT ' + select_str[0]
            # prov_line += ", string_agg('(' || {}.a{} || ') or (' || {}.a{} || ')', ' or ') ".format(
            if smt2:
                prov_line += ", '(or ' || {}.a{} || ' ' || {}.a{} || ')'".format(
                    from_str[0], str(rat_width[from_str[0]] - 1),
                    from_str[1], str(rat_width[from_str[1]] - 1))
            else:
                prov_line += ", '|(' || {}.a{} || ',' || {}.a{} || ')'".format(
                    from_str[0], str(rat_width[from_str[0]] - 1),
                    from_str[1], str(rat_width[from_str[1]] - 1))
            prov_line += ' FROM {} '.format(','.join(from_str))
            prov_line += 'WHERE {} '.format(prov_where_str)
            # prov_line += 'GROUP BY {}'.format(group_by_str[0])
            prov_line += ') UNION (('
            prov_line += 'SELECT ' + select_str[0]
            # prov_line += ", string_agg('('|| {}.a{} ||')', ' or ') ".format(from_str[0], str(rat_width[from_str[0]]-1))
            prov_line += ", {}.a{}".format(from_str[0], str(rat_width[from_str[0]] - 1))
            prov_line += ' FROM ' + from_str[0]
            # prov_line += ' WHERE ' + select_str[0] + ' NOT IN ('
            prov_line += ' WHERE NOT EXISTS ('
            prov_line += 'SELECT {} '.format(', '.join(['a' + str(j) for j in range(rat_width[from_str[1]] - 1)]))
            prov_line += 'FROM {} '.format(from_str[1])
            prov_line += 'WHERE {}) '.format(prov_where_str)
            # prov_line += 'GROUP BY {}'.format(group_by_str[0])
            prov_line += ')) UNION (('
            prov_line += 'SELECT ' + select_str[1]
            # prov_line += ", string_agg('('|| {}.a{} ||')', ' or ') ".format(from_str[1], str(rat_width[from_str[1]]-1))
            prov_line += ", {}.a{}".format(from_str[1], str(rat_width[from_str[1]] - 1))
            prov_line += ' FROM ' + from_str[1]
            # prov_line += ' WHERE ' + select_str[0] + ' NOT IN ('
            prov_line += ' WHERE NOT EXISTS ('
            prov_line += 'SELECT {} '.format(', '.join(['a' + str(j) for j in range(rat_width[from_str[0]] - 1)]))
            prov_line += 'FROM {} '.format(from_str[0])
            prov_line += 'WHERE {}) '.format(prov_where_str)
            # prov_line += 'GROUP BY {})))'.format(group_by_str[1])
            prov_line += ')))'
        else:
            arr_intersect = sub_query_str.split(' INTERSECT ')
            if len(arr_intersect) > 1:
                # Handle INTERSECT

                select_str = [p_select.findall(arr_intersect[i])[0] for i in range(2)]

                from_str = []
                from_match_res = [p_from.findall(arr_intersect[i]) for i in range(2)]

                for i in range(2):
                    if len(from_match_res[i]) == 0:
                        from_match_res[i] = p_from_single.findall(arr_intersect[i])
                    from_str.append(from_match_res[i][0])
                    select_str[i] = select_str[i].replace('*', ', '.join(
                        [from_str[i] + '.a' + str(j) for j in range(rat_width[from_str[i]] - 1)]))

                prov_where_str = ' and '.join(map(lambda x: '(' + '='.join(x) + ')',
                                                  zip([from_str[0] + '.a' + str(j) for j in
                                                       range(rat_width[from_str[0]] - 1)],
                                                      [from_str[1] + '.a' + str(j) for j in
                                                       range(rat_width[from_str[1]] - 1)])))

                # distinct_flag = select_str[0].lower().find('distinct ') != -1
                group_by_str = select_str[0].replace('DISTINCT ', '')
                prov_line += rat_str + '({})'.format(', '.join(rat_cols)) + ' AS ('
                prov_line += 'SELECT ' + select_str[0]
                # prov_line += ", string_agg('(' || {}.a{} || ') and (' || {}.a{} || ')', ' or ') ".format(
                if smt2:
                    prov_line += ", '(or ' || string_agg('(and ' || {}.a{} || ' ' || {}.a{} || ')', ' ') || ')' ".format(
                        from_str[0], str(rat_width[from_str[0]] - 1),
                        from_str[1], str(rat_width[from_str[1]] - 1))
                else:
                    prov_line += ", '|(' || string_agg('&(' || {}.a{} || ',' || {}.a{} || ')' , ', ') || ')' ".format(
                        from_str[0], str(rat_width[from_str[0]] - 1),
                        from_str[1], str(rat_width[from_str[1]] - 1))
                prov_line += 'FROM {} '.format(','.join(from_str))
                prov_line += 'WHERE {} '.format(prov_where_str)
                # if distinct_flag:
                prov_line += 'GROUP BY {})'.format(group_by_str)
            else:
                select_str = p_select.findall(sub_query_str)[0]

                from_match_res = p_from.findall(sub_query_str)
                if len(from_match_res) == 0:
                    from_match_res = p_from_single.findall(sub_query_str)
                from_str = from_match_res[0]
                if not isinstance(from_str, str):
                    from_str = from_str[0]
                where_arr = p_where.findall(sub_query_str)
                prov_where_str = ''

                if len(where_arr) > 0:
                    where_str = where_arr[0]
                    prov_where_str = where_str

                from_str = from_str.strip()
                from_rat_arr = p_rat.findall(from_str)
                if len(from_rat_arr) == 0:
                    prov_line += rat_str + '({})'.format(', '.join(rat_cols)) + ' AS ' + '(' + sub_query_str + ')'
                    if prov_where_str != '':
                        prov_line += 'WHERE {}'.format(prov_where_str)
                else:
                    from_arr = from_str.split(', ')
                    # print(387, from_arr)
                    if len(from_arr) == 1:
                        groupby_arr = p_groupby.findall(sub_query_str)
                        having_arr = p_having.findall(sub_query_str)
                        # print(groupby_arr, having_arr)
                        if len(groupby_arr) > 0:
                            # Handle AGG
                            groupby_str = groupby_arr[0]
                            groupby_str = groupby_str.split(' HAVING ')[0]
                            # print(select_str)
                            select_attrs = select_str.split(', ')
                            in_agg = None
                            agg = None
                            for a in agg_func_list:
                                if select_attrs[-1].strip().lower().startswith(a + '('):
                                    agg = a
                                    in_agg = select_attrs[-1][len(a) + 1:-1]
                                    break
                            # print(agg, in_agg)

                            # if len(having_arr) == 0:
                            #     # Eliminate group by when agg is the last operator
                            #     line = rat_str + '({})'.format(', '.join(rat_cols[:-1])) + ' AS ('
                            #     line += 'SELECT {}, {} '.format(groupby_str, in_agg)
                            #     line += 'FROM {} '.format(from_str)
                            #     if prov_where_str != '':
                            #         line += 'WHERE {}'.format(prov_where_str)
                            #     line += ')'
                            #     print(413, line)
                            #     nl, npl = add_prov(line, smt2)
                            #     return line, npl

                            # if len(having_arr) > 0:
                            #     rat_cols.append('a' + str(len(rat_cols)+1))
                            #     rat_width[rat_str] = len(rat_cols)
                            prov_line += rat_str + '({})'.format(', '.join(rat_cols)) + ' AS '
                            # prov_line += '(SELECT ' + select_str + ", string_agg('('|| {}.a{} ||')', ' or ')".format(from_arr[0], str(rat_width[from_arr[0]]-1))
                            if smt2:
                                if agg == 'sum':
                                    prov_line += '(SELECT ' + ', '.join(select_attrs[
                                                                        :-1]) + ", '(+ ' || string_agg('(* (b2i ' || {}.a{} || ') ' || CONVERT(varchar(10), {}) || ')', ' ') || ')' ".format(
                                        from_arr[0], str(rat_width[from_arr[0]] - 1), in_agg
                                    )
                                elif agg == 'count':
                                    prov_line += '(SELECT ' + ', '.join(select_attrs[
                                                                        :-1]) + ", '(+ ' || string_agg('(b2i ' || {}.a{} || ')', ' ') || ')' ".format(
                                        from_arr[0], str(rat_width[from_arr[0]] - 1)
                                    )
                            else:
                                if agg == 'sum':
                                    prov_line += '(SELECT ' + ', '.join(select_attrs[
                                                                        :-1]) + ", string_agg('@(' || {}.a{} || ')*' || CONVERT(varchar(10), {}), '+') ".format(
                                        from_arr[0], str(rat_width[from_arr[0]] - 1), in_agg
                                    )
                                elif agg == 'count':
                                    prov_line += '(SELECT ' + ', '.join(
                                        select_attrs[:-1]) + ", string_agg('@(' || {}.a{} || ')', '+') ".format(
                                        from_arr[0], str(rat_width[from_arr[0]] - 1)
                                    )
                            if len(having_arr) > 0:
                                # Handle HAVING
                                having_str = having_arr[0]
                                in_agg = None
                                agg = None
                                for a in agg_func_list:
                                    if having_str.strip().lower().startswith(a + '('):
                                        agg = a
                                        in_agg = select_attrs[-1][len(a) + 1:-1]
                                        break
                                op = None
                                operand = None
                                for o in operator_list:
                                    if having_str.find(' {} '.format(o)) > 0:
                                        op = o
                                        operand = having_str.split(' {} '.format(o))[1]
                                        break

                                if smt2:
                                    prov_line += ", '(and '(or ' || string_agg({}.a{}, ' ') || ')' ".format(
                                        from_arr[0], str(rat_width[from_arr[0]] - 1)
                                    )
                                    if agg == 'sum':
                                        # prov_line += ", '(+ ' || string_agg('(* (b2i ' || {}.a{} || ') ' || CONVERT(varchar(10), {}) || ')', ' ') || ')' ".format(
                                        prov_line += " ({} (+ ' || string_agg('(* (b2i ' || {}.a{} || ') ' || CONVERT(varchar(10), {}) || ')', ' ') || ' {}))' ".format(
                                            op, from_arr[0], str(rat_width[from_arr[0]] - 1), in_agg, operand
                                            # from_arr[0], str(rat_width[from_arr[0]]-1), in_agg
                                        )
                                    elif agg == 'count':
                                        # prov_line += ", '(+ ' || string_agg('(b2i ' || {}.a{} || ')', ' ') || ')' ".format(
                                        prov_line += " (+ ' || string_agg('(b2i ' || {}.a{} || ')', ' ') || '))' ".format(
                                            op, from_arr[0], str(rat_width[from_arr[0]] - 1), operand
                                            # from_arr[0], str(rat_width[from_arr[0]]-1)
                                        )
                                else:
                                    prov_line += ", '&(' || '|(' || string_agg({}.a{}, ',') || '),' ".format(
                                        from_arr[0], str(rat_width[from_arr[0]] - 1)
                                    )

                                    if agg == 'sum':
                                        # prov_line += ", '(' || string_agg('@(' || {}.a{} || ')*' || CONVERT(varchar(10), {}), '+') || ')' ".format(
                                        prov_line += "|| string_agg('@(' || {}.a{} || ')*' || CONVERT(varchar(10), {}), '+') ".format(
                                            from_arr[0], str(rat_width[from_arr[0]] - 1), in_agg
                                        )
                                    elif agg == 'count':
                                        # prov_line += ", '(' || string_agg('@(' || {}.a{} || ')', '+') || ')' ".format(
                                        prov_line += "|| string_agg('@(' || {}.a{} || ')', '+') ".format(
                                            from_arr[0], str(rat_width[from_arr[0]] - 1)
                                        )

                                    if operand is not None:
                                        prov_line += " || ' {} {})' ".format(op, operand)
                            else:
                                if smt2:
                                    prov_line += ", '(or ' || string_agg({}.a{}, ' ') || ')' ".format(
                                        from_arr[0], str(rat_width[from_arr[0]] - 1)
                                    )
                                else:
                                    prov_line += ", '|(' || string_agg({}.a{}, ',') || ')' ".format(
                                        from_arr[0], str(rat_width[from_arr[0]] - 1)
                                    )

                            prov_line += 'FROM {} '.format(from_str)
                            if prov_where_str != '':
                                prov_line += 'WHERE {} '.format(prov_where_str)
                            prov_line += 'GROUP BY {})'.format(groupby_str)
                        else:
                            select_str = select_str.replace('*', ', '.join(
                                ['a' + str(j) for j in range(rat_width[from_arr[0]] - 1)]))
                            # rat1(a0, a1) AS (SELECT DISTINCT rat0.a0, string_agg('('|| rat0.a3 ||')', ' or ') FROM rat0 GROUP BY rat0.a0),
                            prov_line += rat_str + '({})'.format(', '.join(rat_cols)) + ' AS '
                            # prov_line += '(SELECT ' + select_str + ", string_agg('('|| {}.a{} ||')', ' or ')".format(from_arr[0], str(rat_width[from_arr[0]]-1))
                            # prov_line += '(SELECT ' + select_str + ", '|(' || string_agg({}.a{}, ',') || ')' ".format(from_arr[0], str(rat_width[from_arr[0]]-1))
                            distinct_flag = select_str.lower().find('distinct ') != -1
                            group_by_str = select_str.replace('DISTINCT ', '')
                            if smt2:
                                if distinct_flag:
                                    prov_line += '(SELECT ' + select_str + ", '(or ' || string_agg({}.a{}, ' ') || ')' ".format(
                                        from_arr[0], str(rat_width[from_arr[0]] - 1))
                                else:
                                    prov_line += '(SELECT ' + select_str + ", {}.a{} ".format(from_arr[0], str(
                                        rat_width[from_arr[0]] - 1))
                            else:
                                if distinct_flag:
                                    prov_line += '(SELECT ' + select_str + ", '|(' || string_agg({}.a{}, ',') || ')' ".format(
                                        from_arr[0], str(rat_width[from_arr[0]] - 1))
                                else:
                                    prov_line += '(SELECT ' + select_str + ", {}.a{} ".format(from_arr[0], str(
                                        rat_width[from_arr[0]] - 1))
                            prov_line += 'FROM {} '.format(from_str)

                            # if prov_where_str != '':
                            if len(prov_where_str) > 1:
                                prov_line += 'WHERE {} '.format(prov_where_str)
                            if distinct_flag:
                                prov_line += 'GROUP BY {})'.format(group_by_str)
                            else:
                                prov_line += ')'
                    elif len(from_arr) == 2:
                        # Handle JOIN
                        select_str = select_str.replace('*', ', '.join(
                            map(lambda x: ', '.join([x + '.a' + str(j) for j in range(rat_width[x] - 1)]), from_arr)))
                        prov_line += rat_str + '({})'.format(', '.join(rat_cols)) + ' AS '
                        prov_line += '(SELECT ' + select_str
                        # prov_line += ", string_agg('('|| {}.a{} || ') and (' || {}.a{} || ')', ' or ') ".format(

                        # print(select_str)
                        # print(from_str)
                        distinct_flag = select_str.lower().find('distinct ') != -1
                        group_by_str = select_str.replace('DISTINCT ', '')
                        if smt2:
                            if distinct_flag:
                                prov_line += ", '(or ' || string_agg('(and '|| {}.a{} || ' ' || {}.a{} || ')', ' ') || ')' ".format(
                                    from_arr[0], str(rat_width[from_arr[0]] - 1),
                                    from_arr[1], str(rat_width[from_arr[1]] - 1)
                                )
                            else:
                                prov_line += ", '(and ' || {}.a{} || ' ' || {}.a{} || ')' ".format(
                                    from_arr[0], str(rat_width[from_arr[0]] - 1),
                                    from_arr[1], str(rat_width[from_arr[1]] - 1)
                                )
                        else:
                            if distinct_flag:
                                prov_line += ", '|(' || string_agg('&('|| {}.a{} || ',' || {}.a{} || ')', ',') || ')' ".format(
                                    from_arr[0], str(rat_width[from_arr[0]] - 1),
                                    from_arr[1], str(rat_width[from_arr[1]] - 1)
                                )
                            else:
                                prov_line += ", '&(' || {}.a{} || ',' || {}.a{} || ')' ".format(
                                    from_arr[0], str(rat_width[from_arr[0]] - 1),
                                    from_arr[1], str(rat_width[from_arr[1]] - 1)
                                )
                        prov_line += ' FROM {} '.format(from_str)
                        if prov_where_str != '':
                            prov_line += 'WHERE {} '.format(prov_where_str)
                        if distinct_flag:
                            prov_line += 'GROUP BY {})'.format(group_by_str)
                        else:
                            prov_line += ')'
                    else:
                        print("More than two tables in one subquery!")

    # print(prov_line)
    return line, prov_line


def rewrite_query_from_file(infile, use_smt2 = False):
    line = infile.readline()
    cnt = 0
    sql_str = ''
    prov_sql_str = ''
    while line:
        if len(line) < 2:
            line = infile.readline()
            continue
        # sql_str += line
        # print(line)
        if line[0:4] == 'WITH':
            new_line, prov_line = add_prov(line, use_smt2)
            sql_str = line
            prov_sql_str = prov_line
            line = infile.readline()
            while line:
                new_line, prov_line = add_prov(line, use_smt2)
                if line[0:6] == 'SELECT':
                    sql_str += '      ' + line
                    prov_sql_str += '\n' + prov_line
                    # print(sql_str)
                    # print(prov_sql_str)
                    # # cur_prov.execute(prov_sql_str)
                    # res = cur_prov.fetchall()
                    # print(res)
                    # print(json.dumps(res, indent=2))
                    # with open(output_path_str, 'w') as f:
                    #     f.write(json.dumps(cur.fetchall(), indent=2))
                    # outfile.write(sql_str.replace('\n', ' ') + '\n')
                    # outfile.write(prov_sql_str.replace('\n', '      ') + '\n')
                    cnt += 1
                    break
                else:
                    prov_sql_str += ',\n'
                    sql_str += line
                    prov_sql_str += prov_line
                line = infile.readline()
        # if line[-2] == ';':
        #     print(sql_str)
        #     sql_str_anti_agg = eliminate_agg(sql_str)
        #     outfile1.write(sql_str + '\n')
        #     outfile1.write(sql_str_anti_agg + '\n')
        #     sql_str = ''
        #     prov_sql_str = add_prov(sql_str_anti_agg)

        line = infile.readline()
    return sql_str, prov_sql_str

def main(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('--inputquerypath', '-i', type=str,
                        help='input query path')
    parser.add_argument('--outputpath', '-o', type=str,
                        help='outputpath')
    parser.add_argument('--database', '-d', type=str,
                        help='database to test')
    parser.add_argument('--usesmt2', '-smt2', action='store_true',
                        help='store provenance in smt2 format')

    args = parser.parse_args()

    if args.database is not None:
        db_name = args.database
    else:
        db_name = 'beers'
    use_smt2 = args.usesmt2

    try:
        conn = psycopg2.connect("host=localhost port=5432 dbname={}".format(db_name))
        cur = conn.cursor()
        conn_prov = psycopg2.connect("host=localhost port=5432 dbname={}_prov".format(db_name))
        cur_prov = conn_prov.cursor()
    except psycopg2.OperationalError:
        print('Fail to connect to database {}'.format(db_name))

    start_time = time.time()

    # directory = os.fsencode(directory_str)

    # input_path_str = './teacher-queries-agg.txt'
    if args.inputquerypath:
        input_path_str = args.inputquerypath
    else:
        input_path_str = './teacher-queries-agg-ra.txt'
    if args.outputpath:
        output_path_str = args.outputpath
    else:
        output_path_str = './teacher-queries-agg-ra-prov.txt'
    # output_path_str1 = './teacher-queries-agg-prov1.txt'
    # output_path_str2 = './teacher-queries-agg-prov2.txt'

    infile = open(input_path_str, 'r')
    line = infile.readline()
    cnt = 0
    outfile = open(output_path_str, 'w')
    # outfile1 = open(output_path_str1, 'w')
    # outfile2 = open(output_path_str2, 'w')
    sql_str = ''
    while line:
        if len(line) < 2:
            line = infile.readline()
            continue
        # sql_str += line
        # print(line)
        if line[0:4] == 'WITH':
            new_line, prov_line = add_prov(line, use_smt2)
            sql_str = line
            prov_sql_str = prov_line
            line = infile.readline()
            while line:
                new_line, prov_line = add_prov(line, use_smt2)
                if line[0:6] == 'SELECT':
                    sql_str += '      ' + line
                    prov_sql_str += '\n' + prov_line
                    print(sql_str)
                    print(prov_sql_str)
                    # cur_prov.execute(prov_sql_str)
                    # res = cur_prov.fetchall()
                    # print(res)
                    # print(json.dumps(res, indent=2))
                    # with open(output_path_str, 'w') as f:
                    #     f.write(json.dumps(cur.fetchall(), indent=2))
                    outfile.write(sql_str.replace('\n', ' ') + '\n')
                    outfile.write(prov_sql_str.replace('\n', '      ') + '\n')
                    cnt += 1
                    break
                else:
                    prov_sql_str += ',\n'
                    sql_str += line
                    prov_sql_str += prov_line
                line = infile.readline()
        # if line[-2] == ';':
        #     print(sql_str)
        #     sql_str_anti_agg = eliminate_agg(sql_str)
        #     outfile1.write(sql_str + '\n')
        #     outfile1.write(sql_str_anti_agg + '\n')
        #     sql_str = ''
        #     prov_sql_str = add_prov(sql_str_anti_agg)

        line = infile.readline()
    outfile.close()
    # outfile1.close()
    # outfile2.close()
    # break

    end_time = time.time()

    print("Elapsed time: ", end_time - start_time)

    conn.close()
    conn_prov.close()


if __name__ == '__main__':
    main(sys.argv)


