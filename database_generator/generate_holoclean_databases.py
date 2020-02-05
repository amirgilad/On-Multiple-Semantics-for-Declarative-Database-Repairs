import csv
import random
import string
import sys
sys.path.append('..\\')

def add_errs(filename, max_rows, num_errors, dcs):
    orig_rows = []
    with open(filename, newline='\n', encoding="utf8") as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='|')
        cnt = 0
        for row in reader:
            new_row = [val.replace('"', '') for val in row]
            new_row[-1] = ''.join(random.choice(string.ascii_lowercase) for i in range(10)) if new_row[-1] == '' else new_row[-1]
            orig_rows.append(new_row)
            if cnt == max_rows:
                break
            cnt += 1

    num_dups = 5000 // row_num # the number of times each tuple is duplicated
    orig_rows_long = [[val for val in orig_rows[0]]]
    for i in range(num_dups):
        orig_rows_long.extend([[val for val in row] for row in orig_rows if "aid" not in row])
    error_rows = [[val for val in row] for row in orig_rows_long]

    # adding errors
    error_trcker = set()
    possible_idxs = [i for i in range(1, len(orig_rows_long)//num_dups + 1)]
    for i in range(num_errors):
        row_idx = random.choice(possible_idxs) # choose index in the first half of the table
        dc_idx = random.randrange(4)
        dc = dcs[dc_idx]
        cur_row = orig_rows_long[row_idx]
        error_trcker.add((cur_row[0], dc[1]))
        error_rows[row_idx][dc[1]] = '-'+str(random.randrange(400)) if dc[1] in [0, 2] else ''.join(random.choice(string.ascii_lowercase) for i in range(8))
        possible_idxs.remove(row_idx)
    actual_err_num = len(error_trcker)
    errornous_tuples_num = len(set([e[0] for e in error_trcker]))
    print("actual error number:", actual_err_num)
    # print("actual errornous tuples number (expected result for independent):", errornous_tuples_num)
    return error_rows, orig_rows_long, errornous_tuples_num




def add_dups(filename, frag_size, num_errors, num_dups, dcs):
    orig_rows = []
    with open(filename, newline='\n', encoding="utf8") as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='|')
        cnt = 0
        for row in reader:
            new_row = [val.replace('"', '') for val in row]
            new_row[-1] = ''.join(random.choice(string.ascii_lowercase) for i in range(10)) if new_row[-1] == '' else new_row[-1]
            orig_rows.append(new_row)
            if cnt == frag_size:
                break
            cnt += 1

    orig_rows_long = [[val for val in orig_rows[0]]]
    for i in range(num_dups):
        orig_rows_long.extend([[val for val in row] for row in orig_rows if "aid" not in row])
    error_rows = [[val for val in row] for row in orig_rows_long]

    # adding errors
    error_trcker = set()
    possible_idxs = [i for i in range(1, frag_size + 1)]
    for i in range(num_errors):
        row_idx = random.choice(possible_idxs) # choose index in the first half of the table
        dc_idx = random.randrange(4)
        dc = dcs[dc_idx]
        cur_row = orig_rows_long[row_idx]
        error_trcker.add((cur_row[0], dc[1]))
        error_rows[row_idx][dc[1]] = '-'+str(random.randrange(400)) if dc[1] in [0, 2] else ''.join(random.choice(string.ascii_lowercase) for i in range(8))
        possible_idxs.remove(row_idx)
    actual_err_num = len(error_trcker)
    errornous_tuples_num = len(set([e[0] for e in error_trcker]))
    print("actual error number:", actual_err_num)
    # print("actual errornous tuples number (expected result for independent):", errornous_tuples_num)
    return error_rows, orig_rows_long, errornous_tuples_num

def write_to_csv_errors(fname, error_num, data, encode = False):
    """write rows to CSV file"""
    if not encode:
        with open(fname+'_' + str(error_num) + "_errors_2.csv", 'w', newline='\n') as csvFile:
            writer = csv.writer(csvFile)
            writer.writerows(data[1:])
        csvFile.close()
    else:
        with open(fname+'_' + str(error_num) + "_errors_2.csv", 'w', newline='\n', encoding='utf-8') as csvFile:
            writer = csv.writer(csvFile)
            writer.writerows(data)
        csvFile.close()

def write_to_csv_rows(fname, dups, row_num, data, encode = False):
    """write rows to CSV file"""
    if not encode:
        with open(fname+'_' + str(dups * row_num) + "_rows.csv", 'w', newline='\n') as csvFile:
            writer = csv.writer(csvFile)
            writer.writerows(data[1:])
        csvFile.close()
    else:
        with open(fname+'_' + str(dups * row_num) + "_rows.csv", 'w', newline='\n', encoding='utf-8') as csvFile:
            writer = csv.writer(csvFile)
            writer.writerows(data)
        csvFile.close()


def convert_to_holoclean_analysis_format(path, filename):
    clean_rows = [['tid','attribute','correct_val']]
    attrs = []
    with open(path + "\\" + filename+'.csv', newline='\n', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='|')
        t_num = -1
        for row in reader:
            if t_num == -1:
                attrs = row
            else:
                for i in range(len(attrs)):
                    clean_rows.append([t_num, attrs[i], row[i]])
            t_num += 1
    with open(path + "\\" + filename+'_analysis.csv', 'w', newline='\n', encoding='utf-8') as csvfilew:
        writer = csv.writer(csvfilew, delimiter=',')
        for row in clean_rows:
            writer.writerow(row)


def convert_from_holoclean_analysis_format(path, filename):
    orig_rows = [[]]
    attrs = []
    with open(path + "\\" + filename+'.csv', newline='\n') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='|')
        cur_idx = 0
        for row in reader:
            if row[0] == 'tid':
                continue
            elif int(row[0]) == 0:
                attrs.append(row[1])
                orig_rows[-1].append(row[2])
            elif int(row[0]) == cur_idx:
                orig_rows[-1].append(row[2])
            elif int(row[0]) > cur_idx: # started new tuple
                cur_idx = int(row[0])
                orig_rows.append([])
                orig_rows[-1].append(row[2])
        orig_rows.insert(0, attrs)
        print(orig_rows)
        print(len(orig_rows))
    with open(path + "\\" + filename+'_orig_struct.csv', 'w', newline='\n') as csvfilew:
        writer = csv.writer(csvfilew, delimiter=',', encoding='utf-8')
        for row in orig_rows:
            writer.writerow(row)


# DCs:
# equal name, not equal oid
# equal aid, not equal org_name
# equal aid, not equal oid
# equal oid, not equal org_name
dcs = [[0, 1], [0, 2], [0, 3], [2, 3]] #
row_num = 1000
path = "..\\data\\holocomp\\"

# Create files with set number of rows and increasing errors
for error_num in [0]:#100, 200, 300, 500, 700,
    error_data, orig_data, errornous_tuples_num = add_errs(path + "author_2000_distinct.csv", row_num, error_num, dcs)
    write_to_csv_errors(path + 'hauthor', error_num, error_data)
    write_to_csv_errors(path + 'holoclean_hauthor', error_num, error_data, encode = True)
    write_to_csv_errors(path + 'holoclean_hauthor_clean', error_num, orig_data, encode = True)
    # write_to_csv_errors(path + 'hauthor_clean', error_num, orig_data, encode = False)

# Create files with set number of errors and increasing rows
# error_num = 700
# # dups_lst = [2, 3, 4, 5, 6, 7, 8]
# dups_lst = [10,20,30,40]
# for dups in dups_lst:
#     error_data, orig_data, errornous_tuples_num = add_dups(path + "author_2000_distinct.csv", row_num, error_num, dups, dcs)
#     # write_to_csv_rows(path + 'hauthor', dups, row_num, error_data)
#     write_to_csv_rows(path + 'holoclean_hauthor', dups, row_num, error_data, encode = True)
#     write_to_csv_rows(path + 'holoclean_hauthor_clean', dups, row_num, orig_data, encode = True)


# err = [100, 200, 300, 500, 700, 1000]
# for e in err:
#     convert_to_holoclean_analysis_format('..\\data\\holocomp','holoclean_hauthor_clean_' + str(e) +'_errors')

# rows = [2000, 3000, 4000, 5000, 6000, 7000, 8000]
# rows = [10000, 20000, 30000, 40000]
# for r in rows:
#     convert_to_holoclean_analysis_format('..\\data\\holocomp','holoclean_hauthor_clean_' + str(r) +'_rows')
