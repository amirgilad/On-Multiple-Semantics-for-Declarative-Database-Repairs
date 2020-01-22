import csv

def get_changed_tuples(fixed, dirty):
    with open(fixed, 'r', newline='\n') as t1, open(dirty, 'r') as t2:
        reader_fixed = csv.reader(t1, delimiter=',', quotechar='|')
        reader_dirty = csv.reader(t2, delimiter=',', quotechar='|')

        fixed_lines = []
        for line in reader_fixed:
            fixed_lines.append(line)
        dirty_lines = []
        for line in reader_dirty:
            dirty_lines.append([val for val in line if val])

        for x in dirty_lines:
            if x not in fixed_lines:
                print(x)
        diff = [x for x in fixed_lines if x not in dirty_lines]
        print(len(diff))



# get_changed_tuples('..\\database_generator\\experiment_dbs\\fixed_by_holoclean\\hauthor_100_errors_fixed.csv', '..\\database_generator\\experiment_dbs\\hauthor_100_errors.csv')
# get_changed_tuples('hospital.csv', 'hospital_clean_fixed.csv')
# get_changed_tuples('fixed_data.csv', 'hospital_clean_fixed.csv')