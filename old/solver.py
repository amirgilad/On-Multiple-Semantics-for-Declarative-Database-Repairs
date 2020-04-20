from z3 import *


def generate_minimal_model(prov):
    var_lst = []
    prov_all = "(and "
    for i in range(len(prov)):  #
        var_lst += prov[i].replace('(', '').replace(')', '').replace('and', '').replace('or ', ' ').split(' ')
        prov_all += prov[i] + " "
    prov_all = prov_all[:-1] + ")"
    var_lst = list(dict.fromkeys(var_lst))
    var_lst = [x for x in var_lst if x != '']

    vals, size = solve_boolean_formula_with_z3_smt2(prov_all, var_lst)
    return vals, size


def solve_boolean_formula_with_z3_smt2(bf, appeared_symbol_list):
    declaration_str = '\n'.join(list(map(lambda x: '(declare-const {} Bool)'.format(x), appeared_symbol_list)))
    declaration_str += '\n(declare-const s Int)'
    declaration_str += '\n(define-fun b2i ((x Bool)) Int (ite x 1 0))'

    size_str = '(+ {})'.format(' '.join(list(map(lambda x: '(b2i {})'.format(x), appeared_symbol_list))))
    assert_str = '(assert {})\n'.format(bf)
    assert_str += '(assert (= s {}))\n(assert (> s 0))'.format(size_str)

    z3_bf = parse_smt2_string(declaration_str + '\n' + assert_str)
    opt = Optimize()
    opt.add(z3_bf)
    s = Int('s')
    opt.minimize(s)

    if opt.check() == sat:
        best_model = opt.model()
        min_size = 0
        for cl in best_model:
            if isinstance(best_model[cl], BoolRef) and best_model[cl]:
                min_size += 1
        return best_model, min_size
    else:
        return None, -1