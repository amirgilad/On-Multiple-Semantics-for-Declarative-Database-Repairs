from z3 import *

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