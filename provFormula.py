# from tt import apply_idempotent_law, picosat
import itertools


def makeDNF(prov_map):
    dnf = ''
    for key in prov_map:
        dnf = dnf + ' and '
        concat = list(itertools.chain.from_iterable(prov_map[key]))
        conj = '(' + ' and '.join(concat) + ')'
        conj = conj.replace(':', '')
        conj = conj.replace('-', '')
        dnf = dnf + conj
    dnf = dnf[5:]
    return dnf
    # return minimize(dnf)


def minimize(dnf):
    res_dnf = dnf + ' '
    prev_dnf = dnf
    while res_dnf != prev_dnf:
        temp_dnf = res_dnf
        # res_dnf = apply_idempotent_law(prev_dnf)
        prev_dnf = temp_dnf
    return res_dnf


def make_lit_map(prov_map):
    lit_map = {}
    i = 1
    for key in prov_map:
        for lst in prov_map[key]:
            for elem in lst:
                if elem not in lit_map:
                    lit_map[elem] = i
                    i += 1
    rev_map = {v : k for k, v in lit_map.items()}
    return lit_map, rev_map



def solve(prov_map):
    lit_map, rev_map = make_lit_map(prov_map)
    formula = []
    for key in prov_map:
        for lst in prov_map[key]:
            formula.append([lit_map[e] for e in lst])
    # for solution in picosat.sat_all(formula):
        # sol = [rev_map[num] if num > 0 else '-'+rev_map[-num] for num in solution]
        # sol = [rev_map[num] for num in solution if num > 0]
        # print(sol)