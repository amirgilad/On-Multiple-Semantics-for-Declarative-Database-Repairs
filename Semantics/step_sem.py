from Semantics.abs_sem import *
import networkx as nx
import time


class StepSemantics(AbsSemantics):
    """This class implements step semantics. This is the semantics which derives one tuple, t, at each state and
    updates the database immediately so that D = D \cup {\Delta(t)} \setminus {t}"""

    def __init__(self, db_conn, rules, tbl_names):
        super(StepSemantics, self).__init__(db_conn, rules, tbl_names)

        self.prov_graph = nx.DiGraph()   # the provenance graph

    def find_mss(self, schema):
        """implementation of end semantics where updates
        to the rules are at the end of the evaluation"""

        if not self.rules:   # verify more than 0 rules
            return set()

        # convert the rules so they will store the provenance
        prov_rules, prov_tbls, proj = self.gen_prov_rules()

        # var to store the assignments
        assignments = []

        # use end semantics to derive all delta tuples and store the provenance
        changed = True
        derived_tuples = set()
        prev_len = 0
        while changed:
            for i in range(len(self.rules)):
                cur_rows = self.db.execute_query(prov_rules[i][1])
                cur_assignments = self.rows_to_prov(cur_rows, prov_tbls[i], schema, proj, prov_rules[i])
                for assignment in cur_assignments:
                    if assignment not in assignments:
                        assignments.append(assignment)
                        derived_tuples.add(assignment[0])
                        self.delta_tuples[self.rules[i][0]].add(assignment[0][1])
            if prev_len == len(derived_tuples):
                changed = False
            prev_len = len(derived_tuples)
            for i in range(len(self.rules)):
                self.db.delta_update(self.rules[i][0], self.delta_tuples[self.rules[i][0]])   # update delta table in db

        # process provenance into a graph
        self.gen_prov_graph(assignments)
        self.compute_benefits()

        # the "heart" of the algorithm. Traverse the prov. graph by layer
        # and greedily find for each layer the nodes whose derivation will
        # be most beneficial to stabilizing the database
        mss = self.traverse_by_layer()
        return mss

    def gen_prov_rules(self):
        """convert every rule to a rule that outputs the provenance"""
        prov_rules = []
        prov_tbls = []
        proj = []
        for i in range(len(self.rules)):
            query = self.rules[i]
            q_parts = query[1].lower().split("from")
            proj = q_parts[0].split('select')[1].strip().split(',')
            proj = [e.strip() for e in proj]
            rest = q_parts[1].split("where")
            prov_tbls.append([tbl.strip() for tbl in rest[0].strip().split(',')])
            prov_proj = ""
            prov_lst = []
            for tbl in prov_tbls[i]:
                if "as" in tbl:
                    prov_proj += tbl.split("as")[1] + ".*, "
                    prov_lst.append(tbl.split("as")[0][:-1].strip())
                else:
                    prov_proj += tbl + ".*, "
            prov_proj = prov_proj[:-2]
            q_prov = "SELECT " + prov_proj + " FROM" + rest[0] + "WHERE" + rest[1]
            prov_rules.append((query[0], q_prov))
        return prov_rules, prov_tbls, proj

    def handle_assignment(self, row, assignment_tuples, schema, prov_tbls, rule):
        """convert a row from the result set into an assignment of tuples"""
        s = 0
        str_row = [str(e) for e in row]
        ans = ("", "")
        for tbl in prov_tbls:
            e = len(schema[tbl]) + s if 'delta_' not in tbl else len(schema[tbl[6:]]) + s
            attrs = [t if "\r" not in t else t[:-4] for t in str_row[s:e]]
            attrs = tuple([int(a) if a.isnumeric() else a for a in attrs])
            assignment_tuples.append((tbl, attrs))
            if rule[0] == tbl:
                ans = ("delta_" + tbl, attrs)
            s = e
        return assignment_tuples, ans

    def rows_to_prov(self, res, prov_tbls, schema, proj, rule):
        """separate every result row into provenance tuples"""
        proj_attrs = []
        for p in proj:
            t = tuple(p.split("."))
            proj_attrs.append(t)
        assignments = []
        for i in range(len(res)):
            example_tuples = []
            row = res[i]
            example_tuples, ans = self.handle_assignment(row, example_tuples, schema, prov_tbls, rule)
            example_tuples = [ans] + example_tuples
            assignments.append(example_tuples)
        return assignments

    def gen_prov_graph(self, assignments):
        """build the provenance graph from the assignments"""
        for assign in assignments:
            # add edges from deriving tuples to the result tuple
            self.prov_graph.add_edges_from([(t, assign[0]) for t in assign[1:]])

    def compute_benefits(self):
        """compute the benefits of all nodes in the prov. graph"""
        for n in self.prov_graph.nodes():
            if "delta_" not in n[0]:
                c_n = self.prov_graph.out_degree(n)
                d_n = self.prov_graph.out_degree(("delta_" + n[0], n[1])) if ("delta_" + n[0], n[1]) in self.prov_graph.nodes else 100000
                b_n = c_n - d_n
                self.prov_graph.node[n]["benefit"] = b_n

    def traverse_by_layer(self):
        """traverse the graph by layers, compute the max benefit vertex for each layer,
        and greedily remove this vertex and all of its assignments it takes part in from the graph"""
        mss = set()   # the MSS according to the heuristic algorithm
        layers = self.divide_into_layers()   # divide the prov. DAG into layers
        for ly in layers[1:]:
            deltas_in_layer = [n for n in ly if "delta_" in n[0]]
            arg_max = None
            cpg = None
            while len(deltas_in_layer) != len(mss):
                if arg_max is not None:
                    mss.add(arg_max)
                cpg = self.gen_updated_graph(cpg, arg_max)   # copy the provenance graph without the nodes removed
                max_b = -1000001
                for tup in ly:
                    orig_tup = (tup[0][6:], tup[1])
                    if tup in cpg.nodes() and orig_tup not in mss and cpg.node[orig_tup]["benefit"] > max_b:
                        max_b = cpg.node[(tup[0][6:], tup[1])]["benefit"]
                        arg_max = (tup[0][6:], tup[1])

                deltas_in_layer = [x for x in deltas_in_layer if x in cpg.nodes()]
                # print(len(deltas_in_layer), len(mss))
            mss.add(arg_max)
        return mss

    def gen_updated_graph(self, previous_graph, arg_max):
        """"takes the previous prov. graph and the node just chosen for the MSS, arg_max,
        and creates a new graph without all the nodes connected to arg_max except \Delta(arg_max)"""
        if arg_max is None:
            return nx.DiGraph(self.prov_graph)
        if previous_graph is None:
            previous_graph = self.prov_graph
        copy_prov_graph = nx.DiGraph(previous_graph)
        for e in previous_graph.edges():
            # check that both vertices in e are successors of arg_max and are not \Delta(arg_max)
            is_legal_successor_u = e[0] in previous_graph.successors(arg_max) or (e[0] == arg_max and
                                        e[0] not in previous_graph.successors(("delta_" + arg_max[0], arg_max[1])))
            is_legal_successor_v = e[1] in previous_graph.successors(arg_max) and e[1] != ("delta_" + arg_max[0], arg_max[1])
            if is_legal_successor_u and is_legal_successor_v:
                copy_prov_graph.remove_edge(e[0], e[1])
                copy_prov_graph.remove_node(e[1])
                if e[0] != arg_max:
                    copy_prov_graph.remove_node(e[1])

        copy_prov_graph.remove_nodes_from(nx.isolates(copy_prov_graph))   # remove isolated nodes from the graph
        return copy_prov_graph

    def divide_into_layers(self):
        """takes the provenance graph and divides it into layers for the algorithm to traverse"""
        layers = [[] for i in range(len([x for x in self.prov_graph.nodes() if "delta_" in x[0]])+1)]
        topo_sort = list(nx.topological_sort(self.prov_graph))
        top_layer = 0
        for n in topo_sort:
            if "delta_" not in n[0]:
                layers[0].append(n)
            else:
                preds = self.prov_graph.predecessors(n)
                idx = max([layers.index(l) for p in preds for l in layers if p in l])
                layers[idx+1].append(n)
                top_layer = idx + 1
        return layers[:top_layer + 1]
