import re


class DependencyGraph():

    def __init__(self, rules):
        self.rules = rules
        self.rules_to_nodes = {rules[i]: self.Node(rules[i].head.lower()+str(i)) for i in range(len(rules))}
        self.nodes_to_rules = {self.Node(rules[i].head.lower()+str(i)): rules[i] for i in range(len(rules))}
        self.edges = {}
        self.get_edges()




    def get_edges(self):
        for rule in self.rules:
            v = self.rules_to_nodes[rule]

            terms = rule.body.lower().split(' ')
            deltas = ['_'.join(terms[i].split('_')[0:2]) for i in range(len(terms)) if 'delta' in terms[i] and '.' not in terms[i]]
            deltas = list(set(deltas))
            for delta in deltas:
                for rule2 in self.rules:
                    if rule2.head.lower() == delta.lower():
                        u = self.rules_to_nodes[rule2]
                        if u not in self.edges:
                            self.edges[u] = []
                        self.edges[u].append(v)
                        v.add_exit_node(u)
                        u.add_entry_node(v)
            print(self.edges)



    def remove_dependencies(self):
        # take a recursive approach. stopping condition when there are no incoming edges
        new_prog = []
        for scr in self.nodes:
            if scr.entry_deg == 0:
                for trgt in scr.exit_nodes:
                    # replace the appearance of scr in body(trgt) with body(scr)
                    print(trgt)




    class Node():

        def __init__(self, head):
            self.name = head
            self.entry_nodes = []
            self.exit_nodes = []

        def entry_deg(self):
            return len(self.entry_nodes)

        def exit_deg(self):
            return len(self.exit_nodes)

        def add_entry_node(self, n):
            self.entry_nodes.append(n)

        def add_exit_node(self, n):
            self.exit_nodes.append(n)

        def __repr__(self):
            return 'Node(' + self.name + ')'