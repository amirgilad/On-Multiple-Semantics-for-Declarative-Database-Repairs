import re
from collections import deque


class DependencyGraph():

    def __init__(self, rules):
        self.rules = rules
        self.rules_to_nodes = {rules[i]: self.Node(rules[i].head.lower()+str(i)) for i in range(len(rules))}
        # self.nodes_to_rules = {self.Node(rules[i].head.lower()+str(i)): rules[i] for i in range(len(rules))}
        self.nodes_to_rules = {v: k for k, v in self.rules_to_nodes.items()}
        # self.edges = {}
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
                        # if u not in self.edges:
                            # self.edges[u] = []
                        # self.edges[u].append(v)
                        v.add_incoming_node(u)
                        u.add_outgoing_node(v)
            # print(self.edges)



    def remove_dependencies(self):
        # take a recursive approach. stopping condition when there are no incoming edges
        new_prog = []
        topo = self.topo_sort()
        cur_node = None
        for scr in topo:
            cur_node = scr
            for trgt in cur_node.outgoing_nodes:
                if trgt.is_removed_deps == False:
                    # replace the appearance of scr in body(trgt) with body(scr)
                    self.replace_in_body(cur_node, trgt)
                    trgt.removed_deps()

                    # print(self.nodes_to_rules[trgt].body)


    def replace_in_body(self, scr, trgt):
        '''replace all occurances of delta in body of trgt with body of scr'''
        scr_body = self.nodes_to_rules[scr].body
        trgt_body = self.nodes_to_rules[trgt].body
        print(self.nodes_to_rules[trgt].body)
        # scr_before_from, scr_after_from = scr_body.lower().split("from",1)[0], scr_body.lower().split("from",1)[1]
        # trgt_before_from, trgt_after_from = trgt_body.lower().split("from", 1)[0], trgt_body.lower().split("from", 1)[1]
        # print('scr before from: ' + scr_before_from)
        # print('scr after from: ' + scr_after_from)
        # print('trgt before from: ' + trgt_before_from)
        # print('trgt after from: ' + trgt_after_from)

        print('before: ' + self.nodes_to_rules[trgt].body)
        print('name to replace: ' + scr.name[:-1])
        self.nodes_to_rules[trgt].body = self.nodes_to_rules[trgt].body.lower().replace(scr.name[:-1], scr.name[:-1].split('_')[1])
        print('after: ' + self.nodes_to_rules[trgt].body)


    def topo_sort(self):
        '''Linear time BFS for topological sort'''
        q = deque([v for v in self.nodes_to_rules.keys() if v.incoming_deg() == 0])
        topo = []
        while len(q) > 0:
            v = q.popleft()
            topo.append(v)
            for t in v.outgoing_nodes:
                q.append(t)
        print(topo)
        return topo


    def __repr__(self):
        topo = self.topo_sort()
        ret_val = ''
        for node in topo:
            if node.outgoing_deg() > 0:
                ret_val += node.name[:-1] + ' is linked to ' + str(node.outgoing_nodes) + '\n'
        return ret_val






    class Node():

        def __init__(self, head):
            self.name = head
            self.incoming_nodes = []
            self.outgoing_nodes = []
            self.is_removed_deps = False

        def removed_deps(self):
            self.is_removed_deps = True

        def incoming_deg(self):
            return len(self.incoming_nodes)

        def outgoing_deg(self):
            return len(self.outgoing_nodes)

        def add_incoming_node(self, n):
            self.incoming_nodes.append(n)

        def add_outgoing_node(self, n):
            self.outgoing_nodes.append(n)

        def __repr__(self):
            return 'Node(' + self.name + ')'