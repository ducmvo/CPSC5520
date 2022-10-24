class BellmanFord:
    def __init__(self, graph=None):
        self.graph = graph

    def add_edge(self, vertex1, vertex2, weight):
        self.graph[vertex1][vertex2] = weight

    def update_graph(self, graph):
        self.graph = graph

    @staticmethod
    def check_cycle_weight(cycle, pred, weights):
        # generate cycle from predecessor table
        curr = cycle[1]
        while pred[curr] not in cycle:
            cycle.append(pred[curr])
            curr = pred[curr]
        cycle.append(pred[curr])

        # trim cycle
        index = cycle.index(pred[curr])
        cycle = cycle[index:]

        # get cycle weight
        weight = 0
        for i in range(len(cycle) - 1):
            weight += weights[(cycle[i], cycle[i + 1])]
        return weight

    def shortest_paths(self, start_vertex, tolerance=0):
        """
        Find the shortest paths (sum of edge weights) from start_vertex to every other vertex.
        Also detect if there are negative cycles and report one of them.
        Edges may be negative.

        For relaxation and cycle detection, we use tolerance. Only relaxations resulting in an improvement
        greater than tolerance are considered. For negative cycle detection, if the sum of weights is
        greater than -tolerance it is not reported as a negative cycle. This is useful when circuits are expected
        to be close to zero.

        g = BellmanFord({'a': {'b': 1, 'c':5}, 'b': {'c': 2, 'a': 10}, 'c': {'a': 14, 'd': -3}, 'e': {'a': 100}})
        dist, prev, neg_edge = g.shortest_paths('a')
        [(v, dist[v]) for v in sorted(dist)]  # the shortest distance from 'a' to each other vertex
        [('a', 0), ('b', 1), ('c', 3), ('d', 0), ('e', inf)]
        [(v, prev[v]) for v in sorted(prev)]  # last edge in shortest paths
        [('a', None), ('b', 'a'), ('c', 'b'), ('d', 'c'), ('e', None)]
        neg_edge is None
        True
        g.add_edge('a', 'e', -200)
        dist, prev, neg_edge = g.shortest_paths('a')
        neg_edge  # edge where we noticed a negative cycle
        ('e', 'a')

        :param start_vertex: start of all paths
        :param tolerance: only if a path is more than tolerance better will it be relaxed
        :return: distance, predecessor, negative_cycle
            distance:       dictionary keyed by vertex of the shortest distance from start_vertex to that vertex
            predecessor:    dictionary keyed by vertex of previous vertex in the shortest path from start_vertex
            negative_cycle: None if no negative cycle, otherwise an edge, (u,v), in one such cycle
        """
        weights = {}
        vertices = set()
        distance = {}
        predecessor = {}
        negative_cycle = None

        # Initialize
        for u in self.graph:
            for v in self.graph[u]:
                vertices.add(u)
                vertices.add(v)
                w = self.graph[u][v]
                weights[(u, v)] = w

        for v in vertices:
            distance[v] = float('inf')
            predecessor[v] = None
        distance[start_vertex] = 0

        # Relaxation and cycle detection
        # Relax edges |V| - 1 times
        for i in range(len(weights) - 1):
            for u, v in weights:
                w = weights[(u, v)]
                d = distance[u] + w
                if d < distance[v] and distance[v] - d > tolerance:
                    distance[v] = d
                    predecessor[v] = u

        # Negative cycle detection:
        for u, v in weights:
            w = weights[(u, v)]
            d = distance[u] + w
            if d < distance[v] and distance[v] - d > tolerance:
                weight = self.check_cycle_weight([u, v], predecessor, weights)
                if weight > tolerance:
                    negative_cycle = (u, v)
                    break

        return distance, predecessor, negative_cycle


if __name__ == '__main__':
    # TEST 1
    data = {'a': {'b': 1, 'c': 5}, 'b': {'c': 2, 'a': 10}, 'c': {'a': 14, 'd': -3}, 'e': {'a': 100}}
    g = BellmanFord(data)
    dist, prev, neg_edge = g.shortest_paths('a')
    res1 = [(v, dist[v]) for v in sorted(dist)]
    exp1 = [('a', 0), ('b', 1), ('c', 3), ('d', 0), ('e', float('inf'))]
    res2 = [(v, prev[v]) for v in sorted(prev)]
    exp2 = [('a', None), ('b', 'a'), ('c', 'b'), ('d', 'c'), ('e', None)]
    dist, prev, neg_edge = g.shortest_paths('a')
    print("Test 1 Passed ->", res1 == exp1 and res2 == exp2 and neg_edge is None)
    print("Negative Edge", neg_edge)

    # last edge in the shortest paths
    # TEST 2
    g.add_edge('a', 'e', -200)
    neg_edge = g.shortest_paths('a')[2]
    print("Test 2 Passed ->", neg_edge is not None, neg_edge, '==', ('a', 'b'))

    # TEST 3
    g1 = {'AUD': {'USD': 0.2877220732518022},
          'USD': {'AUD': -0.2877220732518022, 'JPY': -4.605404058637752, 'CHF': 0.0009404420770567194,
                  'EUR': 0.08967615664177453, 'GBP': 0.2205802689110449},
          'JPY': {'USD': 4.605404058637752, 'GBP': 4.812388442871466}, 'CHF': {'USD': -0.0009404420770567194},
          'EUR': {'USD': -0.08967615664177453}, 'GBP': {'USD': -0.2205802689110449, 'JPY': -4.812388442871466}}

    g = BellmanFord(g1)
    dist, prev, neg_edge = g.shortest_paths('USD')
    print('TEST 3 PASSED ->', neg_edge is None, neg_edge, '==', None)

    # TEST 4
    g2 = {'AUD': {'USD': 0.2878154146747933},
          'USD': {'AUD': -0.2878154146747933, 'JPY': -4.605108484084568, 'CHF': 0.0009404420770567194,
                  'EUR': 0.08971272506129155, 'GBP': 0.22059630984751522},
          'JPY': {'USD': 4.605108484084568, 'GBP': 4.812388442871466}, 'CHF': {'USD': -0.0009404420770567194},
          'EUR': {'USD': -0.08971272506129155, 'CAD': -0.8236141426654595},
          'GBP': {'USD': -0.22059630984751522, 'JPY': -4.812388442871466, 'CAD': 0.5626802184544311},
          'CAD': {'EUR': 0.8236141426654595, 'GBP': -0.5626802184544311}}
    g = BellmanFord(g2)
    dist, prev, neg_edge = g.shortest_paths('USD')
    print(neg_edge)
    print('TEST 3 PASSED ->', neg_edge == ('GBP', 'JPY'), neg_edge, '==', ('GBP', 'JPY'))
