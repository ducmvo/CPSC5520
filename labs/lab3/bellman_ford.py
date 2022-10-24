"""
CPSC-5520, Seattle University
Distributed System - Lab 3
Bellman Ford Shortest-Path Implementation

FOREIGN EXCHANGE MARKETS
Efficient liquid markets in exchanging one country's currency for another

PRICE FEED
All the forex prices available are coming from a single publisher, Forex Provider
format: <timestamp, currency 1, currency 2, exchange rate>
Bytes[0:8] - timestamp is a 64-bit integer number of microseconds since EPOCH, big-endian.
Bytes[8:14] - currency names 3-character ISO ('USD', 'GBP', 'EUR', etc.), 8-bit ASCII from left to right.
Bytes[14:22] - exchange rate, 64-bit floating point number, IEEE 754 binary64 little-endian format.
Bytes[22:32] - reserved, not currently used (all set to 0-bits).

ARBITRAGE
Represent as a negative-weight cycle detected using Bellman-Ford shortest-path algorithm.
Weigh of edges for currency 1 and currency 2 e.g. (c1,c2,-log(rate)) | (c2,c1,log(rate))
Generated negative weigh cycle paths are used to execute trade for profit

:Authors: Duc Vo
:Version: 1
:Date: 10/24/2022
"""


class BellmanFord:
    """
    Bellman Ford Implementation for shortest-path algorithm and negative weight cycle tracing
    """

    def __init__(self, graph=None):
        """Initialized or default graph"""
        self.graph = graph

    def add_edge(self, vertex1, vertex2, weight):
        """Add an edge with weight to graph"""
        self.graph[vertex1][vertex2] = weight

    def bellman_ford(self, graph):
        """Delay initializing graph
        :param graph: encapsulate weighted graph
        """
        self.graph = graph

    def trace(self, edge, pred):
        """Trace the negative weight cycle
        :param edge: an edge in the cycle
        :param pred: predecessor table for tracing
        """
        # generate cycle from predecessor table
        cycle = [*edge]
        curr = cycle[1]
        while pred[curr] not in cycle:
            cycle.append(pred[curr])
            curr = pred[curr]
        cycle.append(pred[curr])

        # trim cycle
        index = cycle.index(pred[curr])
        return cycle[index:]

    def get_weight(self, edge, pred, weights):
        """Calculate weight of a path
        :param edge: An edge in the cycle to be traced
        :param pred: predecessor table for tracing
        :param weights: key-value of edges and weights
        """
        path = self.trace(edge, pred)
        # get cycle weight
        weight = 0
        for i in range(len(path) - 1):
            weight += weights[(path[i], path[i + 1])]
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

        :param start_vertex: start of all paths
        :param tolerance: only if a path is more than tolerance better will it be relaxed
        :return: distance, predecessor, negative_cycle
            distance:       dictionary keyed by vertex of the shortest distance from start_vertex to that vertex
            predecessor:    dictionary keyed by vertex of previous vertex in the shortest path from start_vertex
            negative_cycle: None if no negative cycle, otherwise an edge, (u,v), in one such cycle
        """
        if self.graph is None:
            raise Exception('Graph Not Found. Use bellman_ford() to initialize')

        weights = {}  # key-value of edges and weights
        vertices = set()  # contain all vertices name
        distance = {}  # key-value of edges and distance
        predecessor = {}  # key-value predecessor dictionary
        negative_cycle = None  # an edge in the found negative cycle

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
                weight = self.get_weight([u, v], predecessor, weights)
                if weight > tolerance:
                    negative_cycle = (u, v)
                    break  # stop for the first valid negative cycle

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
    print('TEST 3 PASSED ->', neg_edge == ('GBP', 'JPY'), neg_edge, '==', ('GBP', 'JPY'))
