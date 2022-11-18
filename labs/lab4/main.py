import time
import random
import threading
from chord_query import ChordQuery as Query
from chord_node import ChordNode as Node, ENABLE_JOIN_WITH_DATA
from chord_populate import ChordPopulate as Populate


TEST_BASE = 43544
FILE_NAME = 'Career_Stats_Passing.csv'

class ChordTest:
    def __init__(self, num_nodes=5):
        self.nodes = []
        self.ports = []
        self.threads = []
        self.ids = []
        for _ in range(num_nodes):
            node = self.generate_node()
            self.nodes.append(node)
            self.ports.append(node.address[1])
        
        # node join random running node
        uports = { self.ports[0] }
        for i, node in enumerate(self.nodes):
            p = self.rand_port(list(uports))
            uports.add(p)
            if i == 0: p = 0
            t = threading.Thread(target=self.serve, args=(node, p))
            self.threads.append(t)
    
    def generate_node(self):
        node = Node(0)
        # avoid node id collision for different hashed addresses
        while node.id in self.ids:
            print("COLLISION", node.id, node.address)
            node = Node(0)
        self.ids.append(node.id)
        return node
            
    def run(self):
        for t in self.threads:
            t.start()
            # delay for node to join one at a time
            # TODO: hanlde concurrent join
            time.sleep(0.01) 
        
        cq = Query()
        cp = Populate()
        test_results = ''
        
        # Test query non existing key
        res = cq.query(self.rand_port(), 'Foo')
        test_results += 'TEST 1 PASSED: {}\n'.format(res == 'KEY NOT FOUND')
        
        # Test parse data from file
        data = cp.parse(FILE_NAME)
        
        # Test insert each key-value pair to network
        for key, value in data.items():
            cp.insert(self.rand_port(), key, value)
        
        # Test query the network via random nodes
        res = cq.query(self.rand_port(), 'Foo')
        test_results += 'TEST 2 PASSED: {}\n'.format(res.get('value') != 'Bar')
        
        # Test query a known key from network
        cp.insert(self.rand_port(), 'Foo', 'Bar')
        res = cq.query(self.rand_port(), 'Foo')
        test_results += 'TEST 3 PASSED: {}'.format(res.get('value') == 'Bar')

        if ENABLE_JOIN_WITH_DATA:
            print('{0:=^40}'.format('JOIN AFTER DATA POPULATED'))
            node = self.generate_node()
            p = self.rand_port(self.ports)
            threading.Thread(target=self.serve, args=(node, p)).start()
            print(f'{"=":=^40}')
            time.sleep(0.5)
        
        # Display test results
        print('{0:=^40}'.format('RESULTS'))
        print('NODES', [Node.hash('127.0.0.1', port) for port in self.ports])
        print(test_results)
        print(f'{"=":=^40}')
                 
    def rand_port(self, ports=None):
        if ports is None:
            ports = self.ports
        return random.choice(ports)
    
    @staticmethod    
    def serve(node, port):
        node.run(port)

if __name__ == '__main__':
    ct = ChordTest(5) # 5 random nodes with random ports
    ct.run()

