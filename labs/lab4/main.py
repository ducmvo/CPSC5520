import sys
import time
import random
import threading
from chord_query import ChordQuery as Query
from chord_node import ChordNode as Node, NODES
from chord_populate import ChordPopulate as Populate

TEST_BASE = 43544
FILE_NAME = 'Career_Stats_Passing.csv'

class ChordTest:
    def __init__(self, num_nodes=5):

        self.nodes = []
        self.ports = []
        self.threads = []
        
        for _ in range(num_nodes):
            node = Node(0)
            self.nodes.append(node)
            self.ports.append(node.address[1])
        
        uports = { self.ports[0] }
        for i, node in enumerate(self.nodes):
            p = self.rand_port(list(uports))
            uports.add(p)
            if i == 0: p = 0
            t = threading.Thread(target=self.serve, args=(node, p))
            self.threads.append(t)
            
    def run(self):
        for t in self.threads:
            t.start()
            time.sleep(0.1)
        
        # Populate the network via random nodes
        cp = Populate()
        data = cp.parse(FILE_NAME)
        cp.populate(self.rand_port(), data)

        # Query the network via random nodes
        cq = Query()
        value = cq.query(self.rand_port(), 'Foo')
        print('\n====================\n', 'TEST PASSED: ',
            value != 'Bar', '\n====================\n', )
        cp.insert(self.rand_port(), 'Foo', 'Bar')
        value = cq.query(self.rand_port(), 'Foo')
        
        print('\n====================\n', 'TEST PASSED: ', 
            value == 'Bar', '\n====================\n')
        
        print([Node.hash('127.0.0.1', port) for port in self.ports])
         
                 
    def rand_port(self, ports=None):
        if ports is None:
            ports = self.ports
        return random.choice(ports)
    
    @staticmethod    
    def serve(node, port):
        node.join(port)
        node.serve()

if __name__ == '__main__':
    ct = ChordTest(10) # 10 random nodes with random ports
    ct.run()

