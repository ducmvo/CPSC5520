from chord_node import ChordNode as Node
from chord_populate import ChordPopulate as Populate
from chord_query import ChordQuery as Query
import threading
import time
import random

TEST_BASE = 43544
FILE_NAME = 'Career_Stats_Passing.csv'


class ChordTest:
    def __init__(self):
        self.nums = [11, 2, 21, 19]
        self.nodes = []
        self.ports = []
        self.threads = []
        
        for num in self.nums:
            self.nodes.append(Node(num))
            self.ports.append(num + TEST_BASE)
        
        uports = { TEST_BASE + self.nums[0] }
        for i, node in enumerate(self.nodes):
            p = self.rport(list(uports))
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
        cp.populate(self.rport(), data)

        # Query the network via random nodes
        cq = Query()
        value = cq.query(self.rport(), 'vodangminhduc')
        print('\n====================\n', value != 'khakhakhakha', '\n====================\n', )
        cp.insert(self.rport(), 'vodangminhduc', 'khakhakhakha')
        value = cq.query(self.rport(), 'vodangminhduc')
        print('\n====================\n', value == 'khakhakhakha', '\n====================\n')
    
    def rport(self, ports=None):
        if ports is None:
            ports = self.ports
        return random.choice(ports)
    
    @staticmethod    
    def serve(node, port):
        node.join(port)
        node.serve()

if __name__ == '__main__':
    ct = ChordTest()
    ct.run()

