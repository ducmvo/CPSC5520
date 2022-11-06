"""
chord_node takes a port number of an existing node 
(or 0 to indicate it should start a new network). 
This program joins a new node into the network using a 
system-assigned port number for itself. 
The node joins and then listens for incoming 
connections (other nodes or queriers). 
You can use blocking TCP for this and pickle for the marshaling.
"""
from functools import total_ordering
from hashlib import sha1
import sys
import socket
import pickle
import threading

from chord_finger import FingerEntry, ModRange

M = 3  # FIXME: Test environment 3-bit, normally = hashlib.sha1().digest_size * 8 = 20 * 8 = 160-bit
NODES = 2**M  # Test environment 2^3=8 Nodes, normally = 2^160 Nodes
BUF_SZ = 4096  # socket recv arg
BACKLOG = 100  # socket listen arg
TEST_BASE = 43544  # for testing use port numbers on localhost at TEST_BASE+n

@total_ordering
class Node:
    def __init__(self, address=None):
        self.address = address
        self.id = self.get_id(address)
        self.finger = None
        self.predecessor = None
        self.keys = None
                   
    def __repr__(self):
        """Return a string representation of this node"""
        return '{}'.format(self.id)
    
    def __getstate__(self):
        """Return the state of this node to be pickled"""
        # TODO: include predecessor and finger table
                # 'finger': self.finger
                # 'keys': self.keys
        return {'id': self.id, 'address': self.address }
    
    def __eq__(self, other):
        """Return True if this node is equal to other"""
        if isinstance(other, Node):
            return self.id == other.id
        return self.id == other

    def __lt__(self, other):
        """Return True if this node is less than other"""
        if isinstance(other, Node):
            return self.id < other.id
        return self.id < other
    
    @staticmethod
    def get_id(address) -> int:
        """ Return the SHA-1 hash of the node's address """
        if not address:
            return None
        host, port = address
        hash = sha1()
        hash.update(host.encode())
        hash.update(port.to_bytes(2, 'big'))
        return int.from_bytes(hash.digest(), 'big') % NODES
    
class ChordNode(Node):
    def __init__(self, num=0):
        self.server, self.address = self.start_server(num)
        self.id = self.get_id(self.address)
        self.finger = [None] + [FingerEntry(self.id, k) for k in range(1, M+1)]  # indexing starts at 1
        self.predecessor = None
        self.keys = {}
    
    @property
    def successor(self):
        return self.finger[1].node

    @successor.setter
    def successor(self, n: Node):
        self.finger[1].node = n
    
    def join(self, port: int):
        print('SELF', self)
        if port:
            address = ('127.0.0.1', port)
            np = Node(address)
            self.init_finger_table(np)
            print('JOINED', self.pr_finger())
            self.update_others()
            print('FINISH UPDATE OTHER', self.pr_finger())
        else:
            for i in range(1, M+1):
                self.finger[i].node = self
            self.predecessor = self
            print('CREATED', self.pr_finger())

    def init_finger_table(self, np: Node):
        """Initialize this node's finger table"""
        self.successor = self.call_rpc(np, 'find_successor', self.finger[1].start)
        self.predecessor = self.call_rpc(self.successor, 'get_predecessor')
        self.call_rpc(self.successor, 'set_predecessor', self)
        
        for i in range(1, M):
            if self.finger[i+1].start in ModRange(self.id, self.finger[i].node.id, NODES):
                self.finger[i+1].node = self.finger[i].node
            else:
                self.finger[i+1].node = self.call_rpc(np, 'find_successor', self.finger[i+1].start)  
   
    def find_successor(self, id):
        """ Ask this node to find id's successor = successor(predecessor(id))"""
        np = self.find_predecessor(id)
        return self.call_rpc(np, 'get_successor')

    def find_predecessor(self, id: int):
        """Find the predecessor of id"""        
        np = self
        mr = ModRange(np.id+1, self.successor.id+1, NODES)
        while id not in mr:
            print('ID', id, 'MODRANGE', mr)
            np = self.call_rpc(np, 'closest_preceding_finger', id)
            mr = ModRange(np.id+1, self.call_rpc(np,'get_successor').id+1, NODES)
        return np 
    
    def closest_preceding_finger(self, id: int):
        """Find the closest preceding finger of id"""
        for i in range(M, 0, -1):
            if self.finger[i].node.id in ModRange(self.id + 1, id, NODES):
                return self.finger[i].node
        return self
        
    def update_others(self):
        """ Update all other node that should have this node in their finger tables """
        for i in range(1, M+1):  # find last node p whose i-th finger might be this node
            # FIXME: bug in paper, have to add the 1 +
            # -> key right after the node that ith finger might be this node
            key = (1 + self.id - 2**(i-1) + NODES) % NODES
            p = self.predecessor if key == self else self.find_predecessor(key)
            print('NODE TO BE UPDATED {} p({})'.format(p, key))
            # self might become p's i-th finger node (successor)
            self.call_rpc(p, 'update_finger_table', self, i)

    def update_finger_table(self, s: Node, i: int):
        """ if s is i-th finger of n, update this node's finger table with s """        
        if (self.finger[i].start != self.finger[i].node
                 and s.id in ModRange(self.finger[i].start, self.finger[i].node.id, NODES)):
            print('update_finger_table({},{}): {}[{}] = {} since {} in [{},{})'.format(
                     s.id, i, self.id, i, s.id, s.id, self.finger[i].start, self.finger[i].node.id))
            self.finger[i].node = s
            print('#', self)
            p = self.predecessor
            if p != s:
                self.call_rpc(p, 'update_finger_table', s, i)
            
            print('UPDATED FINGER TABLE', self.pr_finger())
            
    def call_rpc(self, np: Node, method: str, arg1=None, arg2=None):
        """Call a remote procedure on node n"""
        print('C-RPC: {} SEND {} : {}, {}, {}'.format(self.id, np.id, method, arg1, arg2))
        
        if np == self:
            return self.dispatch_rpc(method, arg1, arg2)
            
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            address = np.address
            server.connect(address)
            self.send(server, (method, arg1, arg2))
            data = self.receive(server)
            print('C-RPC: {} RECV {} : {}, {}'.format(self.id, np.id, method, data))
            server.close()
            return data  
    
    def dispatch_rpc(self, method, arg1=None, arg2=None):
        """Dispatch an RPC request to the appropriate method"""
        if method == 'find_successor':
            return self.find_successor(arg1)
        elif method == 'find_predecessor':
            return self.find_predecessor(arg1)
        elif method == 'closest_preceding_finger':
            return self.closest_preceding_finger(arg1)
        elif method == 'get_successor':
            return self.successor
        elif method == 'get_predecessor':
            return self.predecessor
        elif method == 'set_predecessor':
            self.predecessor = arg1
        elif method == 'update_finger_table':
            self.update_finger_table(arg1, arg2)
        elif method == 'pr_finger':
            print(self.pr_finger())
        else:
            raise ValueError('Unknown method: {}'.format(method))
            
    def handle_rpc(self, client):
        """Handle a single RPC request"""
        method, arg1, arg2 = self.receive(client)
        print('H-RPC: {} RECV : {}, {}, {}'.format(self.id, method, arg1, arg2))
        result = self.dispatch_rpc(method, arg1, arg2)
        self.send(client, result)
        print('H-RPC: {} SEND: {}, {}'.format(self.id, method, result)) 
        client.close()
    
    def serve(self):
        """Listen for ready connections to receive data"""
        while True:
            client, _ = self.server.accept()
            threading.Thread(target=self.handle_rpc, args=(client,)).start()
    
    def pr_finger(self):
        """ Print the finger table """
        text = '\n===============================\n'
        text += 'finger table\nstart\t| int.\t| succ.\n'
        for i in range(1, M+1):
            text += '{}\t| [{},{})\t| {}\n'.format(self.finger[i].start, 
                    self.finger[i].start, self.finger[i].next_start, self.finger[i].node)
        text += '===============================\n'
        text += 'PREDECESSOR: {}\n'.format(self.predecessor)
        text += 'SUCCESSOR: {}\n'.format(self.successor)
        return text
    
    @staticmethod
    def start_server(num=None):
        """Create a listening server that accept TCP/IP request
        :return: server socket and its address in a tuple
        """
        num = TEST_BASE + num if num else 0
        address = ('localhost', num)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(address)
        sock.listen()
        print(f'> CHORD SERVER LISTENING ON {sock.getsockname()}\n')
        return sock, sock.getsockname()
    
    @staticmethod
    def send(conn, message=None, buffer_size=BUF_SZ):
        """Serialized and send all data using passed in socket"""
        data = pickle.dumps(message)
        conn.sendall(data)

    @staticmethod
    def receive(conn, buffer_size=BUF_SZ):
        """Receive raw data from a passed in socket
        :return: deserialized data
        """
        data = conn.recv(buffer_size)
        return pickle.loads(data)
             

if __name__ == '__main__':
    if len(sys.argv) not in range(1, 4):
        num = 6
        port = 5000
        print('Usage: python chord.py <port> <num>')
        print('<port>: (optional) port number of a node in network to contact upon joining')
        print('\t default: 0, create a new network')
        print('<num>: (optional) number added to TEST_BASE ({}) to provide specific port to use'.format(TEST_BASE))
        print('\t default: 0, use system generated port number')
        print('Example: python chord.py {} {}'.format(port, num))
        print('\t join network, contacting port {}, listening on port {} ({} + {}) '.format(port, TEST_BASE+num, TEST_BASE, num))
        sys.exit(1)
    
    num, port = 0, 0
    if len(sys.argv) >= 2:
        port = int(sys.argv[1]) 
    if len(sys.argv) == 3:
        num = int(sys.argv[2])
          
    node = ChordNode(num) # start a server
    node.join(port)  # join the network
    node.serve()
    
      
    
    
