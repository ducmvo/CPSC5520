"""
chord_node takes a port number of an existing node 
(or 0 to indicate it should start a new network). 
This program joins a new node into the network using a 
system-assigned port number for itself. 
The node joins and then listens for incoming 
connections (other nodes or queriers). 
You can use blocking TCP for this and pickle for the marshaling.
"""
from datetime import datetime
from enum import Enum
from functools import total_ordering
from hashlib import sha1
import sys
import socket
import pickle
import threading

from chord_finger import FingerEntry, ModRange, NODES, M

# M = 7  # FIXME: Test environment 3-bit, normally = hashlib.sha1().digest_size * 8 = 20 * 8 = 160-bit
# NODES = 2**M  # Test environment 2^3=8 Nodes, normally = 2^160 Nodes
BUF_SZ = 4096  # socket recv arg
BACKLOG = 100  # socket listen arg
TEST_BASE = 43544  # for testing use port numbers on localhost at TEST_BASE+n

class Method(Enum):
    FIND_SUCCESSOR = 'FIND_SUCCESSOR'
    FIND_PREDECESSOR = 'FIND_PREDECESSOR'
    GET_SUCCESSOR = 'GET_SUCCESSOR'
    GET_PREDECESSOR = 'GET_PREDECESSOR'
    SET_PREDECESSOR = 'SET_PREDECESSOR'
    CLOSEST_PRECEDING_FINGER = 'CLOSEST_PRECEDING_FINGER'
    UPDATE_FINGER_TABLE = 'UPDATE_FINGER_TABLE'
    POPULATE = 'POPULATE'
    INSERT = 'INSERT'
    QUERY = 'QUERY'
    
    def is_data_signal(self):
        return self in (Method.POPULATE, Method.INSERT)
    
@total_ordering 
class BaseNode:
    def __init__(self, address=None):
        self.address = address
        self.id = self.hash(*address)
    
    def __repr__(self):
        """Return a string representation of this node"""
        return '{}'.format(self.id)
    
    def __getstate__(self):
        """Return the state of this node to be pickled"""
        return {'id': self.id, 'address': self.address }
    
    def __eq__(self, other):
        """Return True if this node is equal to other"""
        if isinstance(other, BaseNode):
            return self.id == other.id
        return self.id == other

    def __lt__(self, other):
        """Return True if this node is less than other"""
        if isinstance(other, BaseNode):
            return self.id < other.id
        return self.id < other   

    @staticmethod
    def hash(*data: str | int) -> int:
        _hash = sha1()
        for item in data:
            if isinstance(item, int):
                _hash.update(item.to_bytes(2, 'big'))
            elif isinstance(item, str):
                _hash.update(item.encode())
            else:
                raise TypeError('data must be int or str')
        return int.from_bytes(_hash.digest(), 'big') % NODES

class NodeServer:
    def __init__(self) -> None:
        self.server = None
    
    def start_server(self, num=None):
        """Create a listening server that accept TCP/IP request
        :return: server socket and its address in a tuple
        """
        num = TEST_BASE + num if num else 0
        address = ('localhost', num)
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.bind(address)
        self.server.listen()
        print(f'> NODE SERVER :{self.server.getsockname()[1]}')
        return self.server.getsockname()
    
class ChordNode(BaseNode, NodeServer):
    def __init__(self, num=None):
        """Initialize a new node"""
        self.address = self.start_server(num)
        self.id = self.hash(*self.address)
        self.finger = [None] + [FingerEntry(self.id, k) for k in range(1, M+1)]
        self.predecessor = None
        self.keys = {}
        print('> NODE ID', self)
    
    @property
    def successor(self):
        return self.finger[1].node

    @successor.setter
    def successor(self, n: BaseNode):
        self.finger[1].node = n
    
    def populate(self, data=None):
        """Populate this node's keys data"""
        if not data:
            return {'status': 'ERROR', 'message': 'NO DATA'}
        
        if (self == self.predecessor):
            #  handle case where this is the only node in the network
            self.keys, data = data, None
        else:
            # self responsible for keys in (predecessor , self]
            mr = ModRange(self.predecessor.id + 1, self.id + 1, NODES)
            for key in mr:
                if key in data: 
                    self.keys[key] = data[key]        
            # slice (predecessor , self] from data into self.keys
            for key in self.keys: 
                del data[key]
            
        print('> POPULATED KEYS: {}'.format(list(self.keys.keys())))
        if not data: 
            return {'status': 'OK', 'message': 'NETWORK COMPLETE'}
            # return "POPULATED OK: NETWORK COMPLETE"
        return self.call_rpc(self.successor, Method.POPULATE, data, None)
        
    def insert(self, data=None):
        """Insert data to network using this node as the starting point"""
        if not data:
            return {'status': 'ERROR', 'message': 'NO DATA'}
        
        # handle case where this is the only node in the network
        # or key in the range (predecessor , self]
        key, value = data
        mr = ModRange(self.predecessor.id + 1, self.id + 1, NODES)
        if self == self.predecessor or key in mr:
            self.keys[key] = value
            print('> INSERTED KEY: {}, VALUE: {}'.format(self, key, value))
            return {'status': 'OK', 'message': 'NODE {} KEY {}'.format(self.id, key)}
        
        # find the node responsible for the key to insert
        s = self.find_successor(key)
        return self.call_rpc(s, Method.INSERT, data) 
        
    def join(self, port: int):
        if port:
            address = ('127.0.0.1', port)
            np = BaseNode(address)
            print('> JOINING VIA {}'.format(np))
            self.init_finger_table(np)
            self.update_others()
            print('> NETWORK JOINED', self.pr_finger())
        else:
            for i in range(1, M+1):
                self.finger[i].node = self
            self.predecessor = self
            print('> NETWORK CREATED', self.pr_finger())

    def init_finger_table(self, np: BaseNode):
        """Initialize this node's finger table"""
        self.successor = self.call_rpc(np, Method.FIND_SUCCESSOR, self.finger[1].start)
        self.predecessor = self.call_rpc(self.successor, Method.GET_PREDECESSOR)
        self.call_rpc(self.successor, Method.SET_PREDECESSOR, self)
        
        for i in range(1, M):
            if self.finger[i+1].start in ModRange(self.id, self.finger[i].node.id, NODES):
                self.finger[i+1].node = self.finger[i].node
            else:
                self.finger[i+1].node = self.call_rpc(np, Method.FIND_SUCCESSOR, self.finger[i+1].start)  
   
    def find_successor(self, id):
        """ Ask this node to find id's successor = successor(predecessor(id))"""
        np = self.find_predecessor(id)
        return self.call_rpc(np, Method.GET_SUCCESSOR)

    def find_predecessor(self, id: int):
        """Find the predecessor of id"""        
        np = self
        mr = ModRange(np.id+1, self.successor.id+1, NODES)
        while id not in mr:
            print(f'> ID {id} NOT IN MODRANGE {mr}')
            np = self.call_rpc(np, Method.CLOSEST_PRECEDING_FINGER, id)
            mr = ModRange(np.id+1, self.call_rpc(np,Method.GET_SUCCESSOR).id+1, NODES)
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
            print('> NODE TO BE UPDATED {} p({})'.format(p, key))
            # self might become p's i-th finger node (successor)
            self.call_rpc(p, Method.UPDATE_FINGER_TABLE, self, i)

    def update_finger_table(self, s: BaseNode, i: int):
        """ if s is i-th finger of n, update this node's finger table with s """        
        if (self.finger[i].start != self.finger[i].node
                 and s.id in ModRange(self.finger[i].start, self.finger[i].node.id, NODES)):
            print('update_finger_table({},{}): {}[{}] = {} since {} in [{},{})'.format(
                     s.id, i, self.id, i, s.id, s.id, self.finger[i].start, self.finger[i].node.id))
            self.finger[i].node = s
            p = self.predecessor
            if p != s:
                self.call_rpc(p, Method.UPDATE_FINGER_TABLE, s, i)
            
            print('UPDATED FINGER TABLE', self.pr_finger())
            
    def call_rpc(self, np: BaseNode, method: Method, arg1=None, arg2=None):
        """Call a remote procedure on node n"""
        if isinstance(arg1, dict):
            print('{} C-RPC: {} SEND {} {}, {}'.format(
                self.pr_now(), self.id, np.id, 
                method.value, list(arg1.keys())))
        else:  
            print('{} C-RPC: {} SEND {} {}, {} {}'.format(
                self.pr_now(), self.id, np.id, 
                method.value, arg1 or "", arg2 or ""))

        if np == self:
            return self.dispatch_rpc(method, arg1, arg2)
            
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            address = np.address
            server.connect(address)
            if method.is_data_signal():
                # signal to start sending chunked data
                self.send(server, (method, None, arg2))  
                self.receive(server)  
                self.send(server, arg1)
            else:
                self.send(server, (method, arg1, arg2))
            
            # close write side, wait for read    
            server.shutdown(socket.SHUT_WR)  
            data = self.receive(server)
            print('{} C-RPC: {} RECV {} {}, {}'.format(
                self.pr_now(), self.id, np.id, method.value, data))
            server.close()
            return data  
    
    def dispatch_rpc(self, method: Method, arg1=None, arg2=None):
        """Dispatch an RPC request to the appropriate method"""
        if method.value == Method.FIND_SUCCESSOR.value:
            return self.find_successor(arg1)
        elif method.value == Method.FIND_PREDECESSOR.value:
            return self.find_predecessor(arg1)
        elif method.value == Method.CLOSEST_PRECEDING_FINGER.value:
            return self.closest_preceding_finger(arg1)
        elif method.value == Method.UPDATE_FINGER_TABLE.value:
            return self.update_finger_table(arg1, arg2)
        elif method.value == Method.GET_SUCCESSOR.value:
            return self.successor
        elif method.value == Method.GET_PREDECESSOR.value:
            return self.predecessor
        elif method.value == Method.SET_PREDECESSOR.value:
            self.predecessor = arg1
        elif method.value == Method.POPULATE.value:
            return self.populate(arg1)
        elif method.value == Method.INSERT.value:
            return self.insert(arg1)
        else:
            raise ValueError('Unknown method'.format(method))
            
    def handle_rpc(self, client):
        """Handle a single RPC request"""
        method, arg1, arg2 = self.receive(client)
        
        # signal to start receiving data in chunks
        if method.is_data_signal():
            self.send(client, {'status': 'OK', 'message': 'READY TO RECEIVE DATA'})
            arg1 = self.receive(client, chunks=True)
        
        if isinstance(arg1, dict):
            print('{} H-RPC: {} RECV {}, {}'.format(
                self.pr_now(), self.id, 
                method.value,  list(arg1.keys())))
        else:
            print('{} H-RPC: {} RECV {}, {} {}'.format(
                self.pr_now(), self.id, 
                method.value, arg1 or "", arg2 or ""))
        
        result = self.dispatch_rpc(method, arg1, arg2)
        self.send(client, result)
        print('{} H-RPC: {} SEND {}, {}'.format(
            self.pr_now(), self.id, method.value, result or "")) 
        client.close()
    
    def serve(self):
        """Listen for ready connections to receive data"""
        while True:
            client, _ = self.server.accept()
            threading.Thread(target=self.handle_rpc, args=(client,)).start()
    
    def pr_finger(self):
        """ Print the finger table """
        row = '{:>6} | {:<10} | {:<6}\n'
        header = '{:>6}   {:<10}   {:<6}\n'
        text = '\n========================\n'
        text += f'FINGER TABLE - SELF ID {self}\n'
        text += header.format('start', 'int.', 'succ.')
        for i in range(1, M+1):
            start = str(self.finger[i].start)
            interval = '[{},{})'.format(self.finger[i].start, self.finger[i].next_start)
            succ = str(self.finger[i].node)
            text += row.format(start, interval, succ)
        text += 'PREDECESSOR: {}\n'.format(self.predecessor)
        text += 'SUCCESSOR: {}\n'.format(self.successor)
        text += '========================\n'
        return text    
    
    @staticmethod
    def send(conn, message=None, buffer_size=BUF_SZ):
        """Serialized and send all data using passed in socket"""
        data = pickle.dumps(message)
        conn.sendall(data)

    @staticmethod
    def receive(conn, buffer_size=BUF_SZ, chunks=False):
        """Receive raw data from a passed in socket
        :return: deserialized data
        """
        data = b''
        if chunks:
            while True:
                packet = conn.recv(BUF_SZ)
                if not packet: break
                data += packet
        else:
            data = conn.recv(buffer_size)
        return pickle.loads(data)
    
    @staticmethod
    def pr_now():
        """Print current time in H:M:S.f format"""
        return datetime.now().strftime('%H:%M:%S.%f')


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
          
    node = ChordNode(num) # create a new node server
    node.join(port)  # join the network
    node.serve()
    
    # TEST CASE FOR 3-BIT ADDRESS SPACE
    # node 0 -> num 1
    # node 1 -> num 2
    # node 2 -> num 6
    # node 3 -> num 21
    # node 4 -> num 4
    # node 5 -> num 12
    # node 6 -> num 16
    # node 7 -> num 9
    
      
    
    
