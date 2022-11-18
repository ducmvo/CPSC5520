"""
CPSC-5520, Seattle University
LAB4 - Distributed System
CHORD NODE IMPLEMENTATION

Chord System described in the original paper from 2001 by Stoica, Morris, Karger,  Kaashoek, and Balakrishna
Link: https://pdos.csail.mit.edu/papers/chord:sigcomm01/chord_sigcomm.pdf
This implementation using Distributed Hash Table (DHT) is a distributed system 
that provides a lookup service similar to a hash table. 
Consitant Hashing technique is used for mapping keys to a set of nodes in this Chord System.

JOIN
This program joins a new node into the network using a system-assigned port number for itself. 
The node joins and then listens for incoming blocking TCP/IP connections. 

QUERY
The system allows a querier to talk to any arbitrary node in the network 
to query a value for a given key

POPULATE
The system allows a querier to to talk to any arbitrary node in the network  
to insert a key-value pair into the DHT.


:Authors: Duc Vo
:Version: 1
:Date: 11/18/2022

"""

import sys
import socket
import pickle
import threading
from enum import Enum
from hashlib import sha1
from datetime import datetime
from functools import total_ordering

M = 7  # FIXME: Test environment 3-bit, normally = hashlib.sha1().digest_size * 8 = 20 * 8 = 160-bit
NODES = 2**M  # Test environment 2^3=8 Nodes, normally = 2^160 Nodes
BUF_SZ = 4096  # socket recv arg
BACKLOG = 100  # socket listen arg
TEST_BASE = 43544  # for testing use port numbers on localhost at TEST_BASE+n

DIS_LEN = 50
DISPLAY_HANDLE_RPC = True  # Disable printing of RPCs handled by Node
DISPLAY_CALL_RPC = True  # Disable printing of RPCs called by Node
ENABLE_JOIN_WITH_DATA = True  # Enable Node join after network has been populated

# COLOR DISPLAY FOR TERMINAL OUTPUT ONLY
BLUE = '\033[94m'
CYAN = '\033[96m'
GREEN = '\033[92m'
WARNING = '\033[93m'
FAIL = '\033[91m'
END = '\033[0m'

def color(c, msg):
    return f'{c}{msg}{END}'

class Method(Enum):
    FIND_SUCCESSOR = 'FIND_SUCCESSOR'
    FIND_PREDECESSOR = 'FIND_PREDECESSOR'
    GET_SUCCESSOR = 'GET_SUCCESSOR'
    GET_PREDECESSOR = 'GET_PREDECESSOR'
    SET_PREDECESSOR = 'SET_PREDECESSOR'
    CLOSEST_PRECEDING_FINGER = 'CLOSEST_PRECEDING_FINGER'
    UPDATE_FINGER_TABLE = 'UPDATE_FINGER_TABLE'
    INSERT = 'INSERT'
    QUERY = 'QUERY'

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
        self.server.listen(BACKLOG)
        print(color(CYAN, f'> NODE SERVER LISTEN ON PORT {self.server.getsockname()[1]}'))
        return self.server.getsockname()

    def serve(self, handle_connection):
        """Listen for ready connections to receive data"""   
        while True:
            client, _ = self.server.accept()
            threading.Thread(target=handle_connection, args=(client,)).start()
    
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

@total_ordering 
class BaseNode(NodeServer):
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
    
    @staticmethod         
    def validate(port, key=None, port_only=False):
        if not port:
            print('Error: Invalid input, port must be non-empty')
            return False
        
        if  not port_only and not key:
            print('Error: Invalid input, key, value must be non-empty')
            return False
        
        try:
            port = int(port)
            if port > 65535 or port < 0:
                raise ValueError
        except ValueError:
            print('Error: Invalid input, port must be an integer between 0 and 65535')
            return False
        return True
    
class ModRange(object):
    """
    Range-like object that wraps around 0 at some divisor using modulo arithmetic.
    """
    def __init__(self, start, stop, divisor):
        self.divisor = divisor
        self.start = start % self.divisor
        self.stop = stop % self.divisor
        # we want to use ranges to make things speedy, but if it wraps around the 0 node, we have to use two
        if self.start < self.stop:
            self.intervals = (range(self.start, self.stop),)
        elif self.stop == 0:
            self.intervals = (range(self.start, self.divisor),)
        else:
            self.intervals = (range(self.start, self.divisor), range(0, self.stop))

    def __repr__(self):
        """ Something like the interval|node charts in the paper """
        return '[{}:{})%{}'.format(self.start, self.stop, self.divisor)

    def __contains__(self, id):
        """ Is the given id within this finger's interval? """
        for interval in self.intervals:
            if id in interval:
                return True
        return False

    def __len__(self):
        total = 0
        for interval in self.intervals:
            total += len(interval)
        return total

    def __iter__(self):
        return ModRangeIter(self, 0, -1)

class ModRangeIter(object):
    """ Iterator class for ModRange """
    def __init__(self, mr, i, j):
        self.mr, self.i, self.j = mr, i, j

    def __iter__(self):
        return ModRangeIter(self.mr, self.i, self.j)

    def __next__(self):
        if self.j == len(self.mr.intervals[self.i]) - 1:
            if self.i == len(self.mr.intervals) - 1:
                raise StopIteration()
            else:
                self.i += 1
                self.j = 0
        else:
            self.j += 1
        return self.mr.intervals[self.i][self.j]

class FingerEntry(object):
    """
    Row in a finger table.
    """
    def __init__(self, n, k, node=None):
        if not (0 <= n < NODES and 0 < k <= M):
            raise ValueError('invalid finger entry values')
        self.start = (n + 2**(k-1)) % NODES
        self.next_start = (n + 2**k) % NODES if k < M else n
        self.interval = ModRange(self.start, self.next_start, NODES)
        self.node = node

    def __repr__(self):
        """ Something like the interval|node charts in the paper """
        return '[{},{}):{}'.format(self.start, self.next_start, self.node)

    def __contains__(self, id):
        """ Is the given id within this finger's interval? """
        return id in self.interval
 
class ChordNode(BaseNode):
    """
    Chord Node takes a port number of an existing node (or 0 to indicate it should start a new network).
    Notation: 
        C-RPC: Call RPC
        H-RPC: Handle RPC
        P-RPC: Populate RPC
        Q-RPC: Query RPC
    """
    def __init__(self, num=None):
        """Initialize a new node"""
        self.address = self.start_server(num)
        self.id = self.hash(*self.address)
        self.finger = [None] + [FingerEntry(self.id, k) for k in range(1, M+1)]
        self.predecessor = None
        self.keys = {}
        print(color(CYAN, f'> NODE ID {self}'))
    
    @property
    def successor(self):
        return self.finger[1].node

    @successor.setter
    def successor(self, n: BaseNode):
        self.finger[1].node = n
    
        
    def insert(self, key, value):
        """Insert data to network using this node as the starting point"""
        if not key or not value:
            return {'status': 'ERROR', 'message': 'NO DATA'}
        
        mr = ModRange(self.predecessor.id + 1, self.id + 1, NODES)
        if self == self.predecessor or key in mr:
            self.keys[key] = value
            print(color(CYAN,f'> INSERTED KEY: {key}, VALUE: {str(value)[:20]}...'))
            return {'status': 'OK', 'message': 'NODE {} KEY {}'.format(self.id, key)}
        
        # find the node responsible for the key to insert
        s = self.find_successor(key)
        return self.call_rpc(s, Method.INSERT, key, value) 
    
    def query(self, key=None):
        """Query data from network using this node as the starting point"""
        if not key:
            return {'status': 'ERROR', 'message': 'NO KEY PROVIDED'}
        
        # handle case where this is the only node in the network
        # or key in the range (predecessor , self]
        mr = ModRange(self.predecessor.id + 1, self.id + 1, NODES)
        if self == self.predecessor or key in mr:
            if key in self.keys:
                # print('> QUERY KEY: {}, VALUE: {}'.format(key, self.keys[key]))
                return {'status': 'OK', 'message': {'key': key, 'value': self.keys[key]}}
            return {'status': 'OK', 'message': 'KEY NOT FOUND' }
        
        # find the node responsible for the key to query
        s = self.find_successor(key)
        return self.call_rpc(s, Method.QUERY, key)
    
    def join(self, port: int):
        """Create new network if only node or
        Join the network using a provided node as starting point"""
        if port:
            address = ('127.0.0.1', port)
            np = BaseNode(address)
            print(color(CYAN,f'> JOINING VIA {np}'))
            self.init_finger_table(np)
            self.update_others()
            print(color(CYAN,'> NETWORK JOINED'), self.pr_finger())
        else:
            for i in range(1, M+1):
                self.finger[i].node = self
            self.predecessor = self
            print(color(CYAN, '> NETWORK CREATED'), self.pr_finger())

    def init_finger_table(self, np: BaseNode):
        """Initialize this node's finger table"""
        self.successor = self.call_rpc(np, Method.FIND_SUCCESSOR, self.finger[1].start)
        self.predecessor = self.call_rpc(self.successor, Method.GET_PREDECESSOR)
        
        # this RPC sets the predecessor of the successor to this node
        # the remote node will insert coresponding keys into this node
        self.call_rpc(self.successor, Method.SET_PREDECESSOR, self)
        
        for i in range(1, M):
            if self.finger[i+1].start in ModRange(self.id, self.finger[i].node.id, NODES):
                self.finger[i+1].node = self.finger[i].node
            else:
                self.finger[i+1].node = self.call_rpc(np, Method.FIND_SUCCESSOR, self.finger[i+1].start)  
    
    def set_predecessor(self, np: BaseNode):
        """Set this node's predecessor and transfer keys"""
        self.predecessor = np
        if ENABLE_JOIN_WITH_DATA and self != np:
            mr = ModRange(np.id + 1, self.id + 1, NODES)
            moved_keys = []
            for k, v in self.keys.items():
                if k not in mr:
                    moved_keys.append(k)
                    self.call_rpc(np, Method.INSERT, k, v)
            if moved_keys:
                print(color(CYAN, f'> MOVED KEYS {sorted(moved_keys)} TO NODE {np}'))
                for k in moved_keys:
                    del self.keys[k]
                    
    def find_successor(self, id):
        """ Ask this node to find id's successor = successor(predecessor(id))"""
        np = self.find_predecessor(id)
        return self.call_rpc(np, Method.GET_SUCCESSOR)

    def find_predecessor(self, id: int):
        """Find the predecessor of id"""        
        np = self
        mr = ModRange(np.id+1, self.successor.id+1, NODES)
        while id not in mr:
            print(color(CYAN, f'> ID {id} NOT IN RANGE {mr}'))
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
            print(color(CYAN, f'> NODE TO BE UPDATED {p}[{i}] → {self}'))
            # self might become p's i-th finger node (successor)
            self.call_rpc(p, Method.UPDATE_FINGER_TABLE, self, i)

    def update_finger_table(self, s: BaseNode, i: int):
        """ if s is i-th finger of n, update this node's finger table with s """        
        if (self.finger[i].start != self.finger[i].node
                 and s.id in ModRange(self.finger[i].start, self.finger[i].node.id, NODES)):
            self.finger[i].node = s
            p = self.predecessor
            self.call_rpc(p, Method.UPDATE_FINGER_TABLE, s, i)
            
            print('> UPDATED FINGER TABLE', self.pr_finger())
            
    def call_rpc(self, np: BaseNode, method: Method, arg1=None, arg2=None):
        """Call a remote procedure on node n"""
        if np == self:
            return self.dispatch_rpc(method, arg1, arg2)
            
        DISPLAY_CALL_RPC and print('{} C-RPC: {} {} {} {}, {} {}'.format(
            self.pr_now(), self.id, color(GREEN, "SEND"), np.id, method.value, 
            str(arg1)[:DIS_LEN] or "", str(arg2)[:DIS_LEN] or ""))
        
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            address = np.address
            server.connect(address)
            self.send(server, (method, arg1, arg2))
                        
            # close write side, wait for read    
            server.shutdown(socket.SHUT_WR)  
            data = self.receive(server)
            
            DISPLAY_CALL_RPC and print('{} C-RPC: {} {} {} {}, {}'.format(
                self.pr_now(), self.id, color(WARNING, "RECV"), np.id, 
                method.value, str(data)[:DIS_LEN] or ""))
            
            server.close()
            return data  
          
    def handle_rpc(self, client):
        """Handle a single RPC request"""
        method, arg1, arg2 = self.receive(client)
        
        DISPLAY_HANDLE_RPC and print('{} H-RPC: {} {} {}, {} {}'.format(
            self.pr_now(), self.id, color(WARNING, "RECV"), method.value,
            str(arg1)[:DIS_LEN] or "", str(arg2)[:DIS_LEN] or ""
        ))
        data = self.dispatch_rpc(method, arg1, arg2)
        self.send(client, data)
        
        DISPLAY_HANDLE_RPC and print('{} H-RPC: {} {} {}, {}'.format(
            self.pr_now(), self.id, color(GREEN, "SEND"), method.value,
            str(data)[:DIS_LEN] or ""
        ))
        
        client.close()
    
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
            return self.set_predecessor(arg1)
        elif method.value == Method.INSERT.value:
            return self.insert(arg1, arg2)
        elif method.value == Method.QUERY.value:
            return self.query(arg1)
        else:
            return { 'status': 'ERROR', 'message': 'Unknown method {}'.format(method)}
     
    def run(self, port=None):
        """Start the node"""
        threading.Thread(target=self.serve, args=(self.handle_rpc,)).start()
        threading.Thread(target=self.join, args=(port,)).start()
            
    def pr_finger(self):
        """ Print the finger table """
        row = '{:>6} | {:<10} | {:<6}\n'
        header = '{:>6}   {:<10}   {:<6}\n'
        text = '\n========================\n'
        text += f'FINGER TABLE\n'
        text += header.format('start', 'int.', 'succ.')
        for i in range(1, M+1):
            start = str(self.finger[i].start)
            interval = '[{},{})'.format(self.finger[i].start, self.finger[i].next_start)
            succ = str(self.finger[i].node)
            text += row.format(start, interval, succ)
        text += 'PREDECESSOR: {}\n'.format(self.predecessor)
        text += 'SUCCESSOR: {}\n'.format(self.successor)
        text += 'SELF {}\nPORT: {}\n'.format(self, self.address[1])
        text += 'KEYS: [ '
        for k in sorted(list(self.keys.keys())):
            text += str(k) + ', '
        text += ']\n========================\n'
        return text    

if __name__ == '__main__':
    if len(sys.argv) not in range(2, 4):
        num = 6
        port = 5000
        print('Usage: python chord.py <port> <num>')
        print('<port>: port number of a node in network to contact upon joining')
        print('\tuse 0 to create a new network')
        print('<num>: (optional) number added to TEST_BASE ({}) to provide specific port to use'.format(TEST_BASE))
        print('\tdefault: 0, use system generated port number')
        print('Example: python chord.py {} {}'.format(port, num))
        print('\t join network, contacting port {}, listening on port {} ({} + {}) '.format(port, TEST_BASE+num, TEST_BASE, num))
        sys.exit(1)
    
    num, port = 0, 0
    if len(sys.argv) >= 2:
        port = int(sys.argv[1]) 
    if len(sys.argv) == 3:
        num = int(sys.argv[2])
          
    node = ChordNode(num) # create a new node server
    node.run(port)         