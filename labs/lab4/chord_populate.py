"""
chord_populate takes a port number of an existing node and the filename 
of the data file
"""
import csv
from enum import Enum
from hashlib import sha1
import socket
# from chord_node import Method
from chord_finger import NODES
import pickle

# from chord_finger import M, NODES
# M = 3 # sha1().digest_size * 8  # number of bits in the identifier space
# NODES = 2**M  # address space is 2^M = 8 nodes
BUF_SZ = 4096

class Method(Enum):
    POPULATE = 'POPULATE'
    INSERT = 'INSERT'
        
    def is_data_signal(self):
        return self in (Method.POPULATE, Method.INSERT)
   

class ChordPopulate:
    def __init__(self, port, filename):
        self.data = self.parse(filename)
        self.populate(port, self.data)
    
    def populate(self, port, data=None, method=Method.POPULATE):
        """Populate the chord ring via an existing node"""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            address = ('localhost', port)
            server.connect(address)
            # signal server to send large data in chunks
            self.send(server, (method, None, None))
            res = self.receive(server)
            # server status (ready)
            print('> RESPONSE: {}'.format(res))  
            self.send(server, data)
            # close write side, wait for read
            server.shutdown(socket.SHUT_WR)  
            res = self.receive(server)
            # server status (done)
            print('> RESPONSE: {}'.format(res))  
            server.close()
        
    def parse(self, filename):
        """Parse data from file"""
        data = {}
        with open(filename, 'r') as f:
            reader = csv.reader(f)
            header = next(reader)
            for row in reader:
                id = row[0]+row[3]
                key = self.hash(id)
                data[key] = row
        data = dict(sorted(data.items()))
        return data

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
    port = 43555
    filename = 'Career_Stats_Passing.csv'
    cp = ChordPopulate(port, filename)
    repeat = True
    while repeat:
        port = input('Enter port number of an existing node: ')
        key = cp.hash(input('Enter key to insert: '))
        value = input('Enter data to insert: ')
        if not port or not key or not value:
            print('Invalid input, port, key, and value must be non-empty')
            continue
        cp.populate(int(port), (key,value), method=Method.INSERT)
        repeat = input('Continue? (y/n): ')
        if repeat not in ('y', 'Y', ''):
            repeat = False

def match(cp):
    s = set()
    d = {}
    num = 1
    while len(s) < NODES:
        node = cp.hash('127.0.0.1', 43544+num)
        if node not in s:
            s.add(node)
            d[node] = num
        num += 1
        
    d = dict(sorted(d.items()))
    for k,v in d.items():
        print('node', k, 'num', v)