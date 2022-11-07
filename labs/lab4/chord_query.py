"""
chord_query takes a port number of an existing node and a key 
(any value from column 1+4 of the file)
"""

"""
chord_populate takes a port number of an existing node and the filename 
of the data file
"""
from enum import Enum
from hashlib import sha1
import socket
from chord_finger import NODES
import pickle

BUF_SZ = 4096

class Method(Enum):
    QUERY = 'QUERY'
        
    def is_data_signal(self):
        return self in (Method.POPULATE, Method.INSERT)
   
class ChordQuery:
    def __init__(self, port, key):
        val = self.query(port, self.hash(key))
        print(val)
        
    def query(self, port, key, method=Method.QUERY):
        """Query the chord network via an existing node"""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            address = ('localhost', port)
            server.connect(address)
            self.send(server, (method, key, None))
            res = self.receive(server)
            server.close()
            return res

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
    cq = ChordQuery(port, 'vodangminhduc')
    repeat = True
    while repeat:
        port = input('Enter port number of an existing node: ')
        key = cq.hash(input('Enter key to query: '))
        if not port or not key:
            print('Invalid input, port, key, must be non-empty')
            continue
        value = cq.query(int(port), key)
        print('Value:', value)
        repeat = input('Continue? (y/n): ')
        if repeat not in ('y', 'Y', ''):
            repeat = False