"""
chord_query takes a port number of an existing node and a key 
(any value from column 1+4 of the file)
"""

"""
chord_populate takes a port number of an existing node and the filename 
of the data file
"""
import sys
from enum import Enum
from hashlib import sha1
import socket
from chord_finger import NODES
import pickle

BUF_SZ = 4096
ENABLE_CLI = True
ENABLE_QUERY_INPUT = False

class Method(Enum):
    QUERY = 'QUERY'
    POPULATE = 'POPULATE'
    INSERT = 'INSERT'
        
    def is_data_signal(self):
        return self in (Method.POPULATE, Method.INSERT)
   
class ChordQuery:
    def query(self, port, key, method=Method.QUERY):
        """Query the chord network via an existing node"""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            address = ('localhost', int(port))
            server.connect(address)
            self.send(server, (method, self.hash(key), None))
            res = self.receive(server)

            # print('> RESPONSE: {}'.format(res))

            server.close()
            return res['message']['value']
    
    def run(self):
        repeat = True
        while repeat:
            # user input
            port = input('> Enter port number of an existing node: ')
            key = input('> Enter key to query: ')
            
            # validation
            if not port or not key:
                print('> Error: Invalid input, port, key, must be non-empty')
                continue
            try:
                int(port)
            except ValueError:
                print('> Error: Invalid input, port must be an integer')
                continue
            
            # query
            value = cq.query(port, key)
            print(f'> KEY: {key}')
            print(f'> VALUE: {value} \n')
            repeat = input('Continue? (y/n): ')
            print('\n')
            if repeat not in ('y', 'Y', ''):
                repeat = False
            

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
    key = 'random-key-that-will-be-hashed'
    
    if ENABLE_CLI and len(sys.argv) != 3:
        print('Usage: python chord_query.py <port> <key>')
        sys.exit(1)
    elif ENABLE_CLI:
        port = int(sys.argv[1])
        key = sys.argv[2]
    
    cq = ChordQuery()
    value = cq.query(port, key)
    print(f'> KEY: {key}')
    print(f'> VALUE: {value} \n')
    
    # run user interface
    ENABLE_QUERY_INPUT and cq.run()