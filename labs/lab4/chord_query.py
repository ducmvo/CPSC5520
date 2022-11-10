"""
chord_query takes a port number of an existing node and a key 
(any value from column 1+4 of the file)
"""
import sys
import socket
import pickle
from hashlib import sha1
from chord_node import NODES, Method

BUF_SZ = 4096
ENABLE_CLI = True
ENABLE_QUERY_INPUT = True
   
class ChordQuery:
    def query(self, port, key, method=Method.QUERY):
        """Query the chord network via an existing node"""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            address = ('localhost', int(port))
            
            try:
                server.connect(address)
            except ConnectionRefusedError:
                print('> Error: Connection refused, node may not be running')
                sys.exit(1)
                
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
            
            if not self.validate(port, key):
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
    
    if ENABLE_CLI and len(sys.argv) < 3:
        print('Usage: python chord_query.py <port> <key>')
        sys.exit(1)
    elif ENABLE_CLI:
        port = int(sys.argv[1])
        key = ' '.join(sys.argv[2:])
    cq = ChordQuery()
    
    if not cq.validate(port, port_only=True):
        sys.exit(1)
        
    value = cq.query(port, key)
    print(f'> KEY: {key}')
    print(f'> VALUE: {value} \n')
    
    # run user interface
    ENABLE_QUERY_INPUT and cq.run()