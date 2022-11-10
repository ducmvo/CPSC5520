"""
chord_query takes a port number of an existing node and a key 
(any value from column 1+4 of the file)
"""
import sys
import socket
from chord_node import BaseNode as Node, NodeServer as Server, Method

ENABLE_CLI = True  # enable command line arguments for port and key
ENABLE_QUERY_INPUT = True  # user interface for querying with known key
   
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
            
            print(Server.pr_now(), 'Q-RPC: {} SEND {}'.format('QUERY', (method.value, key)))
            
            Server.send(server, (method, Node.hash(key), None))
            res = Server.receive(server)

            print(Server.pr_now(), 'Q-RPC: {} RECV {}'.format('QUERY', (method.value, key))) 

            server.close()
            return res.get('message')
    
    def run(self):
        repeat = True
        while repeat:
            # user input
            port = input('> Enter port number of an existing node: ')
            key = input('> Enter key to query: ')
            
            if not Node.validate(port, key):
                continue
            
            # query
            value = cq.query(port, key)
            print(f'> KEY: {key}')
            print(f'> VALUE: {value} \n')
            repeat = input('Continue? (y/n): ')
            print('\n')
            if repeat not in ('y', 'Y', ''):
                repeat = False
            
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
    
    if not Node.validate(port, port_only=True):
        sys.exit(1)
        
    value = cq.query(port, key)
    print(f'> KEY: {key}')
    print(f'> VALUE: {value} \n')
    
    # run user interface
    ENABLE_QUERY_INPUT and cq.run()