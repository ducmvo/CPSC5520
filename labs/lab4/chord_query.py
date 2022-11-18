"""
CPSC-5520, Seattle University
LAB4 - Distributed System
CHORD QUERY IMPLEMENTATION

Chord System described in the original paper 
from 2001 by Stoica, Morris, Karger,  Kaashoek, and Balakrishna
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
from chord_node import BaseNode as Node, NodeServer as Server, Method

ENABLE_CLI = True  # enable command line arguments for port and key
ENABLE_QUERY_INPUT = True  # user interface for querying with known key
   
class ChordQuery:
    """
    Chord Query used to contact an existing node and query for a key-value pair.
    Notation: 
        C-RPC: Call RPC
        H-RPC: Handle RPC
        P-RPC: Populate RPC
        Q-RPC: Query RPC
    """
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
        """Run the query UI program, let user input port and key repeatedly"""
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
        print('\t <port>: port of a peer to contact')
        print('\t <key>: key to query')
        print('Example: python chord_query.py 43555 random-key-that-will-be-hashed')
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