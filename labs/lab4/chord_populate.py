"""
CPSC-5520, Seattle University
LAB4 - Distributed System
CHORD POPULATE IMPLEMENTATION

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
import csv
import socket
import time
import random
import threading
from chord_query import ChordQuery
from chord_node import ChordNode as Node, NodeServer as Server
from chord_node import  Method, ENABLE_JOIN_WITH_DATA, TEST_BASE, NODES

ENABLE_CLI = True  # enable command line arguments for port and filename, disable to run tests
ENABLE_INSERT = True  # user interface for inserting key-value pairs
FILE_NAME = 'Career_Stats_Passing.csv'

class ChordPopulate: 
    """
    Chord Populate used to populate the chord ring with data via an existing node.
    It supports inserting a single key-value pair from CLI or a batch of key-value pairs from a csv file.
    Notation: 
        C-RPC: Call RPC
        H-RPC: Handle RPC
        P-RPC: Populate RPC
        Q-RPC: Query RPC
    """
    def insert(self, port, key=None, value=None):
        """Insert a key-value pair to the the chord ring via an existing node"""
        if key is None or value is None:
            return
        
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            address = ('localhost', int(port))
            try:
                server.connect(address)
            except ConnectionRefusedError:
                print('> Error: Connection refused, node may not be running')
                sys.exit(1)
            print(Server.pr_now(), 'P-RPC: {} SEND {}'.format(server.getsockname()[1], Method.INSERT))   
            Server.send(server, (Method.INSERT, Node.hash(key), value))
            server.shutdown(socket.SHUT_WR)   # close write side, wait for read
            res = Server.receive(server) # receive status
            print(Server.pr_now(), 'P-RPC: {} RECV {}'.format(server.getsockname()[1], res)) 
            server.close()
            return res
        
    def parse(self, filename):
        """Parse data from file and return a list of key-value pairs"""
        data = {}
        with open(filename, 'r') as f:
            reader = csv.reader(f)
            header = next(reader)
            for row in reader:
                id = row[0]+row[3]
                key = Node.hash(id)
                data[key] = row
        return data

    def run(self):
        """Run the UI program, let user input port and key-value repeatedly"""
        repeat = True
        while repeat:
            # user input
            port = input('Enter port number of an existing node: ')
            key = input('Enter key to insert: ')
            value = input('Enter data to insert: ')
            
            if not Node.validate(port, key, value):
                continue
            
            # insertion
            self.insert(port, key, value)
            repeat = input('Continue? (y/n): ')
            if repeat not in ('y', 'Y', ''):
                repeat = False

class ChordTest:
    """Generate a specified number of nodes and join them to the network
        Test populate data from a file
        Test query the network
        Test insert a key-value pair to the network
        Test query inserted key-value pair
        Test join a new node after data populated
        Note: not handle concurrent join, delay for each node to join
    """
    def __init__(self, num_nodes):
        self.nodes = []
        self.ports = []
        self.threads = []
        self.ids = []
        for _ in range(min(num_nodes, NODES)):
            node = self.generate_node()
            self.nodes.append(node)
            self.ports.append(node.address[1])
        
        # node join random running node
        uports = { self.ports[0] }
        for i, node in enumerate(self.nodes):
            p = self.rand_port(list(uports))
            uports.add(p)
            if i == 0: p = 0
            t = threading.Thread(target=self.serve, args=(node, p))
            self.threads.append(t)
    
    def generate_node(self):
        node = Node(0)
        # avoid node id collision for different hashed addresses
        while node.id in self.ids:
            print("COLLISION", node.id, node.address)
            node = Node(0)
        self.ids.append(node.id)
        return node
            
    def run(self):
        for t in self.threads:
            t.start()
            # delay for node to join one at a time
            # TODO: hanlde concurrent join
            time.sleep(0.05) # increase if more nodes 
        
        cq = ChordQuery()
        cp = ChordPopulate()
        test_results = ''
        
        # Test query non existing key
        res = cq.query(self.rand_port(), 'Foo')
        test_results += 'TEST 1 PASSED: {}\n'.format(res == 'KEY NOT FOUND')
        
        # Test parse data from file
        data = cp.parse(FILE_NAME)
        
        # Test insert each key-value pair to network
        for key, value in data.items():
            cp.insert(self.rand_port(), key, value)
        
        # Test query the network via random nodes
        res = cq.query(self.rand_port(), 'Foo')
        test_results += 'TEST 2 PASSED: {}\n'.format(res.get('value') != 'Bar')
        
        # Test query a known key from network
        cp.insert(self.rand_port(), 'Foo', 'Bar')
        res = cq.query(self.rand_port(), 'Foo')
        test_results += 'TEST 3 PASSED: {}'.format(res.get('value') == 'Bar')

        if ENABLE_JOIN_WITH_DATA:
            print('{0:=^40}'.format('JOIN AFTER DATA POPULATED'))
            node = self.generate_node()
            p = self.rand_port(self.ports)
            print(f'{"=":=^40}')
            threading.Thread(target=self.serve, args=(node, p)).start()
            time.sleep(0.5)
        
        # Display test results
        print('{0:=^40}'.format('RESULTS'))
        print('NODES', self.ids)
        print(test_results)
        print(f'{"=":=^40}')
                 
    def rand_port(self, ports=None):
        if ports is None:
            ports = self.ports
        return random.choice(ports)
    
    @staticmethod    
    def serve(node, port):
        node.run(port)

if __name__ == '__main__':
    port, filename = TEST_BASE, FILE_NAME
    
    if ENABLE_CLI and len(sys.argv) != 3:
        print('Usage: python chord_populate.py <port> <filename>')
        print('\t <port>: port of a peer to contact')
        print('\t <filename>: data file path to populate')
        print('Example: pythonr chord_populate.py 5000 data.csv')
        print('Note: disable CLI mode in the script to run test')
        sys.exit(1)
    
    if ENABLE_CLI: # require port and filename from command line
        port = sys.argv[1]
        filename = sys.argv[2]
    else:  # Test Chord Network automatically
        ct = ChordTest(5) # 5 random nodes with random ports
        ct.run()
        sys.exit(1)
   
    cp = ChordPopulate()
    data = cp.parse(filename)
    
    
    if not Node.validate(port, port_only=True):
        sys.exit(1)
    
    # Method 1: insert each key-value pair one at a time
    for k, v in data.items():
        status = cp.insert(port, k, v)
    
    # run user interface
    ENABLE_INSERT and cp.run()
    
     
