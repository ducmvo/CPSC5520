"""
chord_populate takes a port number of an existing node and the filename 
of the data file
"""
import sys
import csv
import socket
from chord_node import BaseNode as Node, NodeServer as Server, Method

ENABLE_CLI = True  # enable command line arguments for port and filename
ENABLE_INSERT = False  # user interface for inserting key-value pairs

class ChordPopulate: 
    def populate(self, port, data=None, method=Method.POPULATE):
        """Populate the chord ring via an existing node"""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            address = ('localhost', int(port))
            try:
                server.connect(address)
            except ConnectionRefusedError:
                print('> Error: Connection refused, node may not be running')
                sys.exit(1)
            print(Server.pr_now(), 'P-RPC: {} SEND {}'.format(server.getsockname()[1], method))   
            Server.send(server, (method, None, None)) # signal to populate/insert
            res = Server.receive(server) # receive confirmation
            Server.send(server, data)  # send data in chunks
            server.shutdown(socket.SHUT_WR)   # close write side, wait for read
            res = Server.receive(server) # receive status
            print(Server.pr_now(), 'P-RPC: {} RECV {}'.format(server.getsockname()[1], res)) 
            server.close()
            return res
    
    def insert(self, port, key=None, value=None):
        if key is None or value is None:
            return
        return self.populate(port, (Node.hash(key), value), Method.INSERT)
        
    def parse(self, filename):
        """Parse data from file"""
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
    
if __name__ == '__main__':
    port, filename = 43555, 'Career_Stats_Passing.csv'
    
    if ENABLE_CLI and len(sys.argv) != 3:
        print('Usage: python chord_populate.py <port> <filename>')
        sys.exit(1)
    
    if ENABLE_CLI:
        port = sys.argv[1]
        filename = sys.argv[2]
   
    cp = ChordPopulate()
    data = cp.parse(filename)
    
    
    if not Node.validate(port, port_only=True):
        sys.exit(1)
    
    # Method 1: insert each key-value pair one at a time
    for k, v in data.items():
        status = cp.insert(port, k, v)
    
    # Method 2: send all key-value pairs at once
    # status = cp.populate(port, data)
    
    # run user interface
    ENABLE_INSERT and cp.run()