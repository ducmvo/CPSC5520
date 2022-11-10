"""
chord_populate takes a port number of an existing node and the filename 
of the data file
"""
import sys
import csv
import socket
import pickle
from hashlib import sha1
from chord_node import NODES, Method

BUF_SZ = 4096
ENABLE_CLI = True
ENABLE_INSERT = True

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
                
            self.send(server, (method, None, None)) # signal to populate/insert
            res = self.receive(server) # receive confirmation
            self.send(server, data)  # send data in chunks
            server.shutdown(socket.SHUT_WR)   # close write side, wait for read
            res = self.receive(server) # receive status
            server.close()
            return res
    
    def insert(self, port, key=None, value=None):
        if key is None or value is None:
            return
        self.populate(port, (self.hash(key), value), Method.INSERT)
        
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

    def run(self):
        repeat = True
        while repeat:
            # user input
            port = input('Enter port number of an existing node: ')
            key = input('Enter key to insert: ')
            value = input('Enter data to insert: ')
            
            if not self.validate(port, key, value):
                continue
            
            # insertion
            self.insert(port, key, value)
            repeat = input('Continue? (y/n): ')
            if repeat not in ('y', 'Y', ''):
                repeat = False
    
    @staticmethod         
    def validate(port, key=None, value=None, port_only=False):
        if not port:
            print('Error: Invalid input, port must be non-empty')
            return False
        
        if  not port_only and (not key or not value):
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
    filename = 'Career_Stats_Passing.csv'
    
    if ENABLE_CLI and len(sys.argv) != 3:
        print('Usage: python chord_populate.py <port> <filename>')
        sys.exit(1)
    
    if ENABLE_CLI:
        port = sys.argv[1]
        filename = sys.argv[2]
   
    cp = ChordPopulate()
    data = cp.parse(filename)
    
    if not cp.validate(port, port_only=True):
        sys.exit(1)
        
    status = cp.populate(port, data)
    print('> RESPONSE: {}'.format(status))
    
    # run user interface
    ENABLE_INSERT and cp.run()