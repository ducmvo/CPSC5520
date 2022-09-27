# Echo client program
import socket
import sys

if len(sys.argv) != 3:
    print("Usage: python client.py HOST PORT")
    exit(1);
    
host = sys.argv[1]
port = int(sys.argv[2])
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((host, port))
    s.sendall(b'Hello, world')
    data = s.recv(1024)
print('Received', repr(data))