# Echo client program
import socket
import sys
import pickle

if len(sys.argv) != 3:
    print("Usage: python client.py HOST PORT")
    exit(1);
    
host = sys.argv[1]
port = int(sys.argv[2])
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((host, port))
    picklestring = pickle.dumps('JOIN')
    s.sendall(picklestring)
    data = s.recv(1024)
    res = pickle.loads(data)
    for r in res:
        print('HELLO to ', repr(r))
        s.connect((r.get('host'), r.get('port')))
        s.sendall(picklestring)