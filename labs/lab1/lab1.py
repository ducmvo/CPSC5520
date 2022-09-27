# Echo client program
import socket
import sys
import pickle

if len(sys.argv) != 3:
    print("Usage: python client.py HOST PORT")
    exit(1);
    
host = sys.argv[1]
port = int(sys.argv[2])

def connect(host, port, msg):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        picklestring = pickle.dumps(msg)
        s.sendall(picklestring)
        data = s.recv(1024)
        res = pickle.loads(data)
        return res;  
res = connect(host, port, 'JOIN')
for server in res:
    host = server.get('host')
    port = server.get('port')
    print('Hello to ', server) 
    try: 
        print(connect(host, port, 'HELLO'))
    except ConnectionRefusedError as e:
        print('fail to connect: ', e)





