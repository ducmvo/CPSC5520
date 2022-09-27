# Echo client program
import socket
import sys
import pickle

if len(sys.argv) != 3:
    print("Usage: python client.py HOST PORT")
    exit(1);
    
host = sys.argv[1]
port = int(sys.argv[2])
msg = 'JOIN'

def connect(host, port, msg):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        picklestring = pickle.dumps(msg)
        s.sendall(picklestring)
        data = s.recv(1024)
        response = pickle.loads(data)
        return response;  
    
# Join Group Coordinator Daemon (GCD) server
print(f'JOIN {(host, port)}')
group = connect(host, port, msg)

msg = 'HELLO'
for member in group:
    host = member.get('host')
    port = member.get('port')
    print(f'{msg} to {member}') 
    try: 
        response = connect(host, port, msg)
        print(response)
    except ConnectionRefusedError as e:
        print(f'fail to connect: {e}')





