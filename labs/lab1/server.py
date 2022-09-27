# Echo server program
import socket
import sys

if len(sys.argv) != 2:
    print('Usage: python server.py PORT');
    exit(1);

port = int(sys.argv[1]);
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind(('', port))
    s.listen(1)
    conn, addr = s.accept()
    with conn:
        print('Connected by', addr)
        while True:
            data = conn.recv(1024)
            if not data: break
            conn.sendall(data)