# Echo client program
import socket
import sys
import asyncio


async def timeout(sec, soc):
    while sec != 0:
        d = soc.recv(1024)
        if d:
            print('Received', repr(d))
            break
        await asyncio.sleep(1)
        sec -= 1
    raise TimeoutError('Timeout Error!!!')


if len(sys.argv) != 3:
    print("Usage: python client.py HOST PORT")
    exit(1)

host = sys.argv[1]
port = int(sys.argv[2])
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((host, port))
    s.sendall(b'Hello, world')
    asyncio.run(timeout(2, s))
