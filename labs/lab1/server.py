# Echo server program
import socket
import sys
import asyncio
import pickle
import socketserver

import threading

threads = []


class MyThread(threading.Thread):
    @staticmethod
    async def processing():
        print('Processing ...')
        await asyncio.sleep(5)
        print('... Done!')

    def __init__(self, tid, p):
        threading.Thread.__init__(self)
        self.threadID = tid
        self.port = p
        self.server = socketserver.TCPServer(('', self.port), MyServer)

    def run(self):
        print("Listening on port ", self.port)
        self.server.serve_forever()


BUF_SZ = 1024  # tcp receive buffer size


class MyServer(socketserver.BaseRequestHandler):
    JOIN_RESPONSE = ('OK', 'Happy to meet you')

    def handle(self):
        """
        Handles the incoming messages - expects only 'HELLO' messages
        """
        raw = self.request.recv(BUF_SZ)  # self.request is the TCP socket connected to the client
        print(self.client_address)
        if self.server.server_address[1] == 23015:
            asyncio.run(MyThread.processing())

        try:
            message = pickle.loads(raw)
        except (pickle.PickleError, KeyError, EOFError):
            response = bytes('Expected a pickled message, got ' + str(raw)[:100] + '\n', 'utf-8')
        else:
            if message != 'HELLO':
                response = pickle.dumps('====== Unexpected message: ' + str(message))
            else:
                response = pickle.dumps(('OK', f'Happy to meet you, {self.client_address}'))
        self.request.sendall(response)


if __name__ == '__main__':
    if len(sys.argv) != 4:
        print("Usage: python server.py PORT PORT PORT")
        exit(1)
    threadID = 1
    for port in sys.argv[1:]:
        threads.append(MyThread(threadID, int(port)))
        threadID += 1

    # Start threads
    for t in threads:
        t.start()

    # Wait for all threads to complete
    for t in threads:
        t.join()
    print("Exiting Main Thread")
