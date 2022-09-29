"""
CPSC-5520, Seattle University
This is lab1 assignment of Distributed System class
:Authors: Duc Vo
:Version: 1
"""

import socket
import sys
import pickle


class SimpleClient:
    """
    This implement a simple client that connects to a Group Coordinator Daemon (GCD)
    which will respond with a list of potential group members.
    This client then sends a message to each of the group members,
    prints out their response, and then exits.
    """
    BUF_SZ = 1024

    def __init__(self, server_address):
        """Initialize server address and group members
        server_address -- GCD host and port number
        group -- return list of members after connecting to GCD
        """
        self.server_address = server_address
        self.group = []

    @staticmethod
    def __message(server_address, message, buf_sz=BUF_SZ):
        """
        Create a socket to connect to server then send a message
        Return the server message

        Keyword arguments:
            server_address -- tuple of host and port number
            message -- string message send to server
            buf_sz -- buffer size
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1.5)
        sock.connect(server_address)
        pickled_msg = pickle.dumps(message)
        sock.sendall(pickled_msg)
        data = sock.recv(buf_sz)
        return pickle.loads(data)

    def join(self, message):
        """Connect tp GCD server with a message to retrieve group member addresses
        Return list of member addresses
        """
        print(message, self.server_address)
        try:
            self.group = self.__message(self.server_address, message)
        except (socket.timeout, ConnectionError, OSError, pickle.PickleError) as e:
            print('Error: ', e)
        return self.group

    def meet(self, message):
        """Connect and send a message to each of the group member
        then print out their response"""
        for member in self.group:
            print(f'{message} to', member)
            host, port = member.values()
            member_address = (host, port)
            try:
                res = self.__message(member_address, message)
                print(res)
            except (socket.timeout, ConnectionError, OSError, pickle.PickleError) as e:
                print('Error: ', e)


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python3 lab1.py HOST PORT")
        exit(1)

    gcd_address = (sys.argv[1], int(sys.argv[2]))
    gcd_message, member_message = 'JOIN', 'HELLO'

    # Create a SimpleClient instance
    client = SimpleClient(gcd_address)

    # Join GCD server
    client.join(gcd_message)

    # Connect to each group member which received from GCD response
    client.meet(member_message)
