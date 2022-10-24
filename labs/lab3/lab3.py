"""
CPSC-5520, Seattle University
LAB3 - Distributed System
Forex Subscriber Implementation

FOREIGN EXCHANGE MARKETS
Efficient liquid markets in exchanging one country's currency for another

PRICE FEED
All the forex prices available are coming from a single publisher, Forex Provider
format: <timestamp, currency 1, currency 2, exchange rate>
Bytes[0:8] - timestamp is a 64-bit integer number of microseconds since EPOCH, big-endian.
Bytes[8:14] - currency names 3-character ISO ('USD', 'GBP', 'EUR', etc.), 8-bit ASCII from left to right.
Bytes[14:22] - exchange rate, 64-bit floating point number, IEEE 754 binary64 little-endian format.
Bytes[22:32] - reserved, not currently used (all set to 0-bits).

ARBITRAGE
Represent as a negative-weight cycle detected using Bellman-Ford shortest-path algorithm.
Weigh of edges for currency 1 and currency 2 e.g. (c1,c2,-log(rate)) | (c2,c1,log(rate))
Generated negative weigh cycle paths are used to execute trade for profit

:Authors: Duc Vo
:Version: 1
:Date: 10/24/2022
"""

import sys
import socket
import math
from datetime import datetime
from bellman_ford import BellmanFord as BellmanFordMixin
import fxp_bytes_subscriber as fbs

BUF_SZ = 4096  # Buffer size to send or receive data
PUBLISHER_ADDRESS = ('localhost', 50403)  # address of publisher
QUOTE_EXPIRATION = 1.5  # quote time-to-live (sec)
WAITING_TIMEOUT = 5  # listener inactive timeout (sec)


class ForexSubscriber(BellmanFordMixin):
    """
    This class implement a process that listens to currency exchange rates from a price feed
    and prints out a message whenever there is an arbitrage opportunity available.
    """
    def __init__(self, p_addr):
        """Initialize subscriber server and a price graph
        subscriber, subscriber_address - listening socket and address
        graph - nested key-value data structure that store weights (-log(rate) or log(rate)) of vertices (cross)
        rates - key-value for cross and price
        timestamps - key-value for cross and timestamp
        :param p_addr: Forex Provider address
        """
        self.provider_address = p_addr
        self.graph = {}
        self.rates = {}
        self.timestamps = {}
        self.subscriber, self.subscriber_address = self.start_a_server()

    def run(self):
        """
        Run the Forex Subscribe process by subscribe to Foreign Exchange Provider
        then listen to quotes messages. Process will then unmarshall raw message
        and execute Bellman-Ford shortest-path algorithm to find arbitrage opportunity.
        Process will automatically shut down if inactive for predefined timeout duration
        """
        self.subscribe(self.provider_address)
        while True:
            try:
                message = self.subscriber.recv(4096)
            except socket.timeout:
                print('> SUBSCRIBER TIMEOUT, SHUTTING DOWN...')
                self.subscriber.close()
                exit(0)
            else:
                data = fbs.unmarshal_message(message)
                self.update(data)
                self.remove()
                #  Find the shortest paths with BellmanFord algorithm
                dist, prev, neg_edge = self.shortest_paths('USD', tolerance=0.0001)
                if neg_edge is not None:
                    self.trade(neg_edge, prev)

    def trade(self, edge, prev):
        """
        Execute arbitrage trading via generated path and print out result
        :param edge: An edge of a negative weight cycle generated from Bellman-Ford shortest-paths algorithm
        :param prev: predecessor table use to trace back a negative weight cycle for arbitrage path
        """
        # generate arbitrage cycle path
        path = self.trace(edge, prev)[::-1]

        # execute trading arbitrage
        principal = accumulate = 100
        display = '\tSTART WITH {} {}\n'.format(path[0], accumulate)
        for i in range(len(path) - 1):
            u, v = path[i], path[i+1]
            rate = self.rates[(u, v)]
            display += '\tTRADE {}/{} @ {:.7f} → {} {:.2f}\n'.format(u, v, rate, v, accumulate * rate)
            accumulate *= rate
        display += '\tGAIN {:.2f}%'.format(accumulate-principal)
        print('> \033[93mARBITRAGE 💰\033[0m\n\t\033[96mPATH {}\033[0m\n\033[93m{}\033[0m'.format(path, display))

    def remove(self):
        """Remove stale quotes saved that pass expiration duration"""
        stales = ''
        for ts in [*self.timestamps]:
            if (datetime.utcnow() - self.timestamps[ts]).total_seconds() > QUOTE_EXPIRATION:
                u, v = ts
                del self.graph[u][v]
                del self.graph[v][u]
                del self.timestamps[ts]
                del self.rates[(u, v)]
                del self.rates[(v, u)]
                stales += f'[{u}|{v}] '
        if stales:
            print('> \033[93mREMOVE STALE QUOTES {}\033[0m'.format(stales))

    def is_ordered(self, quote):
        """
        Check if a quote is in correct sequence by compare receive timestamp and previous saved one
        :param quote: tuple of quote information includes timestamp, symbols, and rate
        """
        ts, u, v, r = quote
        in_sequence = (u, v) not in self.timestamps or (ts - self.timestamps[(u, v)]).total_seconds() > 0
        print(self.pr_quote(ts, (u, v, r), ignore=not in_sequence))
        return in_sequence

    def update(self, data):
        """
        Update graph with received quote from Foreign Exchange Provider.
        Including check if received message is out of sequence to ignore.
        :param data: quoted information dictionary includes timestamps, symbols, and conversion rates
        """
        for quote in data:
            ts, cross, rate = quote.values()
            u, v = cross.split('/')

            if u not in self.graph:
                self.graph[u] = {}

            if v not in self.graph:
                self.graph[v] = {}

            if u not in self.graph[v]:
                self.graph[v][u] = {}

            if v not in self.graph[u]:
                self.graph[u][v] = {}

            # Check correct order of timestamp before update graph
            if self.is_ordered((ts, u, v, rate)):
                self.timestamps[(u, v)] = ts
                self.graph[u][v] = -1 * math.log(rate)
                self.graph[v][u] = math.log(rate)
                self.rates[(u, v)] = rate
                self.rates[(v, u)] = 1 / rate

    def subscribe(self, address=PUBLISHER_ADDRESS):
        """
        Subscribe to Currency Exchange Provider
        by sending UDP/IP message with listening address.
        """
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            message = fbs.serialize_address(self.subscriber_address)
            self.send(sock, message, address)

    @staticmethod
    def start_a_server():
        """Create a listening server that listening UDP/IP request
        :return: listener socket and its address in a tuple
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(WAITING_TIMEOUT)
        sock.bind(('localhost', 0))  # bind socket to an available port
        print(f'> SUBSCRIBER LISTENING ON {sock.getsockname()}\n')
        return sock, sock.getsockname()

    @staticmethod
    def send(sock, message, address=PUBLISHER_ADDRESS):
        """Send marshalled messaged using passed in socket
        :param sock: socket to send raw data
        :param message: marshalled message to be sent
        :param address: address to send to
        """
        sock.sendto(message, address)

    @staticmethod
    def receive(sock, buffer_size=BUF_SZ):
        """Receive raw data from a passed in socket
        :param sock: socket to receive raw data
        :param buffer_size: receive buffer size of message
        :return: unmarshalled message
        """
        return sock.recv(buffer_size)

    @staticmethod
    def pr_quote(timestamp: datetime, data: tuple, ignore=False):
        """Print quote with timestamp in H:M:S.f format
        :param timestamp: a datetime timestamp of received quote
        :param data: tuple of two currency symbols and rate
        :param ignore: if tru include color and mark ignore
        """
        ts = timestamp.strftime('%y-%m-%d %H:%M:%S.%f')
        u, v, r = data
        if ignore:
            return "\033[91m{}\033[0m {} {} {:.7f}\tIGNORE".format(ts, u, v, r)
        return "{} {} {} {:.7f}".format(ts, u, v, r)


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python3 lab3.py PROVIDER_HOST PROVIDER_PORT")
        exit(1)
    provider_addr = (sys.argv[1], int(sys.argv[2]))
    lab3 = ForexSubscriber(provider_addr)
    lab3.run()
