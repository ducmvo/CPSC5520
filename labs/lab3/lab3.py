import socket
import fxp_bytes_subscriber
from datetime import datetime
import bellman_ford
import math
from enum import Enum

BUF_SZ = 4096  # Buffer size to send or receive data
PUBLISHER_ADDRESS = ('localhost', 50403)
QUOTE_EXPIRATION = 1.5  #
WAITING_TIMEOUT = 5  # seconds not receive message timeout


class ForexSubscriber:
    def __init__(self):
        self.subscriber, self.subscriber_address = self.start_a_server()
        self.graph = {}
        self.bf = bellman_ford.BellmanFord()
        self.timestamps = {}
        self.rates = {}

    def run(self):
        self.subscribe()
        while True:
            try:
                message = self.subscriber.recv(4096)
            except socket.timeout:
                print('> SUBSCRIBER TIMEOUT, SHUTTING DOWN...')
                self.subscriber.close()
                exit(0)
            else:
                data = fxp_bytes_subscriber.unmarshal_message(message)
                self.update_graph(data)
                self.check_stale_quotes()

                dist, prev, neg_edge = self.bf.shortest_paths('USD', tolerance=0.01)
                if neg_edge is not None:
                    cycle = self.get_arbitrage_path(neg_edge, prev)
                    self.exchange(cycle)

    @staticmethod
    def get_arbitrage_path(edge, prev):
        path = [*edge]
        curr = path[1]
        while prev[curr] not in path:
            path.append(prev[curr])
            curr = prev[curr]
        path.append(prev[curr])

        index = path.index(prev[curr])
        return path[index:][::-1]

    def exchange(self, path):
        print('> ARBITRAGE:')
        print('> PATH', path)
        amount = 100
        print('\t START WITH {} {}'.format(path[0], amount))
        for i in range(len(path) - 1):
            u, v = path[i], path[i+1]
            rate = self.rates[(u, v)]
            print('\t EXCHANGE {}/{} @ {:.7f} → {} {:.2f}'.format(u, v, rate, v, amount * rate))
            amount *= rate

    def check_stale_quotes(self):
        stales = []
        for ts in [*self.timestamps]:
            if (datetime.utcnow() - self.timestamps[ts]).total_seconds() > QUOTE_EXPIRATION:
                u, v = ts
                del self.graph[u][v]
                del self.graph[v][u]
                del self.timestamps[ts]
                del self.rates[(u, v)]
                del self.rates[(v, u)]
                stales.append(f'{u}/{v}')
        if len(stales) != 0:
            print('> STALE QUOTES', stales)

    def update_graph(self, data):
        # TODO: Check correct order of timestamp before update graph
        for quote in data:
            u, v = quote['cross'].split('/')
            rate = quote['price']
            ts = quote['timestamp']

            if u not in self.graph:
                self.graph[u] = {}
            if v not in self.graph:
                self.graph[v] = {}

            if u not in self.graph[v]:
                self.graph[v][u] = {}

            if v not in self.graph[u]:
                self.graph[u][v] = {}

            if (u, v) in self.timestamps and (ts - self.timestamps[(u, v)]).total_seconds() < 0:
                print("{} {} {} {} \033[93m[OUT-OF-SEQUENCE]\033[0m".format(self.pr_time(ts), u, v, rate))
                continue

            print("{} {} {} {:.7f}".format(self.pr_time(ts), u, v, rate))
            self.timestamps[(u, v)] = ts
            self.graph[u][v] = -1 * math.log(rate)
            self.graph[v][u] = math.log(rate)
            self.rates[(u, v)] = rate
            self.rates[(v, u)] = 1/rate

        self.bf.update_graph(self.graph)

    def subscribe(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            message = fxp_bytes_subscriber.serialize_address(self.subscriber_address)
            self.send(sock, message, PUBLISHER_ADDRESS)

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
    def send(sock, message, address=PUBLISHER_ADDRESS, buffer_size=BUF_SZ):
        """Serialized and send all data using passed in socket"""
        sock.sendto(message, address)

    @staticmethod
    def receive(sock, buffer_size=BUF_SZ):
        """Receive raw data from a passed in socket
        :return: deserialized data
        """
        return sock.recv(buffer_size)

    @staticmethod
    def pr_time(timestamp):
        """Print timestamp in H:M:S.f format"""
        return timestamp.strftime('%y-%m-%d %H:%M:%S.%f')


if __name__ == '__main__':
    lab3 = ForexSubscriber()
    lab3.run()
