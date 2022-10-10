import selectors
import sys
import socket
import pickle
from datetime import datetime
from enum import Enum

BUF_SZ = 1024
PEER_DIGITS = 100
CHECK_INTERVAL = 1
ASSUME_FAILURE_TIMEOUT = 2


class State(Enum):
    """
    Enumeration of states a peer can be in for the Lab2 class.
    """
    QUIESCENT = 'QUIESCENT'  # Erase any memory of this peer

    # Outgoing message is pending
    SEND_ELECTION = 'ELECTION'  # Keep connection to wait for response
    SEND_VICTORY = 'COORDINATOR'  # One way
    SEND_OK = 'OK'  # One way

    # Incoming message is pending
    WAITING_FOR_OK = 'WAIT_OK'  # When I've sent them an ELECTION message
    WAITING_FOR_VICTOR = 'WHO IS THE WINNER?'  # This one only applies to myself
    WAITING_FOR_ANY_MESSAGE = 'WAITING'  # When I've done an accept on their connect to my server

    def is_incoming(self):
        """Categorization helper."""
        return self not in (State.SEND_ELECTION, State.SEND_VICTORY, State.SEND_OK)


class Peer:
    def __init__(self, gcd_address, next_birthday, su_id):
        self.gcd_address = (gcd_address[0], int(gcd_address[1]))
        days_to_birthday = (next_birthday - datetime.now()).days
        self.pid = (days_to_birthday, int(su_id))
        self.members = {}
        self.states = {}
        self.bully = None
        self.selector = selectors.DefaultSelector()
        self.listener, self.listener_address = self.start_a_server()

    def run(self):
        self.join_group()
        self.start_election('NEW PEER')

        self.selector.register(self.listener, selectors.EVENT_READ)
        while True:
            events = self.selector.select(CHECK_INTERVAL)
            for key, mask in events:
                if key.fileobj == self.listener:
                    self.accept_peer()
                elif mask & selectors.EVENT_READ:
                    self.receive_message(key.fileobj)
                elif mask & selectors.EVENT_WRITE:
                    self.send_message(key.fileobj)
                else:
                    self.set_quiescent(key.fileobj)

            # if run out of time and NOT receive any OK
            self.check_timeouts()

    def accept_peer(self):
        """Callback for new connections"""
        peer, peer_address = self.listener.accept()
        print('ACCEPT PEER ({})'.format(peer_address))
        peer.setblocking(False)
        self.selector.register(peer, selectors.EVENT_READ)

    def join_group(self):
        print('=== JOIN GROUP ===')
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect(self.gcd_address)
            self.send(self, sock, 'JOIN', (self.pid, self.listener_address))
            data = self.receive(sock)
            self.update_members(data)

    def send_message(self, peer):  # selectors.EVENT_WRITE
        print("=== SENDING MESSAGE ===")
        state = self.get_state(peer)
        print('{}: OUTGOING {} [{}]'.format(self.pr_sock(self, peer), state.value, self.pr_now()))
        try:
            self.send(self, peer, state.value, self.members)
        except ConnectionError as e:
            # print(e)
            pass
        except Exception as e:
            # print(e)
            pass
        else:
            if state == State.SEND_ELECTION:
                self.set_state(State.WAITING_FOR_OK, peer, True)

            if state == State.SEND_OK:
                if not self.is_election_in_progress():
                    self.start_election('NOT HAVE AN ELECTION IN PROGRESS')
                self.set_quiescent(peer)

            if state == State.SEND_VICTORY:
                self.set_state(State.WAITING_FOR_ANY_MESSAGE)
                self.set_leader(self.pid)
                self.set_quiescent(peer)

    def receive_message(self, peer):  # selectors.EVENT_READ
        print("=== RECEIVE MESSAGE ===")
        state = self.get_state(peer)
        print('{}: INCOMING {} [{}]'.format(self.pr_sock(self, peer), state.value, self.pr_now()))
        try:
            data = self.receive(peer)
        except ConnectionError as e:
            print(e)
        except Exception as e:
            print(e)
        else:
            message, members = data
            print('"{}" RECEIVED'.format(message))
            if message == State.SEND_OK.value:  # received OK
                self.set_quiescent(peer)
                self.set_state(State.WAITING_FOR_VICTOR)

            if message == State.SEND_ELECTION.value:  # received ELECTION
                """
                When you receive an ELECTION message,
                1. you update your membership list with any members you didn't already know about,
                2. then you respond with the text OK.
                3. If you are currently in an election, that's all you do.
                4. If you aren't in an election, then proceed as though you are initiating a new election.
                """
                self.update_members(members)
                self.set_state(State.SEND_OK, peer, True)  # switch to write

            if message == State.SEND_VICTORY.value:  # received COORDINATOR
                bully = (0, 0)
                for pid in members:
                    if pid[0] > bully[0] or pid[0] == bully[0] and pid[1] > bully[1]:
                        bully = pid
                self.set_leader(bully)
                self.set_quiescent(peer)
                self.set_state(State.WAITING_FOR_ANY_MESSAGE)

    def check_timeouts(self):
        if self.is_expired():
            self.declare_victory('TIMEOUT WAITING FOR OK')

    @staticmethod
    def get_connection(member):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(member)
            sock.setblocking(False)
            return sock
        except Exception as e:
            print(e)
            return None

    def is_election_in_progress(self):
        return self.get_state() in (State.WAITING_FOR_VICTOR, State.WAITING_FOR_OK)

    def is_expired(self, peer=None, threshold=ASSUME_FAILURE_TIMEOUT):
        if peer is None:
            peer = self
        state, timestamp = self.get_state(peer, True)
        if state == State.WAITING_FOR_OK:
            duration = (datetime.now() - timestamp).total_seconds()
            print('wait for ok ....')
            return duration > threshold
        return False

    def set_leader(self, new_leader):
        print("== NEW LEADER == ")
        print("SELF {}".format(self.pid) if new_leader is self.pid else "OTHER {}".format(new_leader))
        self.bully = new_leader

    def get_state(self, peer=None, detail=False):
        """
        Look up current state in state table
        :param peer: socket connected to peer process (None means self)
        :param detail: if True, then the state and timestamp are both returned
        :return: either the state or (state, timestamp) depending on detail (not found gives (QUIESCENT, None))
        """
        if peer is None:
            peer = self
        if peer in self.states:
            status = self.states[peer]
        else:
            status = (State.QUIESCENT, None)
        return status if detail else status[0]

    def set_state(self, state, peer=None, switch_mode=False):
        if peer is None:
            peer = self
        self.states[peer] = (state, datetime.now())
        if switch_mode:
            event = self.selector.get_key(peer).events
            if event == selectors.EVENT_WRITE:
                self.selector.modify(peer, selectors.EVENT_READ)
            else:
                self.selector.modify(peer, selectors.EVENT_WRITE)

    def set_quiescent(self, peer=None):
        if peer is None:
            peer = self
        self.selector.unregister(peer)
        if peer in self.states:
            self.states.pop(peer)
        peer.close()

    def start_election(self, reason):
        print('=== START ELECTION ===')
        print('REASON: ', reason)

        for pid in self.members:
            if self.pid[0] < pid[0] or self.pid[0] == pid[0] and self.pid[1] < pid[1]:
                sock = self.get_connection(self.members[pid])
                if sock is None:
                    continue
                self.set_state(State.SEND_ELECTION, sock)
                self.selector.register(sock, selectors.EVENT_WRITE)

        self.set_state(State.WAITING_FOR_OK)  # Use timeouts for bully as well

    def declare_victory(self, reason):
        print('=== DECLARE VICTORY ===')
        print('REASON: ', reason)
        members = self.members
        self.members = {self.pid, self.listener_address}

        self.set_state(State.WAITING_FOR_ANY_MESSAGE)

        for pid in members:
            if pid == self.pid:
                continue
            sock = self.get_connection(members[pid])
            if sock is None:
                continue
            self.set_state(State.SEND_VICTORY, sock)
            self.selector.register(sock, selectors.EVENT_WRITE)
        self.set_leader(self.pid)
        self.members = members

    def update_members(self, their_idea_of_membership):
        self.members = {**self.members, **their_idea_of_membership}
        # for m in self.members:
        #     print(m, self.members[m])

    @staticmethod
    def start_a_server():
        print('=== START PEER SERVER ===')
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(('localhost', 0))  # bind socket to an available port
        print(f'LISTENING ON {sock.getsockname()}\n')
        sock.listen()
        sock.setblocking(False)  # set non-blocking connections
        return sock, sock.getsockname()

    @staticmethod
    def send(cls, peer, message_name, message_data=None, wait_for_reply=False, buffer_size=BUF_SZ):
        data = pickle.dumps((message_name, message_data))
        peer.sendall(data)

    @staticmethod
    def receive(peer, buffer_size=BUF_SZ):
        data = peer.recv(buffer_size)
        return pickle.loads(data)

    @staticmethod
    def pr_now():
        return datetime.now().strftime('%H:%M:%S.%f')

    @staticmethod
    def pr_sock(self, sock):
        if sock is None or sock == self or sock == self.listener:
            return 'self'
        return self.cpr_sock(sock)

    @staticmethod
    def cpr_sock(sock):
        l_port = sock.getsockname()[1] % PEER_DIGITS
        try:
            r_port = sock.getpeername()[1] % PEER_DIGITS
        except OSError:
            r_port = '???'
        return '{}->{} ({})'.format(l_port, r_port, id(sock))

    def pr_leader(self):
        if self.bully is None:
            return 'unknown'
        if self.bully == self.pid:
            return 'self'
        return self.bully


if __name__ == '__main__':
    student_id = sys.argv[1]
    gcd_addr = ('localhost', 62135)
    next_bd = datetime.fromisoformat('2023-01-28')
    # student_id = '4162581'
    peer_server = Peer(gcd_addr, next_bd, student_id)
    peer_server.run()
