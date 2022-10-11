import selectors
import sys
import socket
import pickle
from datetime import datetime
from enum import Enum
import random

BUF_SZ = 1024
PEER_DIGITS = 100
CHECK_INTERVAL = 0.001
ASSUME_FAILURE_TIMEOUT = 2
PROBING_DURATION = (500, 3000)
ACTIVE_DURATION = (0, 10000)  # active state
INACTIVE_DURATION = (1000, 4000)  # inactive state

ENABLE_PROBING = False
ENABLE_FEIGNING_FAILURE = False


class Color(Enum):
    """
    Enumeration of custom colors to print text.
    """
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    END = '\033[0m'

    @staticmethod
    def yellow(text):
        return f'{Color.WARNING.value}{text}{Color.END.value}'

    @staticmethod
    def green(text):
        return f'{Color.GREEN.value}{text}{Color.END.value}'

    @staticmethod
    def red(text):
        return f'{Color.FAIL.value}{text}{Color.END.value}'

    @staticmethod
    def cyan(text):
        return f'{Color.CYAN.value}{text}{Color.END.value}'

    @staticmethod
    def blue(text):
        return f'{Color.BLUE.value}{text}{Color.END.value}'


class Reason(Enum):
    """
    Enumeration of reasons for a peer action.
    """

    # Start election reason
    NEW_PEER_JOIN = 'PEER JOIN GROUP'
    INACTIVE_LEADER = 'INACTIVE LEADER'
    NO_ELECTION = 'NO ELECTION IN PROGRESS'

    # Timeout reason
    OK_TIMEOUT = 'WAIT OK TIMEOUT'
    RESTART_SERVER = 'SERVER RESTART AFTER FAILURE'
    CREATE_SERVER = 'NEW SERVER CREATED'


class State(Enum):
    """
    Enumeration of states a peer can be in for the Lab2 class.
    """
    QUIESCENT = 'QUIESCENT'  # Erase any memory of this peer

    # Outgoing message is pending
    SEND_ELECTION = 'ELECTION'  # Keep connection to wait for response
    SEND_VICTORY = 'COORDINATOR'  # One way
    SEND_OK = 'OK'  # One way
    SEND_PROBE = 'PROBE'

    # Incoming message is pending
    WAITING_FOR_OK = 'WAITING FOR OK'  # When I've sent them an ELECTION message
    WAITING_FOR_VICTOR = 'WHO IS THE WINNER?'  # This one only applies to myself
    WAITING_FOR_ANY_MESSAGE = 'WAITING FOR MESSAGE'  # When I've done an accept on their connect to my server

    def is_incoming(self):
        """Categorization helper."""
        return self not in (State.SEND_ELECTION, State.SEND_VICTORY, State.SEND_OK)
        # return self in (State.WAITING_FOR_ANY_MESSAGE, State.WAITING_FOR_OK, State.WAITING_FOR_VICTOR)

    def is_active(self):
        return self is not State.QUIESCENT


class Peer:
    def __init__(self, gcd_address, next_birthday, su_id):
        self.gcd_address = (gcd_address[0], int(gcd_address[1]))
        days_to_birthday = (next_birthday - datetime.now()).days
        self.pid = (days_to_birthday, int(su_id))
        self.members = {}
        self.states = {}
        self.bully = None
        self.selector = selectors.DefaultSelector()
        self.listener, self.listener_address = None, None
        self.probing_duration = self._pick_duration(PROBING_DURATION)
        self.inactive_duration = self._pick_duration(INACTIVE_DURATION)
        self.active_duration = self._pick_duration(ACTIVE_DURATION)

    def run(self):
        self.start_server(Reason.CREATE_SERVER.value)
        while True:
            events = self.selector.select(CHECK_INTERVAL)
            for key, mask in events:
                if key.fileobj == self.listener:
                    self.accept_peer()
                elif mask & selectors.EVENT_READ:
                    self.receive_message(key.fileobj)
                else:
                    self.send_message(key.fileobj)

            # if run out of time and NOT receive any OK
            self.check_timeouts()
            ENABLE_PROBING and self.start_probing()
            ENABLE_FEIGNING_FAILURE and self.feign_failure()

    def start_server(self, reason):
        self.listener, self.listener_address = self.start_a_server()
        # self.selector.register(self.listener, selectors.EVENT_READ)
        self.set_state(State.WAITING_FOR_ANY_MESSAGE, self.listener)
        self.join_group()
        self.start_election(reason)

    def feign_failure(self):
        state = self.get_state()
        if (state == State.WAITING_FOR_ANY_MESSAGE or state == State.WAITING_FOR_OK) \
                and self.is_failure_expired(self.active_duration):
            self.inactive_duration = self._pick_duration(INACTIVE_DURATION)
            print(f'> {Color.red("INACTIVE")} [{int(self.inactive_duration * 1000)}ms]')
            self.set_quiescent(self.listener)
            self.set_state(State.QUIESCENT)

        if self.get_state() == State.QUIESCENT and self.is_failure_expired(self.inactive_duration):
            self.active_duration = self._pick_duration(ACTIVE_DURATION)
            print(f'> {Color.cyan("ACTIVE")} [{int(self.active_duration * 1000)}ms]')
            self.start_server(Reason.RESTART_SERVER.value)

    def is_failure_expired(self, threshold):
        state, timestamp = self.get_state(self, True)
        duration = (datetime.now() - timestamp).total_seconds()
        expired = duration > threshold
        print(f'> {self.get_state().value} [{int(duration)}s]\r\033[F')
        return expired

    def start_probing(self):
        # if known leader is not self
        if self.bully and self.bully != self.pid and self.is_probing_expired(self.probing_duration):
            sock = self.get_connection(self.bully)
            # self.set_state(State.WAITING_FOR_OK, self.bully)
            if sock:
                self.set_state(State.SEND_PROBE, sock)
                # self.selector.register(sock, selectors.EVENT_WRITE)
            else:
                self.start_election(Reason.INACTIVE_LEADER.value)
            self.probing_duration = self._pick_duration(PROBING_DURATION)

    def is_probing_expired(self, threshold):
        expired = False
        state, timestamp = self.get_state(detail=True)
        # if state == State.SEND_PROBE:
        duration = (datetime.now() - timestamp).total_seconds()
        expired = duration > threshold
        if expired:
            print(f'> PROBING TIMEOUT [{int(duration * 1000)}ms]')
        return expired

    def send_message(self, peer):
        state = self.get_state(peer)
        print(f'{self.pr_sock(self, peer)}: SEND → {Color.yellow(state.value)} [{self.pr_now()}]')
        try:
            self.send(self, peer, state.value, self.members)
        except Exception as e:
            print(e)
        else:
            if state == State.SEND_ELECTION:
                self.set_state(State.WAITING_FOR_OK, peer, True)

            if state == State.SEND_OK:
                print("SWITCHED FROM RECEIVED ELECTION", id(peer))
                self.set_quiescent(peer)

            if state == State.SEND_VICTORY:
                self.set_state(State.WAITING_FOR_ANY_MESSAGE)
                self.set_quiescent(peer)

            if state == State.SEND_PROBE:
                self.set_state(State.WAITING_FOR_OK, peer, True)


    def receive_message(self, peer):
        state = self.get_state(peer)
        print(f'{self.pr_sock(self, peer)}: STATE | {Color.green(state.value)} [{self.pr_now()}]')
        try:
            data = self.receive(peer)
        except Exception as e:
            print(e)
        else:
            message, members = data
            print(f'{self.pr_sock(self, peer)}: RECV ← {Color.green(message)} [{self.pr_now()}]')

            if message == State.SEND_OK.value:  # received OK
                # self.set_state(State.WAITING_FOR_ANY_MESSAGE)
                # self.set_quiescent(peer)

                # cur_state = self.get_state()
                # if cur_state == State.WAITING_FOR_ANY_MESSAGE:
                #     # self.set_state(State.SEND_PROBE)
                #     pass
                # if cur_state == State.WAITING_FOR_VICTOR:
                #     pass
                # if cur_state == State.WAITING_FOR_OK:  # from election
                #     self.set_state(State.WAITING_FOR_VICTOR)
                # # if self.bully and self.pid != self.bully:
                # #     self.probing_duration = self._pick_duration(PROBING_DURATION)
                # #     self.set_state(State.SEND_PROBE, self.bully)

                if not self.bully:
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
                if not self.is_election_in_progress():
                    self.set_leader(None)
                    self.start_election(Reason.NO_ELECTION.value)

            if message == State.SEND_VICTORY.value:  # received COORDINATOR
                bully = (0, 0)
                for pid in members:
                    if pid[0] > bully[0] or pid[0] == bully[0] and pid[1] > bully[1]:
                        bully = pid
                self.update_members(members)
                self.set_leader(bully)
                self.set_quiescent(peer)
                self.set_state(State.WAITING_FOR_ANY_MESSAGE)

            if message == State.SEND_PROBE.value:
                self.set_state(State.SEND_OK, peer, True)

        if state == State.WAITING_FOR_OK:
            self.set_state(State.WAITING_FOR_ANY_MESSAGE)
            self.set_quiescent(peer)

    def accept_peer(self):
        """Generate a connection for incoming request"""
        peer, peer_address = self.listener.accept()
        peer.setblocking(False)
        self.set_state(State.WAITING_FOR_ANY_MESSAGE, peer)

    def join_group(self):
        print('> JOIN GROUP')
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect(self.gcd_address)
            self.send(self, sock, 'JOIN', (self.pid, self.listener_address))
            data = self.receive(sock)
            self.update_members(data)

    def check_timeouts(self):
        if self.is_expired():
            self.declare_victory(Reason.OK_TIMEOUT.value)

    def get_connection(self, member):
        if member == self.pid:
            return None

        try:
            address = self.members[member]
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(address)
            sock.setblocking(False)
            return sock
        except Exception as e:
            print(member, f'{Color.red("INACTIVE")}')
            return None

    def is_election_in_progress(self):
        return self.get_state() == State.WAITING_FOR_VICTOR

    def is_expired(self, peer=None, threshold=ASSUME_FAILURE_TIMEOUT):
        expired = False
        if peer is None:
            peer = self
        state, timestamp = self.get_state(peer, True)
        if state == State.WAITING_FOR_VICTOR:
            duration = (datetime.now() - timestamp).total_seconds()
            expired = duration > threshold
        if expired:
            print(f'> OK TIMEOUT [{int(duration * 1000)}ms]')
        return expired

    def set_leader(self, new_leader):
        self.bully = new_leader
        if self.bully:
            self.set_state(State.WAITING_FOR_ANY_MESSAGE)

        if self.bully:
            print(Color.blue(f"LEADER IS {self.pr_leader()}"))
        # if self.pid != self.bully:
        #     self.set_state(State.WAITING_FOR_ANY_MESSAGE)
        # self.set_state(State.SEND_PROBE)
        # self.set_state(State.SEND_PROBE, new_leader)

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

        # Stop if set state to self
        if peer is self:
            return

        # Register/Modify with selector after set state
        if switch_mode:
            event = self.selector.get_key(peer).events
            self.selector.modify(peer, (1 << 0) if event == (1 << 1) else (1 << 1))
        else:
            self.selector.register(peer, selectors.EVENT_READ if state.is_incoming() else selectors.EVENT_WRITE)

    def set_quiescent(self, peer=None):
        if peer is None:
            peer = self
        self.selector.unregister(peer)
        if peer in self.states:
            self.states.pop(peer)
        if peer != self:
            peer.close()

    def start_election(self, reason):
        print('> START ELECTION ({})'.format(reason))
        for pid in self.members:
            if self.pid[0] < pid[0] or self.pid[0] == pid[0] and self.pid[1] < pid[1]:
                sock = self.get_connection(pid)
                if sock is None:
                    continue
                self.set_state(State.SEND_ELECTION, sock)

        self.set_state(State.WAITING_FOR_VICTOR)  # Use timeouts for bully as well

    def declare_victory(self, reason):
        print('> DECLARE VICTORY ({})'.format(reason))
        members = {}

        for pid in self.members:
            if pid[0] < self.pid[0] or pid[0] == self.pid[0] and pid[1] <= self.pid[1]:
                members[pid] = self.members[pid]
        self.members = members

        self.set_state(State.WAITING_FOR_ANY_MESSAGE)

        for pid in self.members:
            sock = self.get_connection(pid)
            if sock is None:
                continue
            self.set_state(State.SEND_VICTORY, sock)
        self.set_leader(self.pid)

    def update_members(self, their_idea_of_membership):
        self.members = {**self.members, **their_idea_of_membership}

    @staticmethod
    def start_a_server():
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(('localhost', 0))  # bind socket to an available port
        sock.listen()
        sock.setblocking(False)  # set non-blocking connections
        print(f'> PEER SERVER LISTENING ON {sock.getsockname()}\n')
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
    def _pick_duration(duration_range):
        lo, hi = duration_range
        val = float(random.randint(lo, hi) / 1000.0)
        return val

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
    if len(sys.argv) != 5:
        print("Usage: python3 lab2.py GCD_HOST GCD_PORT NEXT_BD<yyyy-mm-dd> SU_ID<7-digits>")
        exit(1)

    gcd_host, gcd_port, next_bd, student_id = sys.argv[1:]
    gcd_addr = (gcd_host, gcd_port)
    next_bd = datetime.fromisoformat(next_bd)

    peer_server = Peer(gcd_addr, next_bd, student_id)
    peer_server.run()
