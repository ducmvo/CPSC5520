"""
CPSC-5520, Seattle University
This is lab1 assignment of Distributed System class
Create a peer server to be part of a fully interconnected group
and simulate the bully algorithm. Servers will send messages to determine their states

JOIN
When starting up, contact the GCD and send a JOIN message, which is a double: (process_id, listen_address)
and whose response is a list of all the other members (some of which may now be failed).
The message content's process_id is the identity pair (days_to_birthday, SU ID)
and the listen_address is the pair (host, port) of your listening server.
The returned data structure is a dictionary of all group members,
keyed listen_address with values of corresponding process_id. Election then starts.

ELECTION
Initiated when first joining the group or whenever noticing the leader has failed.
The ELECTION message is a list of all the current (alive or failed) group members, including self
When receive an ELECTION message, update membership list with any new members, then respond OK.
If election is not in progress, initiating a new election.

COORDINATOR
The COORDINATOR message is sent by the group leader when she wins an election.
The message is the current list of processes in the group
(without regard to if they are currently working or failed).
There is no response to a COORDINATOR message.
When receive a COORDINATOR message, change state to not be election-in-progress
and update group membership list. Note the new leader.

PROBE
The PROBE message is sent occasionally to the group leader by all the other members of the group.
There is no message data. The response is the text OK.
Between each PROBE message, a random amount of time is chosen between 500 and 3000ms.

Feigning Failure
At startup and whenever recovering from a previous feigned failure,
start a timer and when the timer goes off, pretending to have failed,
then eventually recover and go back to normal operation.
The time to the next failure should be chosen each time randomly between 0 and 10000ms
and the length of failure should be chosen independently between 1000 and 4000ms.

Recovery is done by starting your listening server and initiating an election.
:Authors: Duc Vo
:Version: 1
:Date: 10/11/2022
"""

import selectors
import sys
import socket
import pickle
from datetime import datetime
from enum import Enum
import random

BUF_SZ = 1024  # Buffer size to send or receive data
PEER_DIGITS = 100  # div-mod for num digit of peer port number
CHECK_INTERVAL = 0.001  # Interval for selector loop to check for ready sockets
ASSUME_FAILURE_TIMEOUT = 1  # Duration wait for OK after send ELECTION to declare VICTORY
PROBING_DURATION = (500, 3000)  # Duration range a peer wait to probe the bully
ACTIVE_DURATION = (0, 10000)  # Duration range a peer stay in active state
INACTIVE_DURATION = (1000, 4000)  # Duration range a peer stay in inactive state

ENABLE_PROBING = True  # Enable Probing mode here
ENABLE_FEIGNING_FAILURE = True  # Enable Feigning Failure mode here


class Peer:
    """
    This implement a Peer server that connects to a Group Coordinator Daemon (GCD)
    to report it identity then receive a response with a list of peer members.
    This Peer server then sends a message to each of the group members to join the group.
    This group will be fully interconnected and will endeavor to choose leader using the bully algorithm
    prints out their response, and then exits. Choosing a leader is a form of consensus
    The leader will occasionally receive probing messages from peers to check it status.
    If failure happen, election mechanism from bully algorithm will determine the next leader.
    A Peer server can be configured to be in an active or inactive state after a preset duration
    """
    def __init__(self, gcd_address, next_birthday, su_id):
        """Initialize server address and group members
        server_address -- GCD host and port number
        group -- return list of members after connecting to GCD
        pid = (days_to_birthday, int(su_id))
        members - list of member return by gcd or updated members list from peer
        states - state of registered sockets connected to peers
        bully - the pid of leader
        selector - use to manage socket I/O
        listener, listener_address - listening socket and address
        probing_duration - random probing duration in range
        inactive_duration - random inactive duration in range
        active_duration - random active duration in range
        """
        self.gcd_address = (gcd_address[0], int(gcd_address[1]))
        self.pid = ((next_birthday - datetime.now()).days, int(su_id))
        self.members = {}
        self.states = {}
        self.bully = None
        self.selector = selectors.DefaultSelector()
        self.listener, self.listener_address = None, None
        self.probing_duration = self._pick_duration(PROBING_DURATION)
        self.inactive_duration = self._pick_duration(INACTIVE_DURATION)
        self.active_duration = self._pick_duration(ACTIVE_DURATION)

    def run(self):
        """
        Create and run a server and start a selector loop for peer to listen for ready registered socket
        Selector loop will check for any timeouts in millisecond accuracy
        """
        self.start_up(Reason.CREATE_SERVER.value)
        while True:
            events = self.selector.select(CHECK_INTERVAL)
            for key, mask in events:
                if key.fileobj == self.listener:
                    self.accept_peer()
                elif mask & selectors.EVENT_READ:
                    self.receive_message(key.fileobj)
                else:
                    self.send_message(key.fileobj)

            self.check_timeouts()
            ENABLE_PROBING and self.start_probing()
            ENABLE_FEIGNING_FAILURE and self.feign_failure()

    def start_up(self, reason):
        """
        Start a peer server for the first time or restart after a failure
        Peer server will recontact GCD, join group and start a new election
        :param reason: reason to start/restart a server
        """
        self.listener, self.listener_address = self.start_a_server()
        self.set_state(State.WAITING_FOR_ANY_MESSAGE, self.listener)
        self.join_group()
        self.start_election(reason)

    def feign_failure(self):
        """
        Occasionally feign failure and stay inactive for a random duration in predefined range
        After inactive duration passed, peer server will restart and stay active for a random
        duration in predefined active duration range
        """
        state = self.get_state()
        if (state == State.WAITING_FOR_ANY_MESSAGE or state == State.WAITING_FOR_OK) \
                and self.is_expired(threshold=self.active_duration):
            self.inactive_duration = self._pick_duration(INACTIVE_DURATION)
            print(f'> {Color.red("INACTIVE")} [{int(self.inactive_duration * 1000)}ms]')
            self.set_quiescent(self.listener)
            self.set_state(State.QUIESCENT)

        if self.get_state() == State.QUIESCENT and self.is_expired(threshold=self.inactive_duration):
            self.active_duration = self._pick_duration(ACTIVE_DURATION)
            print(f'> {Color.cyan("ACTIVE")} [{int(self.active_duration * 1000)}ms]')
            self.start_up(Reason.RESTART_SERVER.value)

    def start_probing(self):
        """
        A peer server will occasionally send probing message to the leader to check it active status
        A new election will start when leader is found out to be inactive
        """

        if self.bully and self.bully != self.pid \
                and self.get_state() == State.WAITING_FOR_ANY_MESSAGE \
                and self.is_expired(threshold=self.probing_duration):
            sock = self.get_connection(self.bully)
            if sock:
                self.set_state(State.SEND_PROBE, sock)
            else:
                self.start_election(Reason.INACTIVE_LEADER.value)

            self.set_state(State.WAITING_FOR_ANY_MESSAGE)
            self.probing_duration = self._pick_duration(PROBING_DURATION)
            print(f'> {"NEXT PROBE IN"} [{int(self.probing_duration * 1000)}ms]')

    def send_message(self, peer):
        """
        Send message is control by selector and the state of the socket.
        Predefine outgoing states will be used to register WRITE event with selector to send message
        :param peer: peer socket
        """
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
                self.set_quiescent(peer)

            if state == State.SEND_VICTORY:
                self.set_state(State.WAITING_FOR_ANY_MESSAGE)
                self.set_quiescent(peer)

            if state == State.SEND_PROBE:
                self.set_state(State.WAITING_FOR_OK, peer, True)

    def receive_message(self, peer):
        """
        Receive message is control by selector and the state of the socket.
        Predefine incoming states will be used to register WRITE event with selector to send message
        :param peer: peer socket
        """
        state = self.get_state(peer)
        switching = False
        try:
            data = self.receive(peer)
        except Exception as e:
            print(e)
        else:
            message, members = data
            print(f'{self.pr_sock(self, peer)}: RECV ← {Color.green(message)} [{self.pr_now()}]')
            if message == State.SEND_ELECTION.value:  # received ELECTION
                self.update_members(members)
                self.set_state(State.SEND_OK, peer, True)  # switch to write
                if not self.is_election_in_progress():
                    self.set_leader(None)
                    self.start_election(Reason.NO_ELECTION.value)
                switching = True

            if message == State.SEND_VICTORY.value:  # received COORDINATOR
                bully = (0, 0)
                for pid in members:
                    if pid[0] > bully[0] or pid[0] == bully[0] and pid[1] > bully[1]:
                        bully = pid
                self.update_members(members)
                self.set_leader(bully)

            if message == State.SEND_PROBE.value:
                self.set_state(State.SEND_OK, peer, True)
                switching = True

            if state == State.WAITING_FOR_OK:
                if self.get_state() != State.WAITING_FOR_ANY_MESSAGE:
                    self.set_state(State.WAITING_FOR_ANY_MESSAGE)  # stop wait ok timeout

        if not switching:
            self.set_quiescent(peer)

    def accept_peer(self):
        """
        Generate a new TCP/IP connection for incoming request
        The selector will check if ready socket is the listener to execute
        """
        peer, peer_address = self.listener.accept()
        peer.setblocking(False)
        self.set_state(State.WAITING_FOR_ANY_MESSAGE, peer)

    def join_group(self):
        """
        Join the group of peers by creating a temporary connection to a Group Coordinator Daemon (GCD)
        to report the listener identity and receive a peer members list in response
        """
        print('> JOIN GROUP')
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect(self.gcd_address)
            self.send(self, sock, 'JOIN', (self.pid, self.listener_address))
            data = self.receive(sock)
            self.update_members(data)

    def check_timeouts(self):
        """Declare Victory if timeout while waiting for victory message"""
        if self.get_state() == State.WAITING_FOR_VICTOR and self.is_expired():
            self.declare_victory(Reason.OK_TIMEOUT.value)

    def get_connection(self, member):
        """Create a connection to a peer in encapsulated members list
        :param member: pid of member in members list
        """
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
        """Check if current state of self is in an election
        :return: true if self state is waiting for victory status
        """
        return self.get_state() == State.WAITING_FOR_VICTOR

    def is_expired(self, peer=None, threshold=ASSUME_FAILURE_TIMEOUT):
        """Status of a peer stayed in it state for a duration exceed a threshold
        :return: true if duration exceed a threshold, false otherwise
        """
        if peer is None:
            peer = self
        state, timestamp = self.get_state(peer, True)
        duration = (datetime.now() - timestamp).total_seconds()
        expired = duration > threshold
        return expired

    def set_leader(self, new_leader):
        """Set new leader after election
        :param new_leader: pid of the new leader
        """
        self.bully = new_leader
        if self.bully:
            print(Color.blue(f"LEADER IS {self.pr_leader()}"))

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
        """
        Save the new state to the state table and register it with selector
        :param state: new state of the peer
        :param peer: socket connected to peer process (None means self)
        :param switch_mode: if True, switch registered event of a peer with selector.
                            Register it normally otherwise based on its state
        """
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
        """
        Unregister a peer from selector, remove from state table and close socket
        :param peer: socket connected to peer process (None means self)
        """
        if peer is None:
            peer = self
        self.selector.unregister(peer)
        if peer in self.states:
            self.states.pop(peer)
        if peer != self:
            peer.close()

    def start_election(self, reason):
        """
        Start an election to find new leader
        :param reason: Reason to start a election
        """
        print('> START ELECTION ({})'.format(reason))
        for pid in self.members:
            if self.pid[0] < pid[0] or self.pid[0] == pid[0] and self.pid[1] < pid[1]:
                sock = self.get_connection(pid)
                if sock is None:
                    continue
                self.set_state(State.SEND_ELECTION, sock)

        self.set_state(State.WAITING_FOR_VICTOR)  # Use timeouts for bully as well

    def declare_victory(self, reason):
        """
        Set self to become leader and announce with the rest of the group members
        :param reason: Reason to declare victory
        """
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
        """Update current members list with new members received from other server response"""
        self.members = {**self.members, **their_idea_of_membership}

    @staticmethod
    def start_a_server():
        """Create a listening server that accept TCP/IP request
        :return: listener socket and its address in a tuple
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(('localhost', 0))  # bind socket to an available port
        sock.listen()
        sock.setblocking(False)  # set non-blocking connections
        print(f'> PEER SERVER LISTENING ON {sock.getsockname()}\n')
        return sock, sock.getsockname()

    @staticmethod
    def send(cls, peer, message_name, message_data=None, wait_for_reply=False, buffer_size=BUF_SZ):
        """Serialized and send all data using passed in socket"""
        data = pickle.dumps((message_name, message_data))
        peer.sendall(data)

    @staticmethod
    def receive(peer, buffer_size=BUF_SZ):
        """Receive raw data from a passed in socket
        :return: deserialized data
        """
        data = peer.recv(buffer_size)
        return pickle.loads(data)

    @staticmethod
    def _pick_duration(duration_range):
        """Pick a random duration in range in milliseconds and convert it into seconds
        :param duration_range: range to pick random duration in ms
        :return: random duration in secons
        """
        lo, hi = duration_range
        val = float(random.randint(lo, hi) / 1000.0)
        return val

    @staticmethod
    def pr_now():
        """Print current time in H:M:S.f format"""
        return datetime.now().strftime('%H:%M:%S.%f')

    @staticmethod
    def pr_sock(self, sock):
        """
        Check if socket is self or others
        :return: string "SELF" or return call to cpr_sock()
        """
        if sock is None or sock == self or sock == self.listener:
            return 'SELF'
        return self.cpr_sock(sock)

    @staticmethod
    def cpr_sock(sock):
        """
        Generate port to port string with predefined number of last digit ports and socket id
        :return: formatted string represent port to port and id of passed in sock
        """
        l_port = sock.getsockname()[1] % PEER_DIGITS
        try:
            r_port = sock.getpeername()[1] % PEER_DIGITS
        except OSError:
            r_port = '???'
        return '{}->{} ({})'.format(l_port, r_port, id(sock))

    def pr_leader(self):
        """
        :return: The current leader known, self or unknown
        """
        if self.bully is None:
            return 'UNKNOWN'
        if self.bully == self.pid:
            return 'SELF'
        return self.bully


class State(Enum):
    """
    Enumeration of states a peer can be in for the Lab2 class.
    """
    QUIESCENT = 'QUIESCENT'  # Erase any memory of this peer

    # Outgoing message is pending
    SEND_ELECTION = 'ELECTION'  # Keep connection to wait for OK response
    SEND_VICTORY = 'COORDINATOR'  # Send and done
    SEND_OK = 'OK'  # Send and done
    SEND_PROBE = 'PROBE'  # Keep connection to wait for OK response

    # Incoming message is pending
    WAITING_FOR_OK = 'WAITING FOR OK'  # When I've sent them an ELECTION message
    WAITING_FOR_VICTOR = 'WHO IS THE WINNER?'  # This one only applies to myself
    WAITING_FOR_ANY_MESSAGE = 'WAITING FOR MESSAGE'  # When I've done an accept on their connect to my server

    def is_incoming(self):
        """Categorization helper."""
        return self not in (State.SEND_ELECTION, State.SEND_VICTORY, State.SEND_OK, State.SEND_PROBE)


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


if __name__ == '__main__':
    if len(sys.argv) != 5:
        print("Usage: python3 lab2.py GCD_HOST GCD_PORT NEXT_BD<yyyy-mm-dd> SU_ID<7-digits>")
        exit(1)

    gcd_host, gcd_port, next_bd, student_id = sys.argv[1:]
    gcd_addr = (gcd_host, gcd_port)
    next_bd = datetime.strptime(next_bd, '%Y-%m-%d')

    peer_server = Peer(gcd_addr, next_bd, student_id)
    peer_server.run()
