"""
CPSC-5520, Seattle University
LAB5 - BLOCKCHAIN
BITCOIN PROTOCOLS IMPLEMENTATION

CONNECTING TO THE NETWORK
Connect to a peer in the P2P BitCoin network and get the block number corresponding 
to the block height then display the transactions in the block.

MODIFICATION
Manipulate one of the transactions in the block to change its output account, 
then fix up the block to correctly represent this modified data 
(fix the merkle-tree hashes, etc.). 

REPORT
Show with a program-generated report how the hash of the block has changed 
and the ways in which this block would be rejected by peers in the network.
Program written in Python 3 with no use of publicly available BitCoin libraries.
Use TCP/IP to communicate with a full node in the network.

:Authors: Duc Vo
:Version: 1
:Date: 12/1/2022

"""

import sys
import socket
import threading
import hashlib
import time
from random import getrandbits
from hashlib import sha256
from time import gmtime, strftime

HDR_SZ = 24 # size of the header in bytes
BUF_SZ = 4096 # size of the buffer in bytes
RUNNING = True  # global variable to stop threads

NODE_ADDR = '8.209.105.138', 8333 # address of the node - backup
NODE_ADDR = '94.75.198.120', 8333 # address of the node - using this one

STOP_BLOCK = '00' * 32 # Stop Block Hash
GENESIS =  '6fe28c0ab6f1b372c1a6a246ae63f74f931e8365e15a089c68d6190000000000' # Genesis Block Hash

MY_PK66 =  '029ac1d823a5926279b1eabb6a84e6a614340ba59423e200005a64aeb4b40904b1' # Public Key (compressed)
MY_PK130 = '049ac1d823a5926279b1eabb6a84e6a614340ba59423e200005a64aeb4b40904b157cb478e96d5e37c8902c31cad21db601625d21e2de3f0a4b52008debb2a17e0' # Public Key


################################################################################
ENABLE_DETAIL_TRANSACTION = False # detailed transaction inputs/outputs, disable for TXIDs display only
ENABLE_DISPLAY_GETBLOCKS = False # display of getblocks messages
ENABLE_DISPLAY_INV = False # display of inv messages
ENABLE_MINNING = False # minning
ENABLE_MODIFICATION_REPORT = True # modification report
################################################################################

class Utility:
    """ Utility class provides helper functions to marshal/unmarshal bitcoin message data types """
    
    def checksum(self, b):
        """ Returns the first 4 bytes of the SHA256(SHA256(b)) """
        return self._hash(b)[:4]

    def generate_pkscript(self, script_type:str='p2pkh'):
        """ Returns the public key script for the given script type using default public key """
        OP_HASH160 = bytes.fromhex('a9')
        OP_DUP = bytes.fromhex('76')
        OP_EQUALVERIFY = bytes.fromhex('88')
        OP_CHECKSIG = bytes.fromhex('ac')
        
        data = bytes.fromhex(MY_PK130)
        pkscript = self.uint8_t(len(data)) + data + OP_CHECKSIG
        
        if script_type == 'p2pkh':
            data = self.hash160(bytes.fromhex(MY_PK66))
            pkscript = OP_DUP + OP_HASH160 + self.compactsize_t(len(data))  + data + OP_EQUALVERIFY + OP_CHECKSIG
            
        return pkscript

    def compactsize_t(self, n):
        """ Returns the compactsize_t representation of n """
        if n < 252:
            return self.uint8_t(n)
        if n < 0xffff:
            return self.uint8_t(0xfd) + self.uint16_t(n)
        if n < 0xffffffff:
            return self.uint8_t(0xfe) + self.uint32_t(n)
        return self.uint8_t(0xff) + self.uint64_t(n)

    def unmarshal_compactsize(self, b):
        """ Unmarshals a compactsize_t from the given byte array """
        key = b[0]
        if key == 0xff:
            return b[0:9], self.unmarshal_uint(b[1:9])
        if key == 0xfe:
            return b[0:5], self.unmarshal_uint(b[1:5])
        if key == 0xfd:
            return b[0:3], self.unmarshal_uint(b[1:3])
        return b[0:1], self.unmarshal_uint(b[0:1])

    def bool_t(self, flag):
        """ Returns the byte representation of boolean flag """
        return self.uint8_t(1 if flag else 0)
    
    @staticmethod
    def ipv6_from_ipv4(ipv4_str):
        """ Returns the IPv6 bytes representation of the given IPv4 address """
        pchIPv4 = bytearray([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff])
        return pchIPv4 + bytearray((int(x) for x in ipv4_str.split('.')))
    
    @staticmethod
    def ipv6_to_ipv4(ipv6):
        """ Returns the IPv4 address of the given IPv6 address """
        return '.'.join([str(b) for b in ipv6[12:]])
        
    @staticmethod
    def uint8_t(n):
        """ Returns the byte representation of the given 8-bit unsigned integer """
        return int(n).to_bytes(1, byteorder='little', signed=False)
    
    @staticmethod
    def uint16_t(n):
        """ Returns the byte representation of the given 16-bit unsigned integer """
        return int(n).to_bytes(2, byteorder='little', signed=False)
    
    @staticmethod
    def int32_t(n):
        """ Returns the byte representation of the given 32-bit signed integer """
        return int(n).to_bytes(4, byteorder='little', signed=True)
    
    @staticmethod
    def uint32_t(n):
        """ Returns the byte representation of the given 32-bit unsigned integer """
        return int(n).to_bytes(4, byteorder='little', signed=False)
    
    @staticmethod
    def int64_t(n):
        """ Returns the byte representation of the given 64-bit signed integer """
        return int(n).to_bytes(8, byteorder='little', signed=True)
    
    @staticmethod
    def uint64_t(n):
        """ Returns the byte representation of the given 64-bit unsigned integer """
        return int(n).to_bytes(8, byteorder='little', signed=False)
    
    @staticmethod
    def unmarshal_int(b):
        """ Unmarshals a signed integer from the given byte array """
        return int.from_bytes(b, byteorder='little', signed=True)
    
    @staticmethod
    def unmarshal_uint(b):
        """ Unmarshals an unsigned integer from the given byte array """
        return int.from_bytes(b, byteorder='little', signed=False)

    @staticmethod
    def hash160(b):
        """ Returns the RIPEMD160(SHA256(b)) """
        return hashlib.new('ripemd160', sha256(b).digest()).digest()

    @staticmethod
    def _hash(b):
        """ Returns the SHA256(SHA256(b)) """
        return sha256(sha256(b).digest()).digest()

class BitCoinPeer(Utility):
    """ BitCoinPeer class implements the bitcoin protocol """
    
    def __init__(self):
        """ Initializes the BitCoinPeer class by creating a TCP socket then
        establishes a TCP/IP connection to the node and exchanges the version/veract messages.
        Using a separate thread to receive messages from the remote node.
        """
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect(NODE_ADDR)
        self.host, self.port = self.sock.getsockname()
        self.latest_block = GENESIS # latest downloaded block hash
        self.block = None  # current block being processed
        self.blocks = {} # dictionary of blocks indexed by block height
        self.thread = threading.Thread(target=self.receive_message).start()
        
        # establish the connection by exchanging the version/verack messages
        self.send_message('version')
        
        # wait for connection to be established in the receive_message thread
        self.connected = False
        while not self.connected:
            continue
        
    def shutdown(self):
        """ Closes the TCP/IP connection to the remote node 
        (...by sending a 'ping' message ;) )"""
        global RUNNING
        RUNNING = False
        self.send_message('ping', self.uint64_t(getrandbits(64)))
    
    def marshall_message(self, cmd:str, payload:bytes=b''):
        """ Returns the byte representation of the given message 
        :param cmd: message command
        :param payload: message payload
        :return: byte representation of the message
        """     
        magic = bytes.fromhex('f9beb4d9')  # 4 bytes  start string char[4]
        command = (cmd + ((12 - len(cmd)) * "\00")).encode()
                
        if cmd == 'version':  
            version = self.uint32_t(70015)
            my_services = self.uint64_t(0)
            epoch_time = self.uint64_t(int(time.time()))
            your_services = self.uint64_t(1)
            rec_host = self.ipv6_from_ipv4(NODE_ADDR[0])
            rec_port = self.uint16_t(NODE_ADDR[1])
            my_services2 = self.uint64_t(0)
            my_host = self.ipv6_from_ipv4(self.sock.getsockname()[0])
            my_port = self.uint16_t(self.sock.getsockname()[1])
            nonce = self.uint64_t(0)
            user_agent_size = self.uint8_t(0)
            user_agent = b''
            start_height = self.uint32_t(0)
            relay = self.bool_t(False)
            payload = (version + my_services + epoch_time + your_services + rec_host 
                    + rec_port + my_services2 + my_host + my_port + nonce 
                    + user_agent_size + user_agent + start_height + relay)
        
        payload_size = self.uint32_t(len(payload)) 
        cksum = self.checksum(payload)
        header = magic + command + payload_size + cksum
        
        return header + payload
            
    def send_message(self, command:str='version', payload:bytes=b''):
        """ Sends the given message to the remote node """
        msg = self.marshall_message(command, payload)
        if command != 'getblocks' or ENABLE_DISPLAY_GETBLOCKS:
            self.print_message(msg, 'sending')
        self.send(self.sock, msg)
            
    def receive_message(self): 
        """ Receives messages from the remote node and processes them """
        while RUNNING:            
            # receive header
            header = self.receive(self.sock, HDR_SZ)
            
            if not RUNNING:
                self.sock.close()
                continue
                
            # receive payload
            payload_size = self.unmarshal_uint(header[16:20])
            payload = self.receive(self.sock, payload_size)
            # print received message
            command = str(bytearray([b for b in header[4:16] if b != 0]), encoding='utf-8')
            
            if command != 'inv' or ENABLE_DISPLAY_INV:
                self.print_message(header+payload, 'received')
            
            if command == 'version':
                self.send_message('verack')
                self.connected = True
            
            if command == 'ping':
                self.send_message('pong', payload)
                
            
            if command == 'inv':
                self.latest_block = payload[-32:].hex()
                inv_size, invsz = self.unmarshal_compactsize(payload)
                blocks = payload[len(inv_size):]
                self.save_blocks(blocks)
                self.latest_block = blocks[-32:].hex()
                
            if command == 'block':
                self.block = payload
                self.print_block_msg(payload)
                
    def report_block(self, payload:bytes, script_type='p2pkh', txidx=0, outidx=0):
        """ Prints the block modification report and comparision with the original block 
        :param payload: the block payload
        :param script_type: the script type (p2pkh, p2sh, p2wpkh, p2wsh)
        :param txidx: the transaction index to modify
        :param outidx: the output index to modify
        :return: None
        """
        print('\n{:=^65}\n'.format('MODIFICATION REPORT'))
        print('\n\033[92m{:^65}'.format('============================'))
        print('{:^65}'.format('||     ORIGINAL ↓ BLOCK     ||'))
        print('{:^65}\033[0m'.format('============================'))
        self.print_block_msg(payload)
        
        print('\n{:^70}\n'.format('\033[93m ==== MODIFICATION ==== \033[0m'))
        modified_payload = self.modify_block(payload, txidx, outidx, script_type)
        print('\n{:^70}\n'.format('\033[93m ========================= \033[0m'))
        
        print('\n\033[92m{:^65}'.format('============================'))
        print('{:^65}'.format('||     MODIFIED ↓ BLOCK     ||'))
        print('{:^65}\033[0m'.format('============================='))
        
        self.print_block_msg(modified_payload)
        
        print('\n\033[92m{:^65}'.format('==============================='))
        print('{:^65}'.format('||     DIFFERENCE ↓ REPORT     ||'))
        print('{:^65}\033[0m'.format('================================'))
        
        # the next block prev_hash is the current block header hash
        print('current header hash : ', self._hash(payload[:80]).hex())
        # modified payload produce different block header hash
        print('modified header hash: ', self._hash(modified_payload[:80]).hex())
        
        print('original merkle tree:')
        self.print_merkle_tree(self.get_block_transactions(payload))
        print('modified merkle tree:')
        self.print_merkle_tree(self.get_block_transactions(modified_payload))
        
        print('\n{:=^65}\n'.format('END REPORT'))
        
    def print_merkle_tree(self, txn_list:list):
        """ Prints the merkle tree of the given list of transactions 
        :param txn_list: list of transactions
        :return: merkle root
        """
        def shorten(h):
            h = h[::-1].hex()
            return h[:2] + '..' + h[-2:]
        
        if len(txn_list) == 1:
            print('MERKLE ROOT: {}\n'.format(txn_list[0].hex()))
            return txn_list[0]
                
        if len(txn_list) % 2 == 1:
            txn_list.append(txn_list[-1])
            
        new_txn_list = []
        
        for i in range(0, len(txn_list), 2):
            node = self._hash(txn_list[i] + txn_list[i+1])
            print("H({}|{})".format( 
                    shorten(txn_list[i]), 
                    shorten(txn_list[i+1])
                    ), end=' - ')
            new_txn_list.append(node)
            
        print('\n')
        txn_list = new_txn_list
        return self.print_merkle_tree(new_txn_list)
                   
    def save_blocks(self, blocks:bytes):
        """Store downloaded blocks in the dictionary indexed by height
        :param blocks: bytes of inv messages
        """
        height = len(self.blocks) + 1
        for i in range(0, len(blocks), 36):
            block_hash = blocks[i+4:i+36]
            self.blocks[height] = block_hash
            height += 1
        
    def get_block(self, height):
        """ get block by height by sending getdata message 
        :param height: block height
        :return: block hash
        """           
        while height > len(self.blocks):
            print('{:.^65}'.format(f'DOWNLOADING BLOCKS {len(self.blocks) + 1} - {500 + len(self.blocks)}'))
            block = self.latest_block
            payload = self.uint32_t(70015) + self.uint8_t(1) 
            payload += bytes.fromhex(self.latest_block + STOP_BLOCK)
            self.send_message('getblocks', payload)
            # wait for downloading blocks
            while block == self.latest_block:
                continue

        return self.blocks[height]
    
    def getdata(self, block_hash:bytes):
        """send getdata message to get block data
        :param block_hash: block hash
        :return: block data
        """
        payload = bytes.fromhex('01' + '02000000') + block_hash
        self.send_message('getdata', payload)
        time.sleep(1)
        return self.block
    
    def print_message(self, msg, text=None):
        """
        Report the contents of the given bitcoin message
        :param msg: bitcoin message including header
        :return: message type
        """
        print('\n{}MESSAGE'.format('' if text is None else (text + ' ')))
        print('({}) {}'.format(len(msg), msg[:60].hex() + ('' if len(msg) < 60 else '...')))
        payload = msg[HDR_SZ:]
        command = self.print_header(msg[:HDR_SZ], self.checksum(payload))
        # FIXME print out the payloads of other types of messages, too
        if command == 'version':
            self.print_version_msg(payload)
        return command

    def print_block_msg(self, b, detailed=ENABLE_DETAIL_TRANSACTION):
        """
        Report the contents of the given bitcoin block message
        :param b: bitcoin block message
        :return: None
        """
        prefix = '  '
        print(prefix + 'BLOCK HEADER')
        print(prefix + '-' * 56)
        prefix *= 2
        print('{}{:32} version'.format(prefix, b[:4].hex()))
        prev_block = b[4:36][::-1].hex()
        print('{}{:32} previous block: {}'.format(prefix, f'{b[4:36].hex()[:24]}...', f'{prev_block[:2]}..{prev_block[-2:]}'))
        merk_root = b[36:68][::-1].hex()
        print('{}{:32} merkle root: {}'.format(prefix, f'{b[36:68].hex()[:24]}...' , f'{merk_root[:2]}..{merk_root[-2:]}'))
        time_str = strftime("%a, %d %b %Y %H:%M:%S GMT", gmtime(self.unmarshal_int(b[68:72])))
        print('{}{:32} timestamp {}'.format(prefix, b[68:72].hex(), time_str))
        print('{}{:32} bits'.format(prefix, b[72:76].hex()))
        print('{}{:32} nonce: {}'.format(prefix, b[76:80].hex(), self.unmarshal_uint(b[76:80])))
        txn_count_t, txn_count = self.unmarshal_compactsize(b[80:])
        i = 80+len(txn_count_t)
        print('{}{:32} transaction count: {}'.format(prefix, txn_count_t.hex(), txn_count))
        
        prefix = '  '
        print(prefix + 'TRANSACTIONS')
        print(prefix + '-' * 56)
        for index in range(txn_count):
            size = self.get_transaction_size(b[i:])
            txn_hash = self._hash(b[i:i+size])
            print(prefix + 'TXID[{}]: {}'.format(index, txn_hash[::-1].hex()))
            detailed and self.print_transaction(b[i:i+size])
            i += size

    def get_block_transactions(self, b):
        """ Generate a list of transaction hashes from a block
        :param b: bitcoin block
        :return: list of transaction hashes
        """
        txn_count_t, txn_count = self.unmarshal_compactsize(b[80:])
        i = 80+len(txn_count_t)
        txn_list = []
        for _ in range(txn_count):
            size = self.get_transaction_size(b[i:])
            txn_hash = self._hash(b[i:i+size])
            txn_list.append(txn_hash)
            i += size
        return txn_list
            
    def get_merkle_root(self, txn_list):
        """ Generate merkle root from a list of transaction hashes
        :param txn_list: list of transaction hashes
        :return: merkle root
        """
        if len(txn_list) == 1:
            return txn_list[0]
                
        if len(txn_list) % 2 == 1:
            txn_list.append(txn_list[-1])
            
        new_txn_list = []
        
        for i in range(0, len(txn_list), 2):
            node = self._hash(txn_list[i] + txn_list[i+1])
            new_txn_list.append(node)
            
        txn_list = new_txn_list
        return self.get_merkle_root(new_txn_list)

    def modify_block(self, b, txn_idx, output_idx, script_type='p2pkh'):
        """ Modify the output of the given transaction in the given block
        :param b: bitcoin block message
        :param txn_idx: index of the transaction to modify
        :param output_idx: index of the output to modify
        :param script_type: type of script to use in the output
        :return: modified block
        """
        b = bytearray(b)
        txn_list = []
        txn_count_t, txn_count = self.unmarshal_compactsize(b[80:])
        i = 80 + len(txn_count_t)
        for index in range(txn_count):
            if index == txn_idx:
                print('→ replaced payout address transaction[{}], outputs[{}] '.format(txn_idx, output_idx))   
                        
            size, modified_txn = self.modify_transaction(
                b[i:], 
                modified=index==txn_idx, 
                idx=output_idx, 
                script_type=script_type
                )
            
            # update the transaction
            b[i:i+size] = modified_txn
            
            # build transaction list
            txn_hash = self._hash(modified_txn)
            txn_list.append(txn_hash)
        
            # move index to the next transaction    
            i += size
            
        # update merkle root
        original_merkle_root = b[36:68]
        modified_merkle_root = self.get_merkle_root(txn_list)
        b[36:68] = modified_merkle_root
        
        print('→ minning block with modified payout address...')
        start_time = time.time()
        difficulty = self.unmarshal_uint(b'\xff' * 29) # 6 zeros
        b[:80] = self.mine_block(b[:80], difficulty)
        end_time = time.time()
        
        seconds_elapsed = end_time - start_time
        hours, remain = divmod(seconds_elapsed, 3600)
        minutes, seconds = divmod(remain, 60)
        print('→ mined block duration {:02.0f}:{:02.0f}:{:02.0f}'.format(hours, minutes, seconds))
        
        print('original ROOT: {}'.format(original_merkle_root.hex()))
        print('modified ROOT: {}'.format(modified_merkle_root.hex()))
        return b

    def mine_block(self, b, difficulty=None):
        """ Mine the given block
        :param b: bitcoin block message
        :param difficulty: difficulty target
        :return: mined block
        """
        if difficulty is None:
            difficulty = self.unmarshal_uint(b'\xff' * 32)
            
        b = bytearray(b)
        nonce = 0
        while ENABLE_MINNING:
            b[76:80] = self.uint32_t(nonce)
            block_hash = self._hash(b)
            if self.unmarshal_uint(block_hash) <= difficulty:
                print('nonce: {}'.format(nonce))
                print('hash: {}'.format(block_hash.hex()))
                break
            nonce += 1
        return b
        
    def modify_transaction(self, b, modified=False, idx=1, script_type='p2pkh'):
        """ Modify a specific output address of a transaction by replacing the 
        PubKeyScript of the given transaction output
        :param b: transaction
        :param modified: if True, modify the transaction
        :param idx: index of the output to modify
        :param script_type: script type of the output to modify
        :return: modified transaction
        """
        # INPUTS
        inputs, insz = self.unmarshal_compactsize(b[4:])
        i = 4 + len(inputs)
        for _ in range(insz):
            scripts_size, ssz = self.unmarshal_compactsize(b[i+36:])
            i += 40 + len(scripts_size) + ssz

        # OUTPUTS            
        outputs, outsz = self.unmarshal_compactsize(b[i:])
        i += len(outputs)
        for index in range(outsz):
            i += 8 # pay amount
            script_size, ssz = self.unmarshal_compactsize(b[i:])
            # modified output index 0
            i += len(script_size)
            if modified and index == idx:
                pkscript = self.generate_pkscript(script_type)
                print('→ original PubKeyScript: {}'.format(b[i:i+ssz].hex()))
                print('→ modified PubkeyScript: {}'.format(pkscript.hex()))
                for j in range(len(pkscript)):
                    b[i+j] = pkscript[j]
            i += ssz
        return i + 4, b[:i + 4]

    def get_transaction_size(self, b):
        """Get the size of a transaction using umarshal_compactsize technique
        :param b: transaction
        :return: size of the transaction
        """
        # INPUTS
        inputs, insz = self.unmarshal_compactsize(b[4:])
        i = 4 + len(inputs)
        for _ in range(insz):
            scripts_size, ssz = self.unmarshal_compactsize(b[i+36:])
            i += 40 + len(scripts_size) + ssz

        # OUTPUTS            
        outputs, outsz = self.unmarshal_compactsize(b[i:])
        i += len(outputs)
        for _ in range(outsz):
            i += 8 # pay amount
            script_size, ssz = self.unmarshal_compactsize(b[i:])
            i += len(script_size) + ssz
        return i + 4
        
    def print_transaction(self, b):
        """ Display transaction details with inputs and outputs 
        :param b: transaction
        :return: None
        """
        # PRINT VERSION
        prefix = '  ' * 2
        print('{}{:34} Version'.format(prefix,  str(self.unmarshal_int(b[:4]))))
        
        # PRINT INPUTS
        prefix = '  ' * 2
        print('\n' + prefix + 'INPUTS')
        print(prefix + '-' * 56)
        prefix = '  ' * 3
        inputs, insz = self.unmarshal_compactsize(b[4:])
        print('{}{:32} Number of inputs'.format(prefix, inputs.hex()))
        i = 4 + len(inputs)
        for _ in range(insz):
            print(prefix + '.' * 56)
            print('{}{:32} Transaction Identifier'.format(prefix, b[i:i+32].hex()))
            print('{}{:32} Output Index'.format(prefix, b[i+32:i+36].hex()))
            scripts_size, ssz = self.unmarshal_compactsize(b[i+36:])
            i += 36 + len(scripts_size)
            print('{}{:32} Signature Script'.format(prefix, b[i:i+ssz].hex()))
            print('{}{:32} Sequence Number'.format(prefix, b[i+ssz:i+ssz+4].hex()))
            i += ssz + 4

        # PRINT OUTPUTS            
        prefix = '  ' * 2
        print('\n' + prefix + 'OUTPUTS')
        print(prefix + '-' * 56)
        outputs, outsz = self.unmarshal_compactsize(b[i:])
        
        prefix = '  ' * 3
        print('{}{:32} Number of outputs'.format(prefix, outputs.hex()))
        i += len(outputs)
        for _ in range(outsz):
            print(prefix + '.' * 56)
            print('{}{:32} Amount: {} (satoshis)'.format(prefix, b[i:i+8].hex(), self.unmarshal_uint(b[i:i+8])/10**8))
            script_size, ssz = self.unmarshal_compactsize(b[i+8:])
            i += 8 + len(script_size)
            print('{}{:32} Pubkey Script'.format(prefix, b[i:i+ssz].hex()))
            i += ssz
        print('\n{}{:34} Locktime'.format(prefix, b[i:i+4].hex()))
        return i + 4

    def print_version_msg(self, b):
        """
        Report the contents of the given bitcoin version message (sans the header)
        :param payload: version message contents
        """
        # pull out fields
        version, my_services, epoch_time, your_services = b[:4], b[4:12], b[12:20], b[20:28]
        rec_host, rec_port, my_services2, my_host, my_port = b[28:44], b[44:46], b[46:54], b[54:70], b[70:72]
        nonce = b[72:80]
        user_agent_size, uasz = self.unmarshal_compactsize(b[80:])
        i = 80 + len(user_agent_size)
        user_agent = b[i:i + uasz]
        i += uasz
        start_height, relay = b[i:i + 4], b[i + 4:i + 5]
        extra = b[i + 5:]

        # print report
        prefix = '  '
        print(prefix + 'VERSION')
        print(prefix + '-' * 56)
        prefix *= 2
        print('{}{:32} version {}'.format(prefix, version.hex(), self.unmarshal_int(version)))
        print('{}{:32} my services'.format(prefix, my_services.hex()))
        time_str = strftime("%a, %d %b %Y %H:%M:%S GMT", gmtime(self.unmarshal_int(epoch_time)))
        print('{}{:32} epoch time {}'.format(prefix, epoch_time.hex(), time_str))
        print('{}{:32} your services'.format(prefix, your_services.hex()))
        print('{}{:32} your host {}'.format(prefix, rec_host.hex(), self.ipv6_to_ipv4(rec_host)))
        print('{}{:32} your port {}'.format(prefix, rec_port.hex(), self.unmarshal_uint(rec_port)))
        print('{}{:32} my services (again)'.format(prefix, my_services2.hex()))
        print('{}{:32} my host {}'.format(prefix, my_host.hex(), self.ipv6_to_ipv4(my_host)))
        print('{}{:32} my port {}'.format(prefix, my_port.hex(), self.unmarshal_uint(my_port)))
        print('{}{:32} nonce'.format(prefix, nonce.hex()))
        print('{}{:32} user agent size {}'.format(prefix, user_agent_size.hex(), uasz))
        print('{}{:32} user agent \'{}\''.format(prefix, user_agent.hex(), str(user_agent, encoding='utf-8')))
        print('{}{:32} start height {}'.format(prefix, start_height.hex(), self.unmarshal_uint(start_height)))
        print('{}{:32} relay {}'.format(prefix, relay.hex(), bytes(relay) != b'\0'))
        if len(extra) > 0:
            print('{}{:32} EXTRA!!'.format(prefix, extra.hex()))

    def print_header(self, header, expected_cksum=None):
        """
        Report the contents of the given bitcoin message header
        :param header: bitcoin message header (bytes or bytearray)
        :param expected_cksum: the expected checksum for this version message, if known
        :return: message type
        """
        magic, command_hex, payload_size, cksum = header[:4], header[4:16], header[16:20], header[20:]
        command = str(bytearray([b for b in command_hex if b != 0]), encoding='utf-8')
        psz = self.unmarshal_uint(payload_size)
        if expected_cksum is None:
            verified = ''
        elif expected_cksum == cksum:
            verified = '(verified)'
        else:
            verified = '(WRONG!! ' + expected_cksum.hex() + ')'
        prefix = '  '
        print(prefix + 'HEADER')
        print(prefix + '-' * 56)
        prefix *= 2
        print('{}{:32} magic'.format(prefix, magic.hex()))
        print('{}{:32} command: {}'.format(prefix, command_hex.hex(), command))
        print('{}{:32} payload size: {}'.format(prefix, payload_size.hex(), psz))
        print('{}{:32} checksum {}'.format(prefix, cksum.hex(), verified))
        return command

    @staticmethod
    def htoi(hex_str, order='little'):
        return int.from_bytes(bytes.fromhex(hex_str), byteorder=order)
        
    @staticmethod
    def send(conn, data: bytes):
        """Send raw bytes stream using passed in socket"""
        conn.sendall(data)

    @staticmethod
    def receive(conn, buffer_size=BUF_SZ):
        """Receive raw data from a passed in socket
        :return: bytes stream of data
        """
        payload = b''
        while len(payload) != buffer_size:
            payload += conn.recv(buffer_size)
        return payload

if __name__ == '__main__':
    peer = BitCoinPeer()
    
    suid = 4162581 % 10000
    
    print('GETTING BLOCK {}...'.format(suid))
    block_hash = peer.get_block(suid)
    block = peer.getdata(block_hash)
    print('\nMORE DETAILS OF BLOCK {}...'.format(suid))
    peer.print_block_msg(block, detailed=True)
    print('\n> BLOCK [{}] HASH : {}\n'.format(suid, block_hash.hex()))
    
    if not ENABLE_MODIFICATION_REPORT:
        peer.shutdown()
        sys.exit(0)
    
    # NOTE: 1. This block only has a coinbase transaction.
    #       2. Using a block with more transactions would be more interesting
    #           to demonstrate modification and rebuilding of merkle tree.
    #       3. getblocks messages can be used to get later a blocks, however it might
    #           be slow and dependent on the network speed.
    #       2. Hard coded the two adjacent blocks below and use getdata messages should
    #           be faster and sufficient for the demonstration.
    
    this_block = '90f0a9f110702f808219ebea1173056042a714bad51b916cb680000000000000' # Block 100,001
    next_block = 'aff7e0c7dc29d227480c2aa79521419640a161023b51cdb28a3b010000000000' # Block 100,002
    
    print('GETTING BLOCK 100001...')
    block = peer.getdata(bytes.fromhex(this_block))
    print('\n> BLOCK [100001] HASH : {}'.format(block_hash.hex()))
    
    print('GETTING NEXT BLOCK 100002...')
    next_block = peer.getdata(bytes.fromhex(next_block))
    
    peer.report_block(block,'p2pkh',2,0)
    print('actual next block [100002] previous-block-header-hash: {}'.format(next_block[4:36].hex()))
    
    peer.shutdown()
    sys.exit(0)
    