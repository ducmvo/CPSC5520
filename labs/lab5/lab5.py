from time import sleep
from hashlib import sha256
import struct
import sys

network_string = "f9beb4d9"  # Mainnet

def send(msg,payload):
    ## Command is ASCII text, null padded to 12 bytes
    command = msg + ( ( 12 - len(msg) ) * "\00" )

    ## Payload length is a uint32_t
    payload_raw = payload
    payload_len = struct.pack("I", len(payload_raw))

    ## Checksum is first 4 bytes of SHA256(SHA256(<payload>))
    checksum = sha256(sha256(payload_raw.encode('utf-8')).digest()).digest()[:4]

    sys.stdout.write(
        network_string
        + command
        + payload_len
        + checksum
        + payload_raw
    )
    sys.stdout.flush()

send("version",
      "71110100" # ........................ Protocol Version: 70001
    + "0000000000000000" # ................ Services: Headers Only (SPV)
    + "c6925e5400000000" # ................ Time: 1415484102
    + "00000000000000000000000000000000"
    + "0000ffff7f000001208d" # ............ Receiver IP Address/Port
    + "00000000000000000000000000000000"
    + "0000ffff7f000001208d" # ............ Sender IP Address/Port
    + "0000000000000000" # ................ Nonce (not used here)
    + "1b" # .............................. Bytes in version string
    + "2f426974636f696e2e6f726720457861"
    + "6d706c653a302e392e332f" # .......... Version string
    + "93050500" # ........................ Starting block height: 329107
    + "00" # .............................. Relay transactions: false
)


def compactsize_t(n):
    if n < 252:
        return uint8_t(n)
    if n < 0xffff:
        return uint8_t(0xfd) + uint16_t(n)
    if n < 0xffffffff:
        return uint8_t(0xfe) + uint32_t(n)
    return uint8_t(0xff) + uint64_t(n)


def unmarshal_compactsize(b):
    key = b[0]
    if key == 0xff:
        return b[0:9], unmarshal_uint(b[1:9])
    if key == 0xfe:
        return b[0:5], unmarshal_uint(b[1:5])
    if key == 0xfd:
        return b[0:3], unmarshal_uint(b[1:3])
    return b[0:1], unmarshal_uint(b[0:1])


def bool_t(flag):
    return uint8_t(1 if flag else 0)


def ipv6_from_ipv4(ipv4_str):
    pchIPv4 = bytearray([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff])
    return pchIPv4 + bytearray((int(x) for x in ipv4_str.split('.')))


def ipv6_to_ipv4(ipv6):
    return '.'.join([str(b) for b in ipv6[12:]])


def uint8_t(n):
    return int(n).to_bytes(1, byteorder='little', signed=False)


def uint16_t(n):
    return int(n).to_bytes(2, byteorder='little', signed=False)


def int32_t(n):
    return int(n).to_bytes(4, byteorder='little', signed=True)


def uint32_t(n):
    return int(n).to_bytes(4, byteorder='little', signed=False)


def int64_t(n):
    return int(n).to_bytes(8, byteorder='little', signed=True)


def uint64_t(n):
    return int(n).to_bytes(8, byteorder='little', signed=False)


def unmarshal_int(b):
    return int.from_bytes(b, byteorder='little', signed=True)


def unmarshal_uint(b):
    return int.from_bytes(b, byteorder='little', signed=False)


def print_message(msg, text=None):
    """
    Report the contents of the given bitcoin message
    :param msg: bitcoin message including header
    :return: message type
    """
    print('\n{}MESSAGE'.format('' if text is None else (text + ' ')))
    print('({}) {}'.format(len(msg), msg[:60].hex() + ('' if len(msg) < 60 else '...')))
    payload = msg[HDR_SZ:]
    command = print_header(msg[:HDR_SZ], checksum(payload))
    if command == 'version':
        print_version_msg(payload)
    # FIXME print out the payloads of other types of messages, too
    return command


def print_version_msg(b):
    """
    Report the contents of the given bitcoin version message (sans the header)
    :param payload: version message contents
    """
    # pull out fields
    version, my_services, epoch_time, your_services = b[:4], b[4:12], b[12:20], b[20:28]
    rec_host, rec_port, my_services2, my_host, my_port = b[28:44], b[44:46], b[46:54], b[54:70], b[70:72]
    nonce = b[72:80]
    user_agent_size, uasz = unmarshal_compactsize(b[80:])
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
    print('{}{:32} version {}'.format(prefix, version.hex(), unmarshal_int(version)))
    print('{}{:32} my services'.format(prefix, my_services.hex()))
    time_str = strftime("%a, %d %b %Y %H:%M:%S GMT", gmtime(unmarshal_int(epoch_time)))
    print('{}{:32} epoch time {}'.format(prefix, epoch_time.hex(), time_str))
    print('{}{:32} your services'.format(prefix, your_services.hex()))
    print('{}{:32} your host {}'.format(prefix, rec_host.hex(), ipv6_to_ipv4(rec_host)))
    print('{}{:32} your port {}'.format(prefix, rec_port.hex(), unmarshal_uint(rec_port)))
    print('{}{:32} my services (again)'.format(prefix, my_services2.hex()))
    print('{}{:32} my host {}'.format(prefix, my_host.hex(), ipv6_to_ipv4(my_host)))
    print('{}{:32} my port {}'.format(prefix, my_port.hex(), unmarshal_uint(my_port)))
    print('{}{:32} nonce'.format(prefix, nonce.hex()))
    print('{}{:32} user agent size {}'.format(prefix, user_agent_size.hex(), uasz))
    print('{}{:32} user agent \'{}\''.format(prefix, user_agent.hex(), str(user_agent, encoding='utf-8')))
    print('{}{:32} start height {}'.format(prefix, start_height.hex(), unmarshal_uint(start_height)))
    print('{}{:32} relay {}'.format(prefix, relay.hex(), bytes(relay) != b'\0'))
    if len(extra) > 0:
        print('{}{:32} EXTRA!!'.format(prefix, extra.hex()))


def print_header(header, expected_cksum=None):
    """
    Report the contents of the given bitcoin message header
    :param header: bitcoin message header (bytes or bytearray)
    :param expected_cksum: the expected checksum for this version message, if known
    :return: message type
    """
    magic, command_hex, payload_size, cksum = header[:4], header[4:16], header[16:20], header[20:]
    command = str(bytearray([b for b in command_hex if b != 0]), encoding='utf-8')
    psz = unmarshal_uint(payload_size)
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