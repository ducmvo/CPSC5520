"""
CPSC-5520, Seattle University
Distributed System - Lab 3
Utilities Functions For Forex Subscriber Bytes Manipulation

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

import ipaddress
from datetime import datetime, timedelta
import struct

MAX_QUOTES_PER_MESSAGE = 50
MICROS_PER_SECOND = 1_000_000
EPOCH = datetime(1970, 1, 1)


def serialize_address(address):
    """
    Convert an address contain host and port to a byte stream used to send to publisher.
    :return: byte stream address
    :param address: tuple host:str and port:int
    """
    host, port = address
    data = bytes()
    data += ipaddress.ip_address(host).packed
    data += int(port).to_bytes(2, 'big')
    return data


def deserialize_utcdatetime(timestamp: bytes):
    """
    Convert a byte stream sent in big-endian to UTC datetime from a Forex Provider message.
    :param timestamp: raw bytes represent timestamp sent in big-endian
    """
    micros = int.from_bytes(timestamp, 'big')
    return EPOCH + timedelta(microseconds=micros)


def deserialize_price(price):
    """
    Convert a byte stream sent in little-endian format 64-bit floating point number, IEEE 754 binary64
    :param price: price bytes in little-endian format
    """
    return struct.unpack('<d', price)[0]


def unmarshal_message(message):
    """
    Reconstruct the quote sequence from the byte stream  received from Publisher
    :param message: byte stream received in UDP message
    :return: list of quote structures ('cross' and 'price', 'timestamp')
    """
    data = [message[i:i + 32] for i in range(0, len(message), 32)]
    records = []
    for record in data:
        timestamp = deserialize_utcdatetime(record[0:8])
        cross = record[8:11].decode('utf8') + '/' + record[11:14].decode('utf8')
        price = deserialize_price(record[14:22])
        records.append({'timestamp': timestamp, 'cross': cross, 'price': price})

    return records


if __name__ == '__main__':
    res = deserialize_price(b'\x05\x04\x03\x02\x01\xff?C')
    print('PASSED -> ', res == 9006104071832581.0)
    res = deserialize_utcdatetime(b'\x00\x007\xa3e\x8e\xf2\xc0')
    print('PASSED -> ', res == datetime(1971, 12, 10, 1, 2, 3, 64000))
    res = serialize_address(('127.0.0.1', 65534))
    print('PASSED -> ', res == b'\x7f\x00\x00\x01\xff\xfe')
