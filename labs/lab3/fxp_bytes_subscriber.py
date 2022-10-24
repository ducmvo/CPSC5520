import ipaddress
from datetime import datetime, timedelta
import struct
MAX_QUOTES_PER_MESSAGE = 50
MICROS_PER_SECOND = 1_000_000
EPOCH = datetime(1970, 1, 1)


def serialize_address(address):
    host, port = address
    data = bytes()
    data += ipaddress.ip_address(host).packed
    data += int(port).to_bytes(2, 'big')
    return data


def deserialize_utcdatetime(timestamp: bytes):
    micros = int.from_bytes(timestamp, 'big')
    return EPOCH + timedelta(microseconds=micros)


def deserialize_serialize_price(price):
    return struct.unpack('d', price)[0]


def unmarshal_message(message):
    data = [message[i:i + 32] for i in range(0, len(message), 32)]
    records = []
    for record in data:
        timestamp = deserialize_utcdatetime(record[0:8])
        cross = record[8:11].decode('utf8') + '/' + record[11:14].decode('utf8')
        price = deserialize_serialize_price(record[14:22])
        records.append({'timestamp': timestamp, 'cross': cross, 'price': price})

    return records


if __name__ == '__main__':
    # res = unmarshal_message(message)
    # res = deserialize_serialize_price(b'\x05\x04\x03\x02\x01\xff?C')
    # res = deserialize_utcdatetime(b'\x00\x007\xa3e\x8e\xf2\xc0')
    # datetime(1971, 12, 10, 1, 2, 3, 64000)
    res = serialize_address(('127.0.0.1', 65534))
    print(res)
