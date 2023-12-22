import socket
import time
from kafka.conn import BrokerConnection
from kafka.record import MemoryRecords, MemoryRecordsBuilder


# Also: https://kafka-python.readthedocs.io/en/master/apidoc/KafkaClient.html#kafka.KafkaClient.send
def send_req(port, req, resp_count=1):
    conn = BrokerConnection("localhost", port, socket.AF_UNSPEC)
    conn.connect_blocking()
    assert conn.connected()

    assert len(conn.in_flight_requests) == 0
    conn.send(req, blocking=True)
    assert len(conn.in_flight_requests) == 1

    resps = []
    while len(resps) < resp_count:
        time.sleep(0.2)
        results = conn.recv()
        _resps = [r[0] for r in results]
        resps = [*resps, *_resps]

    if resp_count == 1:
        return resps[0]
    else:
        return resps


# magic: [0, 1, 2]
# compression_type: [0, 1, 2, 3]
def encode_records(magic, key, value, n):
    builder = MemoryRecordsBuilder(
        magic=magic, compression_type=0, batch_size=1024 * 10
    )
    for offset in range(n):
        builder.append(timestamp=10000 + offset, key=key, value=value)
    builder.close()
    return builder.buffer()


def decode_records(records_bs):
    record_batch = MemoryRecords(records_bs)
    rets = []
    while record_batch.has_next():
        batch = record_batch.next_batch()
        rets.append(list(batch))
    return rets
