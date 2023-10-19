import socket
import time
from kafka.conn import BrokerConnection


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
