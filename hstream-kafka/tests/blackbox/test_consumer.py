import pytest
import timeit
import time
import random
import string

import kafka
from kafka import KafkaConsumer
from kafka.admin import NewTopic
from kafka.structs import TopicPartition
from kafka.protocol.fetch import FetchRequest
from kafka.protocol.produce import ProduceRequest, ProduceResponse

from common import send_req, encode_records, decode_records

topic_name = "blackbox_test_topic"


def force_create_topic(admin, topic_name, partitions=1):
    try:
        admin.delete_topics([topic_name])
    except kafka.errors.UnknownTopicOrPartitionError:
        pass
    topic = NewTopic(
        name=topic_name, num_partitions=partitions, replication_factor=1
    )
    admin.create_topics([topic])


@pytest.fixture
def new_topic(
    kafka_admin_client, hstream_kafka_admin_client, topic_name=topic_name
):
    force_create_topic(kafka_admin_client, topic_name)
    force_create_topic(hstream_kafka_admin_client, topic_name)
    yield
    kafka_admin_client.delete_topics([topic_name])
    hstream_kafka_admin_client.delete_topics([topic_name])


def test_fetch_empty_topics(kafka_port, hstream_kafka_port):
    req = FetchRequest[2](
        -1,  # replica_id
        2000,  # fetch_max_wait_ms
        1,  # fetch_min_bytes
        [],
    )
    kafka_resp = send_req(kafka_port, req)
    hstream_kafka_resp = send_req(hstream_kafka_port, req)
    assert kafka_resp == hstream_kafka_resp


def test_fetch_zero_min_bytes_should_return_immediately(
    new_topic, kafka_port, hstream_kafka_port
):
    req = FetchRequest[2](
        -1,  # replica_id
        2000,  # fetch_max_wait_ms
        1,  # fetch_min_bytes
        [(topic_name, [(0, 0, 200)])],
    )
    req0 = FetchRequest[2](
        -1,  # replica_id
        4000,  # fetch_max_wait_ms
        0,  # fetch_min_bytes
        [(topic_name, [(0, 0, 200)])],
    )

    t = timeit.timeit(lambda: send_req(kafka_port, req), number=1)
    assert t > 2  # should be more than 2s
    t = timeit.timeit(lambda: send_req(kafka_port, req0), number=1)
    assert t < 1  # should be less than 1s

    t = timeit.timeit(lambda: send_req(hstream_kafka_port, req), number=1)
    assert t > 2  # should be more than 2s
    t = timeit.timeit(lambda: send_req(hstream_kafka_port, req0), number=1)
    assert t < 1  # should be less than 1s


def test_offsets_of_empty_topic(new_topic, kafka_port, hstream_kafka_port):
    kafka_consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=f"localhost:{kafka_port}",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        fetch_max_wait_ms=1000,
        api_version=(0, 10),
    )
    hstream_kafka_consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=f"localhost:{hstream_kafka_port}",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        fetch_max_wait_ms=1000,
        api_version=(0, 10),
    )
    partition = TopicPartition(topic_name, 0)
    assert kafka_consumer.beginning_offsets(
        [partition]
    ) == hstream_kafka_consumer.beginning_offsets([partition])
    assert kafka_consumer.end_offsets(
        [partition]
    ) == hstream_kafka_consumer.end_offsets([partition])


# parts: [(partition, payload_length)]
def gen_produce(magic, topic_name, parts):
    topic_data = [
        (p, encode_records(magic, b"", b"h" * length, 1)) for p, length in parts
    ]
    if magic <= 1:
        return ProduceRequest[2](
            required_acks=-1,
            timeout=1000,
            topics=[(topic_name, topic_data)],
        )
    else:
        return ProduceRequest[3](
            transactional_id=None,
            required_acks=-1,
            timeout=10000,
            topics=[(topic_name, topic_data)],
        )


def send_produce(port, req, exp_topic_name):
    produce_resp = send_req(port, req)
    for t in produce_resp.topics:
        assert t[0] == exp_topic_name
        for _, errcode, *xs in t[1]:
            assert errcode == 0


# parts: [(partition, offset, partition_max_bytes)]
def gen_fetch(max_bytes, topic_name, parts):
    return FetchRequest[4](
        -1,  # replica_id
        500,  # fetch_max_wait_ms
        10,  # fetch_min_bytes
        max_bytes,
        0,  # isolation_level
        [(topic_name, parts)],
    )


def send_fetch(port, req, exp_topic_name):
    fetch_resp = send_req(port, req)
    for topic, partitions in fetch_resp.topics:
        assert topic == exp_topic_name
        for (
            partition_id,
            error_code,
            highwater_offset,
            last_stable_offset,  # v4
            aborted_transactions,  # v4
            records_bs,
        ) in partitions:
            assert error_code == 0
            yield records_bs, decode_records(records_bs)


@pytest.fixture
def setup_TestFetchMaxBytes(kafka_admin_client, hstream_kafka_admin_client):
    topic_name = "".join(
        random.choices(string.ascii_uppercase + string.digits, k=10)
    )
    force_create_topic(kafka_admin_client, topic_name, partitions=2)
    force_create_topic(hstream_kafka_admin_client, topic_name, partitions=2)
    # Wait topic creation done
    time.sleep(1)
    yield topic_name
    kafka_admin_client.delete_topics([topic_name])
    hstream_kafka_admin_client.delete_topics([topic_name])


@pytest.mark.usefixtures("setup_TestFetchMaxBytes")
@pytest.mark.parametrize("magic", [2])
@pytest.mark.parametrize("port_var", ["kafka_port", "hstream_kafka_port"])
class TestFetchMaxBytes:
    def test_max_bytes(self, port_var, magic, setup_TestFetchMaxBytes, request):
        port = request.getfixturevalue(port_var)
        topic = setup_TestFetchMaxBytes

        # Real records(length) in partition0:
        #
        # record1 record2
        # 118     88
        send_produce(
            port,
            gen_produce(magic, topic, [(0, 50), (1, 50)]),
            topic,
        )
        send_produce(
            port,
            gen_produce(magic, topic, [(0, 20), (1, 20)]),
            topic,
        )

        # Case1
        [(records_bs, [[record]])] = list(
            send_fetch(
                port,
                gen_fetch(10, topic, [(0, 0, 2000)]),
                topic,
            )
        )
        assert len(records_bs) == 118
        record.value == b"h" * 50

        # Case2
        [(records_bs, [[record]])] = list(
            send_fetch(
                port,
                gen_fetch(118 + 10, topic, [(0, 0, 2000)]),
                topic,
            )
        )
        assert len(records_bs) == 128
        record.value == b"h" * 50

        # Case3
        [(records_bs, [[record1], [record2]])] = list(
            send_fetch(
                port,
                gen_fetch(118 + 88, topic, [(0, 0, 2000)]),
                topic,
            )
        )
        assert len(records_bs) == 118 + 88
        record1.value == b"h" * 50
        record2.value == b"h" * 20

        # Case4
        [(records0_bs, records0), (records1_bs, records1)] = list(
            send_fetch(
                port,
                gen_fetch(118 + 117, topic, [(0, 0, 118), (1, 0, 118)]),
                topic,
            )
        )
        assert len(records0_bs) == 118
        assert len(records1_bs) == 0

        # Case5
        [(records0_bs, records0), (records1_bs, records1)] = list(
            send_fetch(
                port,
                gen_fetch(118 + 118, topic, [(0, 0, 118), (1, 0, 118)]),
                topic,
            )
        )
        assert len(records0_bs) == 118
        assert len(records1_bs) == 118
