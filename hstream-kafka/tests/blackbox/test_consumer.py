import pytest
import timeit

import kafka
from kafka import KafkaConsumer
from kafka.admin import NewTopic
from kafka.structs import TopicPartition
from kafka.protocol.fetch import FetchRequest

from common import send_req

topic_name = "blackbox_test_topic"


def force_create_topic(admin, topic_name):
    try:
        admin.delete_topics([topic_name])
    except kafka.errors.UnknownTopicOrPartitionError:
        pass
    topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
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
