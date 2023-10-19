import pytest
from kafka import KafkaAdminClient


@pytest.fixture
def kafka_port():
    return 19092


@pytest.fixture
def hstream_kafka_port():
    return 19093


@pytest.fixture
def kafka_admin_client(kafka_port):
    return KafkaAdminClient(bootstrap_servers=f"localhost:{kafka_port}")


@pytest.fixture
def hstream_kafka_admin_client(hstream_kafka_port):
    return KafkaAdminClient(bootstrap_servers=f"localhost:{hstream_kafka_port}")
