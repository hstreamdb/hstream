import pytest
from kafka.protocol.admin import ApiVersionRequest, ApiVersionResponse

from common import send_req


def test_send_api_versions(kafka_port, hstream_kafka_port):
    for v in range(0, 2):
        req = ApiVersionRequest[v]()
        kafka_resp = send_req(kafka_port, req)
        hstream_kafka_resp = send_req(hstream_kafka_port, req)
        assert isinstance(kafka_resp, ApiVersionResponse[v])
        assert isinstance(hstream_kafka_resp, ApiVersionResponse[v])
