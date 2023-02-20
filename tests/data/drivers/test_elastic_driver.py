from datetime import datetime
import pytest
import pytest_check as check
from unittest.mock import Mock, patch

# from opensearch_dsl.connections import add_connection, connections
# from elasticsearch_dsl.connections import add_connection
from opensearch_dsl import Search
from opensearch_dsl.query import Query
from opensearch_dsl import Search, Q

from msticpy.common.exceptions import (
    MsticpyConnectionError,
    MsticpyNotConnectedError,
    MsticpyUserConfigError,
)

from msticpy.data.drivers.elastic_driver import ElasticDriver


@pytest.fixture
def mock_client():
    client = ElasticDriver()
    client.service = Mock()
    client.service.search.return_value = response
    client._connected = True
    return client


response = {
    "_shards": {"failed": 0, "successful": 10, "total": 10},
    "hits": {
        "hits": [
            {
                "_index": "test",
                "_type": "company",
                "_id": "elasticsearch",
                "_score": 12.0,
                "_source": {"city": "Amsterdam", "name": "Elasticsearch"},
            }
        ],
        "max_score": 12.0,
        "total": 123,
    },
    "timed_out": False,
    "took": 123,
}


def test_elastic_connect_no_params():
    """Check failure with no args."""

    el_driver = ElasticDriver()
    check.is_true(el_driver._loaded)

    with pytest.raises(MsticpyUserConfigError) as mp_ex:
        el_driver.connect()
        check.is_false(el_driver._connected)
    check.is_in("Elastic connection parameter missing", mp_ex.value.args)


def test_elastic_connect_req_params():
    """Check load/connect success with required params."""

    # TODO mock sthg

    el_driver = ElasticDriver()
    check.is_true(el_driver._loaded)

    # [SuppressMessage("Microsoft.Security", "CS002:SecretInNextLine", Justification="Test code")]
    el_driver.connect(http_auth=("user", "password"))
    check.is_true(el_driver._connected)

    # [SuppressMessage("Microsoft.Security", "CS002:SecretInNextLine", Justification="Test code")]
    el_driver.connect(api_key="YWRtaW46cGFzc3dvcmQ=")
    check.is_true(el_driver._connected)


def test_query_string_query(mock_client):

    res = mock_client.query(index="test", query="test:test")
    mock_client.service.search.assert_called_once_with(
        index=["test"], body={"query": {"query_string": {"query": "test:test"}}}
    )
    check.equal(
        res.to_dict("records"),
        [
            {
                "_index": "test",
                "_type": "company",
                "_id": "elasticsearch",
                "_score": 12.0,
                "city": "Amsterdam",
                "name": "Elasticsearch",
            }
        ],
    )


def test_Query_query_with_dates(mock_client):
    res = {
        "query": {
            "bool": {
                "filter": [
                    {
                        "range": {
                            "@timestamp": {
                                "gte": datetime(2023, 1, 1),
                                "lte": datetime(2023, 1, 1),
                            }
                        }
                    }
                ],
                "must": [{"match": {"test": "test"}}],
            }
        }
    }

    mock_client.query(
        index="test",
        query=Q("match", test="test"),
        start=datetime(2023, 1, 1),
        end=datetime(2023, 1, 1),
    )
    mock_client.service.search.assert_called_once_with(index=["test"], body=res)


def test_Search_query(mock_client):

    mock_client.query(index="test", query=Search().query(Q("match", test="test")))
    mock_client.service.search.assert_called_once_with(
        index=["test"], body={"query": {"match": {"test": "test"}}}
    )
