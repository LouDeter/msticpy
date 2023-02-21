#  -------------------------------------------------------------------------
#  Copyright (c) Microsoft Corporation. All rights reserved.
#  Licensed under the MIT License. See License.txt in the project root for
#  license information.
#  --------------------------------------------------------------------------
"""Elastic Driver class."""
import datetime
from typing import Any, Dict, Optional, Tuple, Union, List

import pandas as pd

from ..._version import VERSION
from ...common.exceptions import (
    MsticpyConnectionError,
    MsticpyImportExtraError,
    MsticpyUserConfigError,
    MsticpyParameterError,
)
from ...common.utility import check_kwargs, export
from .driver_base import DriverBase, QuerySource

try:
    import elasticsearch
    import elasticsearch_dsl

    elasticsearch_lib = True
except ImportError:
    elasticsearch_lib = False

try:
    import opensearchpy
    import opensearch_dsl

    opensearch_lib = True
except ImportError:
    opensearch_lib = False

__version__ = VERSION
__author__ = "Neil Desai, Ian Hellen"

ELASTIC_CONNECT_ARGS: Dict[str, str] = {
    "hosts": "(string or List(string)) The host name (default is 'localhost').",
    "api_key": "(Tuple) Authenticate via API key",
    "basic_auth": "(string or Tuple[str, str]) Login and password for basic auth (use "
    + "http_auth if you don't know the auth type)",
    "http_auth": "(string or Tuple[str, str]) Login and password for standard auth",
    "http_compress": "(Boolean) Enables GZip compression for quest bodies (default is True)",
    "use_ssl": "(Boolean) Enables SSL (default is True)",
    "verify_certs": "(Boolean) Enable SSL verification for the connection (default is "
    + "True)",
    "ssl_assert_hostname": "(Boolean) Enable verification of the hostname of the "
    + "certificate (default is True)",
    "ssl_show_warn": "(Boolean) Enable a warning when disabling certificate "
    + "verification (default is True)",
    "ca_certs": "(string) Path to a CA certificate",
    "client_cert": "(string) Path to a client certificate used for authentication",
    "client_key": "(string) Key to open the client cert",
}


class XSearchDriver(DriverBase):
    """Driver to connect and query from ElasticSearch or OpenSearch."""

    _CONNECT_DEFAULTS: Dict[str, Any] = {
        "hosts": [{"host": "localhost", "port": 9200}],
        "http_compress": True,
        "use_ssl": True,
    }
    _ELASTIC_REQUIRED_ARGS: List[str] = []

    def __init__(self, **kwargs):
        """Instantiate Driver."""
        super().__init__(**kwargs)
        self.service = None
        self._loaded = True
        self._connected = False
        self._debug = kwargs.get("debug", False)
        self.ConnectionError = Exception
        self.ConnectionTimeout = Exception
        self.TransportError = Exception
        self.SSLError = Exception
        self.Backend = None
        self.BESearch = None
        self.BEQuery = None

    def connect(self, connection_str: Optional[str] = None, **kwargs):
        """
        Connect to Elastic cluster.

        Parameters
        ----------
        connection_str : Optional[str], optional
            Connection string with Elastic connection parameters

        Other Parameters
        ----------------
        kwargs :
            Connection parameters can be supplied as keyword parameters.

        Notes
        -----
        Default configuration is read from the DataProviders/Elastic
        section of msticpyconfig.yaml, if available.
        Auth args are defined at : https://elasticsearch-py.readthedocs.io/en/latest/api.html
        """
        cs_dict = self._get_connect_args(connection_str, **kwargs)
        self.service = self.Backend(**cs_dict)
        try:
            self.service.info()
        except self.AuthenticationException as err:
            raise MsticpyConnectionError(
                f"Authentication error connecting to Elastic: {err}",
                title="Elastic authentication",
                help_uri="https://msticpy.readthedocs.io/en/latest/DataProviders.html",
            ) from err
        except (
            self.ConnectionError,
            self.ConnectionTimeout,
            self.TransportError,
            self.SSLError,
        ) as err:
            raise MsticpyConnectionError(
                f"Error connecting to Elastic: {err}",
                title="Elastic connection",
                help_uri="https://msticpy.readthedocs.io/en/latest/DataProviders.html",
            ) from err

        self._connected = True
        print("connected")

    def _get_connect_args(
        self, connection_str: Optional[str], **kwargs
    ) -> Dict[str, Any]:
        """Check and consolidate connection parameters."""
        cs_dict: Dict[str, Any] = self._CONNECT_DEFAULTS
        # Fetch any config settings
        cs_dict.update(self._get_config_settings("Elastic"))
        # If a connection string - parse this and add to config
        if connection_str:
            cs_items = connection_str.split(";")
            cs_dict.update(
                {
                    cs_item.split("=")[0].strip(): cs_item.split("=")[1]
                    for cs_item in cs_items
                }
            )
        elif kwargs:
            # if connection args supplied as kwargs
            cs_dict.update(kwargs)
            check_kwargs(cs_dict, list(ELASTIC_CONNECT_ARGS.keys()))

        missing_args = set(self._ELASTIC_REQUIRED_ARGS) - cs_dict.keys()
        if missing_args or (
            "api_key" not in cs_dict
            and "http_auth" not in cs_dict
            and "basic_auth" not in cs_dict
        ):
            raise MsticpyUserConfigError(
                "One or more connection parameters are missing for Elastic connector",
                ", ".join(missing_args),
                f"Required parameters either api_key or http_auth, {', '.join(self._ELASTIC_REQUIRED_ARGS)}",
                "All parameters: (See https://elasticsearch-py.readthedocs.io/en/latest/api.html)",
                *[f"{arg}: {desc}" for arg, desc in ELASTIC_CONNECT_ARGS.items()],
                title="Elastic connection parameter missing",
            )
        return cs_dict

    def query(
        self,
        # query: Union[str, Query, Search],
        query,
        query_source: Optional[QuerySource] = None,
        index: Union[str, list] = None,
        start: Optional[datetime.datetime] = None,
        end: Optional[datetime.datetime] = None,
        size: Optional[int] = None,
        **kwargs,
    ) -> Union[pd.DataFrame, Any]:
        """
        Execute query and retrieve results.

        Parameters
        ----------
        index : Union[str, list]
            Index to search again
        query : Union[str, dict, Query, Search]
            Elastic query to execute.
            - string: the query_string query will be used. It's much like lucene search.
            example: query("event.code:4624")
            https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html
            - dict: it will be considered as a DSL query
            example: query({"must": [{"match": {"event.code": "4624"}}]})
            https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html
            - Query: you want to create a query with elastic_dsl Q
            example:  query(Q("match", event__code="4624"))
            https://elasticsearch-dsl.readthedocs.io/en/latest/search_dsl.html#queries
            - Search: you want the full power of the Search object
            example: query(Search().query(Q("match", winlog__event_id="4624")))
            https://elasticsearch-dsl.readthedocs.io/en/latest/search_dsl.html#the-search-object
        query_source : Optional[QuerySource]
            The query definition object
        start : Optional[datetime.datetime]
            The start datetime for the query
        end : Optional[datetime.datetime]
            The end datetime for the query
        size : Optional[int]
            The number of event to get. Default size is 10.

        Other Parameters
        ----------------
        kwargs :
            table (string)
                Alias of index

        Returns
        -------
        Union[pd.DataFrame, Any]
            Query results in a dataframe.
            or query response if an error.

        """
        del query_source
        if not self._connected:
            raise self._create_not_connected_err("Elastic")

        if kwargs.get("table", None) and index is None:
            index = kwargs["table"]
        if index is None:
            raise MsticpyParameterError("Index parameter is missing", parameters="index")

        if isinstance(query, self.BESearch):
            search = query.using(self.service).index(index)
        elif isinstance(query, self.BEQuery):
            search = self.BESearch(using=self.service, index=index).query(query)
        elif isinstance(query, str):
            search = self.BESearch(using=self.service, index=index).query(
                "query_string", query=query
            )
        elif isinstance(query, dict):
            search = self.BESearch(using=self.service, index=index).from_dict(query)
        else:
            raise MsticpyParameterError(
                "Accepted types for query are string, dict, elasticsearch_dsl.Search "
                + "or elasticsearch_dsl.query.Query",
                parameters="query",
            )

        # set number of results
        if size is not None:
            search = search.extra(size=size)

        # set date filter
        if start is not None:
            if end is not None:
                search = search.filter(
                    "range", **{"@timestamp": {"gte": start, "lte": end}}
                )
            else:
                search = search.filter("range", **{"@timestamp": {"gte": start}})
        else:
            if end is not None:
                search = search.filter("range", **{"@timestamp": {"lte": end}})

        results = search.execute().to_dict()["hits"]["hits"]
        pd_result = pd.json_normalize(results)
        pd_result.rename(
            {col: col.replace("_source.", "") for col in pd_result.columns},
            axis=1,
            inplace=True,
        )

        return pd_result

    def query_with_results(self, query: str, **kwargs) -> Tuple[pd.DataFrame, Any]:
        """
        Execute query string and return DataFrame of results.

        Parameters
        ----------
        query : str
            Query to execute.

        Returns
        -------
        Union[pd.DataFrame,Any]
            A DataFrame (if successful) or
            the underlying provider result if an error occurs.

        """
        raise NotImplementedError(f"Not supported for {self.__class__.__name__}")


@export
class ElasticDriver(XSearchDriver):
    """
    ElasticSearch interface for the mutualized XSearchDriver.
    """

    def __init__(self, **kwargs):
        """Instantiate Driver."""
        super().__init__(**kwargs)
        if not elasticsearch_lib:
            raise MsticpyImportExtraError(
                "Cannot use this feature without elasticsearch_dsl installed",
                title="Error importing elasticsearch_dsl",
                extra="elasticsearch",
            )
        self.ConnectionError = elasticsearch.ConnectionError
        self.ConnectionTimeout = elasticsearch.ConnectionTimeout
        self.TransportError = elasticsearch.TransportError
        self.SSLError = elasticsearch.SSLError
        self.AuthenticationException = elasticsearch.AuthenticationException
        self.Backend = elasticsearch.Elasticsearch
        self.BESearch = elasticsearch_dsl.Search
        self.BEQuery = elasticsearch_dsl.query.Query


@export
class OpenSearchDriver(XSearchDriver):
    """
    OpenSearch interface for the mutualized XSearchDriver.
    """

    def __init__(self, **kwargs):
        """Instantiate Driver."""
        super().__init__(**kwargs)
        if not opensearch_lib:
            raise MsticpyImportExtraError(
                "Cannot use this feature without elasticsearch_dsl installed",
                title="Error importing elasticsearch_dsl",
                extra="elasticsearch",
            )
        self.ConnectionError = opensearchpy.ConnectionError
        self.ConnectionTimeout = opensearchpy.ConnectionTimeout
        self.TransportError = opensearchpy.TransportError
        self.SSLError = opensearchpy.SSLError
        self.AuthenticationException = opensearchpy.AuthenticationException
        self.Backend = opensearchpy.OpenSearch
        self.BESearch = opensearch_dsl.Search
        self.BEQuery = opensearch_dsl.query.Query
