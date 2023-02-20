#  -------------------------------------------------------------------------
#  Copyright (c) Microsoft Corporation. All rights reserved.
#  Licensed under the MIT License. See License.txt in the project root for
#  license information.
#  --------------------------------------------------------------------------
"""Elastic Driver class."""
import json
from datetime import datetime
from typing import Any, Dict, Iterable, Optional, Tuple, Union

import pandas as pd

from ..._version import VERSION
from ...common.exceptions import (
    MsticpyConnectionError,
    MsticpyImportExtraError,
    MsticpyUserConfigError,
    MsticpyParameterError,
)
from ...common.utility import check_kwargs, export
from ..core.query_defns import Formatters
from .driver_base import DriverBase, QuerySource

try:
    # from elasticsearch import Elasticsearch
    # from elasticsearch_dsl import Search
    from opensearchpy import OpenSearch as Elasticsearch
    from opensearchpy import AuthenticationException, AuthorizationException
    from opensearchpy import (
        ConnectionError,
        ConnectionTimeout,
        TransportError,
        SSLError,
    )
    from opensearch_dsl import Search
    from opensearch_dsl.query import Query
except ImportError as imp_err:
    raise MsticpyImportExtraError(
        "Cannot use this feature without elasticsearch_dsl installed",
        title="Error importing elasticsearch_dsl",
        extra="elasticsearch",
    ) from imp_err


__version__ = VERSION
__author__ = "Neil Desai, Ian Hellen"


ELASTIC_CONNECT_ARGS: Dict[str, str] = {
    "hosts": "(string or List(string)) The host name (default is 'localhost').",
    "api_key": "(Tuple) Authenticate via API key",
    "basic_auth": "(string or Tuple[str, str]) Login and password for basic auth (use http_auth if you don't know the auth type)",
    "http_auth": "(string or Tuple[str, str]) Login and password for standard auth",
    "http_compress": "(Boolean) Enables GZip compression for quest bodies (default is True)",
    "use_ssl": "(Boolean) Enables SSL (default is True)",
    "verify_certs": "(Boolean) Enable SSL verification for the connection(default is True)",
    "ssl_assert_hostname": "(Boolean) Enable verification of the hostname of the certificate (default is True)",
    "ssl_show_warn": "(Boolean) Enable a warning when disabling certificate verificaiton (default is True)",
    "ca_certs": "(string) Path to a CA certificate",
    "client_cert": "(string) Path to a client certificate used for authentication",
    "client_key": "(string) Key to open the client cert",
}


@export
class ElasticDriver(DriverBase):
    """Driver to connect and query from Elastic Search."""

    _CONNECT_DEFAULTS: Dict[str, Any] = {
        "hosts": [{"host": "localhost", "port": 9200}],
        "http_compress": True,
        "use_ssl": True,
    }
    _ELASTIC_REQUIRED_ARGS = []

    def __init__(self, **kwargs):
        """Instantiate Elastic Driver."""
        super().__init__(**kwargs)
        self.service = None
        self._loaded = True
        self._connected = False
        self._debug = kwargs.get("debug", False)

    def connect(self, connection_str: str = None, **kwargs):
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
        self.service = Elasticsearch(**cs_dict)
        try:
            self.service.info()
        except AuthenticationException as err:
            raise MsticpyConnectionError(
                f"Authentication error connecting to Elastic: {err}",
                title="Elastic authentication",
                help_uri="https://msticpy.readthedocs.io/en/latest/DataProviders.html",
            ) from err
        except (ConnectionError, ConnectionTimeout, TransportError, SSLError) as err:
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
            "api_key" not in cs_dict.keys()
            and "http_auth" not in cs_dict.keys()
            and "basic_auth" not in cs_dict.keys()
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
        query: Union[str, Query, Search],
        query_source: QuerySource = None,
        index: Union[str, list] = None,
        start=None,
        end=None,
        size=None,
        **kwargs,
    ) -> Union[pd.DataFrame, Any]:
        """
        Execute query and retrieve results.

        Parameters
        ----------
        index : Union[str, list]
            Index to search again
        query : Union[str, dict, Query, Search]
            Elastic query to execute
        query_source : QuerySource
            The query definition object
        start : datetime
            The start datetime for the query
        end : datetime
            The end datetime for the query
        size : int
            The number of event to get.

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

        if isinstance(query, Search):
            s = query.using(self.service).index(index)
        elif isinstance(query, Query):
            s = Search(using=self.service, index=index).query(query)
        elif isinstance(query, str):
            s = Search(using=self.service, index=index).query(
                "query_string", query=query
            )
        elif isinstance(query, dict):
            s = Search(using=self.service, index=index).from_dict(query)
        else:
            raise MsticpyParameterError(
                "Accepted types for query are string, dict, elasticsearch_dsl.Search or elasticsearch_dsl.query.Query"
            )

        # set number of results
        if size is not None:
            s = s.extra(size=size)

        # set date filter
        if start is not None:
            if end is not None:
                s = s.filter("range", **{"@timestamp": {"gte": start, "lte": end}})
            else:
                s = s.filter("range", **{"@timestamp": {"gte": start}})
        else:
            if end is not None:
                s = s.filter("range", **{"@timestamp": {"lte": end}})

        results = s.execute().to_dict()["hits"]["hits"]
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
