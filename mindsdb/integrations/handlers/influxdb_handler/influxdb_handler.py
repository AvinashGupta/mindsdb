from typing import Optional
from collections import OrderedDict

import pandas as pd
from influxdb import InfluxDBClient
from mindsdb_sql import parse_sql

from mindsdb.integrations.libs.base_handler import DatabaseHandler

from mindsdb_sql.parser.ast.base import ASTNode

from mindsdb.utilities.log import log
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE

import pandas as pd
from mindsdb.integrations.libs.base_handler import DatabaseHandler

class InfluxDBHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the InfluxDB statements.
    """

    name = 'influxdb'

    def __init__(self, name: str, connection_data: Optional[dict], **kwargs):
        """
        Initialize the handler.
        Args:
            name (str): name of particular handler instance
            connection_data (dict): parameters for connecting to the database
            **kwargs: arbitrary keyword arguments.
        """
        super().__init__(name)
        self.parser = parse_sql
        self.dialect = 'influxdb'

        self.connection_data = connection_data
        self.host = connection_data.get("host")
        self.port = int(connection_data.get("port") or 8086)
        self.user = connection_data.get("username")
        self.password = connection_data.get("password")
        self.database = connection_data.get("database")

        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False

    def connect(self) -> StatusResponse:
        """
        Set up the connection required by the handler.
        Returns:
            HandlerStatusResponse
        """

        if self.is_connected is True:
            return self.connection

        config = {
            'host': self.host,
            'port': self.port,
            'user': self.user,
            'password': self.password,
            'database': self.database
        }

        connection = InfluxDBClient(**config)
        self.is_connected = True
        self.connection = connection
        return self.connection


    def disconnect(self):
        """
        Close any existing connections.
        """
        
        if self.is_connected is False:
            return

        self.connection.close()
        self.is_connected = False
        return self.is_connected

    def check_connection(self) -> StatusResponse:
        """
        Check connection to the handler.
        Returns:
            HandlerStatusResponse
        """

        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            self.connect()
            response.success = True
        except Exception as e:
            log.error(f'Error connecting to InfluxDB, {e}!')
            response.error_message = str(e)
        finally:
            if response.success is True and need_to_close:
                self.disconnect()
            if response.success is False and self.is_connected is True:
                self.is_connected = False

        return response

    def native_query(self, query: str) -> StatusResponse:
        """
        Receive raw query and act upon it somehow.
        Args:
            query (str): query in native format
        Returns:
            HandlerResponse
        """

        need_to_close = self.is_connected is False

        connection = self.connect()

        try:
            result = connection.execute_statement(Statement=query)
            if result['Items']:
                records = []
                for record in result['Items']:
                    records.append(self.parse_record(record))
                response = Response(
                    RESPONSE_TYPE.TABLE,
                    data_frame=pd.json_normalize(records)
                )
            else:
                response = Response(RESPONSE_TYPE.OK)
        except Exception as e:
            log.error(f'Error running query: {query} on InfluxDB!')
            response = Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(e)
            )

        connection.close()
        if need_to_close is True:
            self.disconnect()

        return response

    def query(self, query: ASTNode) -> StatusResponse:
        """
        Receive query as AST (abstract syntax tree) and act upon it somehow.
        Args:
            query (ASTNode): sql query represented as AST. May be any kind
                of query: SELECT, INSERT, DELETE, etc
        Returns:
            HandlerResponse
        """

        return self.native_query(query.to_string())

    def get_tables(self) -> StatusResponse:
        """
        Return list of entities that will be accessible as tables.
        Returns:
            HandlerResponse
        """

        result = self.connection.list_tables()

        df = pd.DataFrame(
            data=result['TableNames'],
            columns=['table_name']
        )

        response = Response(
            RESPONSE_TYPE.TABLE,
            df
        )

        return response

    def get_columns(self, table_name: str) -> StatusResponse:
        """
        Returns a list of entity columns.
        Args:
            table_name (str): name of one of tables returned by self.get_tables()
        Returns:
            HandlerResponse
        """


connection_args = OrderedDict()

connection_args_example = OrderedDict()