import os
import json
from dotenv import load_dotenv
import psycopg2
import logging
import enum
import typing

logger = logging.getLogger(__name__)


class ConnStringType(enum.Enum):
    SOURCE = "SRC_CONN_STRING"
    DESTINATION = "DST_CONN_STRINGS"


class MultiClusterCursor:
    def __init__(self, cursors: typing.List[psycopg2.extensions.cursor]):
        self.cursors = cursors

    def execute(self, query, params=None):
        for cursor in self.cursors:
            cursor.execute(query, params)

    def fetchall(self):
        return [cursor.fetchall() for cursor in self.cursors]

    def close(self):
        for cursor in self.cursors:
            cursor.close()


def _create_connection(conn_string):
    try:
        conn = psycopg2.connect(conn_string)
        conn.autocommit = True
        logger.info(f"Connected to {conn_string.split()[0]}")
        return conn
    except psycopg2.OperationalError as e:
        logger.error(f"Error occurred while connecting to database: {e}")
        raise


def _close_connection_impl(conn):
    if conn is not None and not conn.closed:
        try:
            conn.close()
            logger.info(f"Database connection closed ({conn.dsn})")
        except Exception as err:
            logger.info(
                f"Error occurred while closing the connection: {err}, connection params: {conn.dsn}"
            )
            raise
    else:
        logger.info(f"Database connection already closed ({conn.dsn})")


class MultiClusterConnection:
    def __init__(self, conn_strs: typing.List[str]):
        self.connections = [_create_connection(conn_str) for conn_str in conn_strs]
        self.closed = False

    def cursor(self):
        cursors = [conn.cursor() for conn in self.connections]
        return MultiClusterCursor(cursors)

    def commit(self):
        for conn in self.connections:
            conn.commit()

    def rollback(self):
        for conn in self.connections:
            conn.rollback()

    def close(self):
        for conn in self.connections:
            _close_connection_impl(conn)
        self.closed = True

    def set_autocommit(self, option: bool):
        for conn in self.connections:
            conn.autocommit = option


def _close_connection(
    conn: typing.Union[psycopg2.extensions.connection, MultiClusterConnection]
):
    if isinstance(conn, MultiClusterConnection):
        conn.close()
    if isinstance(conn, psycopg2.extensions.connection):
        _close_connection_impl(conn)


class DatabaseConnector:
    def __init__(self):
        load_dotenv()
        aa = os.getenv("DST_CONN_STRINGS")
        self.conn_strings = {
            ConnStringType.SOURCE: os.getenv("SRC_CONN_STRING"),
            ConnStringType.DESTINATION: json.loads(
                os.getenv("DST_CONN_STRINGS").replace("\n", "")
            ),
        }
        self.connections: typing.List[
            typing.Union[psycopg2.extensions.connection, MultiClusterConnection]
        ] = []

    def get_src_connection(self):
        conn_str = self.conn_strings[ConnStringType.SOURCE]
        conn = _create_connection(conn_str)
        self.connections.append(conn)
        return conn

    def get_dst_connection(self):
        conn_strs = self.conn_strings[ConnStringType.DESTINATION]
        conn = MultiClusterConnection(conn_strs)
        self.connections.append(conn)
        return conn

    def get_src_conn_string(self):
        return self.conn_strings[ConnStringType.SOURCE]

    def __del__(self):
        for conn in self.connections:
            _close_connection(conn)
