import os
from dotenv import load_dotenv
import psycopg2
import logging
import enum
import typing


logger = logging.getLogger(__name__)


class ConnStringType(enum.Enum):
    SOURCE = "SRC_CONN_STRING"
    DESTINATION = "DST_CONN_STRING"


class DatabaseConnector:
    def __init__(self):
        load_dotenv()
        self.conn_strings = {
            ConnStringType.SOURCE: os.getenv("SRC_CONN_STRING"),
            ConnStringType.DESTINATION: os.getenv("DST_CONN_STRING"),
        }
        self.connections: typing.List[psycopg2.extensions.connection] = []

    def get_connection(self, type: ConnStringType):
        try:
            conn = psycopg2.connect(self.conn_strings[type])
            conn.autocommit = True
            self.connections.append(conn)
            logger.info(f"Connected to {type.name} database")
            return conn
        except psycopg2.OperationalError as e:
            logger.error(f"Error occurred while connecting to {type.name}: {e}")
            raise

    def get_src_connection(self):
        return self.get_connection(ConnStringType.SOURCE)

    def get_dst_connection(self):
        return self.get_connection(ConnStringType.DESTINATION)

    def get_conn_string(self, type: ConnStringType):
        return self.conn_strings[type]

    def get_src_conn_string(self):
        return self.get_conn_string(ConnStringType.SOURCE)

    def get_dst_conn_string(self):
        return self.get_conn_string(ConnStringType.DESTINATION)

    def __del__(self):
        for conn in self.connections:
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
