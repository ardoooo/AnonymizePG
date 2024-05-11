import logging
import psycopg2
import typing

from src.utils import db_connector


logger = logging.getLogger(__name__)


def prepare_src_table(
    conn: psycopg2.extensions.connection, src_table: str, proccesed_column: str
):
    logger.info(f"Starting to prepare source table '{src_table}'")
    cur = conn.cursor()
    try:
        cur.execute(f"ALTER TABLE {src_table} ADD COLUMN {proccesed_column} BOOLEAN;")
        logger.info(f"Added column '{proccesed_column}' to '{src_table}'")

        cur.execute(
            f"CREATE INDEX CONCURRENTLY {proccesed_column} ON {src_table}({proccesed_column}) WHERE {proccesed_column} IS NULL;"
        )
        logger.info(f"Created index for column '{proccesed_column}' on '{src_table}'")

        logger.info(f"Successfully completed preparation of source table '{src_table}'")
    except psycopg2.Error as err:
        logger.error(f"Failed to prepare source table '{src_table}': {err}")
        raise

    finally:
        cur.close()


def prepare_transfer_table(
    conn: psycopg2.extensions.connection,
    src_table: str,
    transfer_table: str,
    id_column: str,
    publication: typing.Optional[str],
    table_schema,
    with_replication: bool,
):
    logger.info(f"Starting to prepare '{transfer_table}' based on '{src_table}'.")
    cur = conn.cursor()

    try:
        columns_str = ", ".join([f"{column[0]} {column[1]}" for column in table_schema])

        cur.execute(
            f"CREATE TABLE {transfer_table} ({columns_str}, {id_column} BIGSERIAL PRIMARY KEY)"
        )
        logger.info(f"Created table '{transfer_table}'")

        if with_replication:
            if publication is None:
                raise Exception("Publication is None, but mode is with replication")

            cur.execute(
                f"CREATE PUBLICATION {publication} FOR TABLE {transfer_table} WITH (publish = 'insert')"
            )
            logger.info(
                f"Created publication '{publication}' for table '{transfer_table}'"
            )

        logger.info(f"Successfully completed preparation of '{transfer_table}'")
    except psycopg2.Error as err:
        logger.error(f"Failed to prepare transfer table '{transfer_table}': {err}")
        raise

    finally:
        cur.close()


def slot_name_generator():
    i = 1
    while True:
        yield (f"transfer_slot_replica_{i}",)
        i += 1


def prepare_dst_table(
    dst_conn: db_connector.MultiClusterConnection,
    src_conn_string: str,
    transfer_table: str,
    id_column,
    publication: str,
    subscription: str,
    table_schema,
):
    logger.info(f"Starting to prepare destination table '{transfer_table}'")
    dst_cur = dst_conn.cursor()
    try:
        columns_str = ", ".join([f"{column[0]} {column[1]}" for column in table_schema])

        dst_cur.execute(f"CREATE TABLE IF NOT EXISTS {transfer_table} ({columns_str});")
        dst_cur.execute(
            f"ALTER TABLE {transfer_table} ADD COLUMN IF NOT EXISTS {id_column} BIGINT;"
        )

        logger.info(f"Created table '{transfer_table}' in destination database")

        dst_cur.execute(
            f"CREATE INDEX IF NOT EXISTS {id_column} ON {transfer_table}({id_column});"
        )
        logger.info(f"Created index for column '{id_column}' on '{transfer_table}'")

        dst_cur.execute(
            f"CREATE SUBSCRIPTION {subscription} CONNECTION '{src_conn_string}' PUBLICATION {publication} WITH (slot_name = %s);",
            slot_name_generator(),
        )
        logger.info(
            f"Created subscription '{subscription}' for publication '{publication}'"
        )

        logger.info(
            f"Successfully completed preparation of destination table '{transfer_table}'"
        )
    except psycopg2.Error as err:
        logger.error(f"Failed to prepare destination table '{transfer_table}': {err}")
        raise

    finally:
        dst_cur.close()


def prepare_all_tables(
    src_conn: psycopg2.extensions.connection,
    dst_conn: typing.Optional[db_connector.MultiClusterConnection],
    src_conn_string: typing.Optional[str],
    src_table: str,
    transfer_table: str,
    proccesed_column: str,
    id_column,
    publication: typing.Optional[str],
    subscription: typing.Optional[str],
    transfer_table_schema,
    with_replication: bool,
):

    prepare_src_table(src_conn, src_table, proccesed_column)
    prepare_transfer_table(
        src_conn,
        src_table,
        transfer_table,
        id_column,
        publication,
        transfer_table_schema,
        with_replication,
    )

    if with_replication:
        if dst_conn is None:
            raise Exception("Dst_conn is None, but mode is with replication")
        if src_conn_string is None:
            raise Exception("Src_conn_string is None, but mode is with replication")
        if publication is None:
            raise Exception("Publication is None, but mode is with replication")
        if subscription is None:
            raise Exception("Subscription is None, but mode is with replication")

        prepare_dst_table(
            dst_conn,
            src_conn_string,
            transfer_table,
            id_column,
            publication,
            subscription,
            transfer_table_schema,
        )
