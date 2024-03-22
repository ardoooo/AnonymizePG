import logging
import psycopg2

import src.utils.utils


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
    publication: str,
):
    logger.info(f"Starting to prepare '{transfer_table}' based on '{src_table}'.")
    cur = conn.cursor()

    try:
        columns = src.utils.utils.get_columns(cur, src_table)
        columns_str = ", ".join([f"{column[0]} {column[1]}" for column in columns])

        cur.execute(
            f"CREATE TABLE {transfer_table} ({columns_str}, {id_column} BIGSERIAL PRIMARY KEY)"
        )
        logger.info(f"Created table '{transfer_table}'")

        cur.execute(
            f"CREATE PUBLICATION {publication} FOR TABLE {transfer_table} WITH (publish = 'insert')"
        )
        logger.info(f"Created publication '{publication}' for table '{transfer_table}'")

        logger.info(f"Successfully completed preparation of '{transfer_table}'")
    except psycopg2.Error as err:
        logger.error(f"Failed to prepare transfer table '{transfer_table}': {err}")
        raise

    finally:
        cur.close()


def prepare_dst_table(
    src_conn: psycopg2.extensions.connection,
    dst_conn: psycopg2.extensions.connection,
    src_conn_string: str,
    transfer_table: str,
    proccesed_column: str,
    id_column,
    publication: str,
    subscription: str,
):
    logger.info(f"Starting to prepare destination table '{transfer_table}'")
    src_cur = src_conn.cursor()
    dst_cur = dst_conn.cursor()
    try:
        columns = src.utils.utils.get_columns(src_cur, transfer_table)
        columns_str = ", ".join([f"{column[0]} {column[1]}" for column in columns])

        dst_cur.execute(
            f"CREATE TABLE IF NOT EXISTS {transfer_table} ({columns_str});"
        )
        dst_cur.execute(f"ALTER TABLE {transfer_table} ADD COLUMN IF NOT EXISTS {proccesed_column} BOOLEAN;")
        dst_cur.execute(f"ALTER TABLE {transfer_table} ADD COLUMN IF NOT EXISTS {id_column} BOOLEAN;")

        logger.info(f"Created table '{transfer_table}' in destination database")



        dst_cur.execute(
            f"CREATE INDEX IF NOT EXISTS {id_column} ON {transfer_table}({id_column});"
        )
        logger.info(f"Created index for column '{id_column}' on '{transfer_table}'")

        dst_cur.execute(
            f"CREATE SUBSCRIPTION {subscription} CONNECTION '{src_conn_string}' PUBLICATION {publication};"
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
        src_cur.close()
        dst_cur.close()
