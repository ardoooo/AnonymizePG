import logging
import psycopg2


logger = logging.getLogger(__name__)


def src_cleanup_script_helpers(
    conn: psycopg2.extensions.connection,
    src_table: str,
    proccesed_column: str,
    transfer_table: str,
    publication: str,
):
    logger.info("Starting cleanup for source-related script helpers.")
    cur = conn.cursor()

    try:
        cur.execute(
            f"ALTER TABLE {src_table} DROP COLUMN IF EXISTS {proccesed_column};"
        )
        logger.info(
            f"Dropped column '{proccesed_column}' from table '{src_table}' if it exists."
        )

        cur.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {proccesed_column};")
        logger.info(f"Dropped index for column '{proccesed_column}' if it exists.")

        cur.execute(f"DROP PUBLICATION IF EXISTS {publication}")
        logger.info(f"Dropped publication '{publication}' if it exists.")

        cur.execute(f"DROP TABLE IF EXISTS {transfer_table} CASCADE;")
        logger.info(f"Dropped table '{transfer_table}' if it exists.")

        logger.info("Successfully completed cleanup for source-related script helpers.")

    except psycopg2.Error as err:
        logger.error(
            f"An error occurred while cleaning up source-related script helpers: {err}"
        )
        raise

    finally:
        cur.close()


def dst_cleanup_script_helpers(conn: psycopg2.extensions.connection, transfer_table: str, subscription: str):
    logger.info("Starting cleanup for destination-related script helpers.")
    cur = conn.cursor()

    try:
        cur.execute(f"DROP SUBSCRIPTION IF EXISTS {subscription};")
        logger.info(f"Dropped subscription '{subscription}' if it exists.")

        logger.info(
            "Successfully completed cleanup for destination-related script helpers"
        )

    except psycopg2.Error as err:
        logger.error(
            f"An error occurred while cleaning up destination-related script helpers: {err}"
        )
        raise

    finally:
        cur.close()


def cleanup_script_helpers(
    src_conn: psycopg2.extensions.connection,
    dst_conn: psycopg2.extensions.connection,
    src_table: str,
    proccesed_column: str,
    transfer_table: str,
    publication: str,
    subscription: str,
):
    src_cleanup_script_helpers(
        src_conn, src_table, proccesed_column, transfer_table, publication
    )
    dst_cleanup_script_helpers(dst_conn, transfer_table, subscription)
