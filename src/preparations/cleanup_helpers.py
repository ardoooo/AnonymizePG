import logging
import psycopg2


logger = logging.getLogger(__name__)


def src_cleanup_script_helpers(
    conn: psycopg2.extensions.connection,
    src_table: str,
    proccesed_column: str,
    transfer_table: str,
    publication: str,
    after_except: bool,
):
    logger.info(
        f"Starting cleanup for source-related script helpers, after exception: {after_except}"
    )
    conn.autocommit = True
    cur = conn.cursor()

    ifExistsClause = "IF EXISTS" if after_except else ""
    try:
        cur.execute(f"DROP INDEX CONCURRENTLY {ifExistsClause} {proccesed_column};")
        logger.info(f"Dropped index for column '{proccesed_column}'")

        cur.execute(
            f"ALTER TABLE {src_table} DROP COLUMN {ifExistsClause} {proccesed_column};"
        )
        logger.info(f"Dropped column '{proccesed_column}' from table '{src_table}'")

        cur.execute(f"DROP PUBLICATION {ifExistsClause} {publication}")
        logger.info(f"Dropped publication '{publication}'")

        cur.execute(f"DROP TABLE {ifExistsClause} {transfer_table} CASCADE;")
        logger.info(f"Dropped table '{transfer_table}'")

        logger.info("Successfully completed cleanup for source-related script helpers.")

    except psycopg2.Error as err:
        logger.error(
            f"An error occurred while cleaning up source-related script helpers: {err}"
        )
        raise

    finally:
        cur.close()


def dst_cleanup_script_helpers(
    conn: psycopg2.extensions.connection,
    transfer_table: str,
    id_column: str,
    subscription: str,
    after_except: bool,
):
    logger.info(
        f"Starting cleanup for destination-related script helpers, after exception: {after_except}"
    )
    conn.autocommit = True
    cur = conn.cursor()

    try:
        cur.execute(
            f"DROP SUBSCRIPTION {'IF EXISTS' if after_except else ''} {subscription};"
        )
        logger.info(f"Dropped subscription '{subscription}'")

        cur.execute(f"DROP INDEX CONCURRENTLY {id_column};")
        logger.info(f"Dropped index for column '{id_column}'")

        cur.execute(
            f"ALTER TABLE {transfer_table} DROP COLUMN {id_column} CASCADE;"
        )
        logger.info(f"Dropped column '{id_column}' from table '{transfer_table}'")

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
    id_column: str,
    processed_column: str,
    transfer_table: str,
    publication: str,
    subscription: str,
    after_except: bool = False,
):
    if after_except:
        src_conn.rollback()
        dst_conn.rollback()

    src_cleanup_script_helpers(
        src_conn, src_table, processed_column, transfer_table, publication, after_except
    )
    dst_cleanup_script_helpers(
        dst_conn,
        transfer_table,
        id_column,
        subscription,
        after_except,
    )
