import logging
import psycopg2
import typing

from src.utils import db_connector


logger = logging.getLogger(__name__)


def src_cleanup_script_helpers(
    conn: psycopg2.extensions.connection,
    src_table: str,
    proccesed_column: str,
    transfer_table: str,
    id_column: str,
    publication: typing.Optional[str],
    with_replication: bool,
    after_except: bool,
):
    if after_except:
        conn.rollback()

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

        if with_replication:
            if publication is None:
                raise Exception("Publication is None, but mode is with replication")

            cur.execute(f"DROP PUBLICATION {ifExistsClause} {publication}")
            logger.info(f"Dropped publication '{publication}'")

            cur.execute(f"DROP TABLE {ifExistsClause} {transfer_table} CASCADE;")
            logger.info(f"Dropped table '{transfer_table}'")
        else:
            cur.execute(
                f"ALTER TABLE {transfer_table} DROP COLUMN {ifExistsClause} {id_column} CASCADE;"
            )
            logger.info(f"Dropped column '{id_column}' from table '{transfer_table}'")

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
    if after_except:
        conn.rollback()

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

        cur.execute(f"ALTER TABLE {transfer_table} DROP COLUMN {id_column} CASCADE;")
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
    dst_conn: typing.Optional[db_connector.MultiClusterConnection],
    src_table: str,
    id_column: str,
    processed_column: str,
    transfer_table: str,
    publication: typing.Optional[str],
    subscription: typing.Optional[str],
    with_replication: bool,
    after_except: bool = False,
):
    src_cleanup_script_helpers(
        src_conn,
        src_table,
        processed_column,
        transfer_table,
        id_column,
        publication,
        with_replication,
        after_except,
    )

    if with_replication:
        if dst_conn is None:
            raise Exception("Dst conn is None, but mode is with replication")
        if subscription is None:
            raise Exception("Subscription is None, but mode is with replication")

        dst_cleanup_script_helpers(
            dst_conn,
            transfer_table,
            id_column,
            subscription,
            after_except,
        )
