import argparse
import logging
import multiprocessing
import psycopg2

import src.log_config
from src.settings import load_settings, get_processing_settings


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("settings", type=str, help="Path to the configuration file.")
    args = parser.parse_args()

    load_settings(args.settings)
    src.log_config.setup_logger_settings()


from src import names, transform
from src import utils
from src.preparations import cleanup_helpers, preparations
from src.replication_cleanup import replication_cleanup


logger = logging.getLogger(__name__)


def build_transformer(conn: psycopg2.extensions.connection):
    settings = get_processing_settings()

    common_settings = {
        "conn": conn,
        "src_table": names.SRC_TABLE,
        "transfer_table": names.TRANSFER_TABLE,
        "processed_column": names.PROCCESED_COLUMN,
        "continuous_mode": settings.get("continuous_mode", False),
        "batch_size": settings["batch_size"],
        "sleep_ms": settings["batch_sleep_ms"],
    }

    method = settings["method"]

    if method == "copy":
        common_settings["columns"] = settings["columns"]
        return transform.copier.Copier(**common_settings)
    elif method == "aggr":
        common_settings["column_operations"] = settings["column_operations"]
        return transform.aggregator.Aggregator(**common_settings)
    elif method == "reduce_aggr":
        common_settings["column_operations"] = settings["column_operations"]
        return transform.reduce_aggregator.ReduceAggregator(**common_settings)
    elif method == "shuffle":
        common_settings["groups"] = settings["groups"]
        return transform.shuffler.Shuffler(**common_settings)
    elif method == "select_random":
        common_settings["groups"] = settings["groups"]
        return transform.random_selector.RandomSelector(**common_settings)
    elif method == "uuid":
        common_settings["column_operations"] = settings["column_operations"]
        return transform.uuid_replacer.UuidReplacer(**common_settings)


def cleanup(src_conn, after_except=False):
    cleanup_helpers.src_cleanup_script_helpers(
        src_conn,
        names.SRC_TABLE,
        names.PROCCESED_COLUMN,
        names.TRANSFER_TABLE,
        names.ID_COLUMN,
        None,
        False,
        after_except,
    )


def prepare_all_tables(src_conn, transfer_table_schema):
    preparations.prepare_all_tables(
        src_conn,
        None,
        None,
        names.SRC_TABLE,
        names.TRANSFER_TABLE,
        names.PROCCESED_COLUMN,
        names.ID_COLUMN,
        None,
        None,
        transfer_table_schema,
        False,
    )


def process():
    logger.info("Start of work")

    connector = utils.db_connector.DatabaseConnector(only_src=True)
    src_conn = connector.get_src_connection()

    try:
        logger.info("Starting preparations")

        transform = build_transformer(src_conn)
        transfer_table_schema = transform.get_transfer_table_schema()

        prepare_all_tables(
            src_conn,
            transfer_table_schema,
        )
        logger.info("Preparations completed successfully")

        transform.process()

        logger.info("Starting final cleanup")
        cleanup(src_conn)
        logger.info("Final cleanup completed successfully")

    except KeyboardInterrupt as err:
        logger.info("Get KeyboardInterrupt")

        logger.info("Starting cleanup after KeyboardInterrupt")
        cleanup(src_conn, after_except=True)
        logger.info("Cleanup after KeyboardInterrupt completed successfully")

    except Exception as err:
        logger.error(f"Error during execution: {err}")

        logger.info("Starting cleanup after error")
        cleanup(src_conn, after_except=True)
        logger.info("Cleanup after error completed successfully")


if __name__ == "__main__":
    process()
