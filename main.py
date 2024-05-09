import argparse
import logging
import multiprocessing
from pathlib import Path

import psycopg2

from src import monitoring, names, transform
from src import utils
from src.preparations import cleanup_helpers, preparations
from src.replication_cleanup import replication_cleanup
from src.settings import load_settings, get_settings, get_processing_settings


logger = None


def setup_logger_settings():
    global logger

    settings = get_settings()
    if "logs_dir" in settings:
        logs_dir = settings["logs_dir"]
        dir_path = Path(logs_dir)
        dir_path.mkdir(parents=True, exist_ok=True)

        monitoring.log_config.setup_logging(
            logs_dir + "/logs.log", logs_dir + "/error_logs.log"
        )
    else:
        monitoring.log_config.setup_logging(None, None, True)

    logger = logging.getLogger(__name__)


def get_transformer(conn: psycopg2.extensions.connection):
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


def cleanup(src_conn, dst_conn, after_exception=False):
    cleanup_helpers.cleanup_script_helpers(
        src_conn,
        dst_conn,
        names.SRC_TABLE,
        names.ID_COLUMN,
        names.PROCCESED_COLUMN,
        names.TRANSFER_TABLE,
        names.PUBLICATION,
        names.SUBSCRIPTION,
        after_except=after_exception,
    )


def prepare_all_tables(src_conn, dst_conn, connector, transfer_table_schema):
    preparations.prepare_all_tables(
        src_conn,
        dst_conn,
        connector.get_src_conn_string(),
        names.SRC_TABLE,
        names.TRANSFER_TABLE,
        names.PROCCESED_COLUMN,
        names.ID_COLUMN,
        names.PUBLICATION,
        names.SUBSCRIPTION,
        transfer_table_schema,
    )


def process():
    logger.info("Start of work")

    settings = get_processing_settings()

    connector = utils.db_connector.DatabaseConnector()
    src_conn = connector.get_src_connection()
    dst_conn = connector.get_dst_connection()

    stop_event = multiprocessing.Event()

    try:
        logger.info("Starting preparations")

        transform = get_transformer(src_conn)
        transfer_table_schema = transform.get_transfer_table_schema()

        prepare_all_tables(
            src_conn,
            dst_conn,
            connector,
            transfer_table_schema,
        )
        logger.info("Preparations completed successfully")

        proc_to_remove = multiprocessing.Process(
            target=replication_cleanup.remove_replicated_records,
            args=(
                connector.get_src_connection(),
                connector.get_dst_connection(),
                names.TRANSFER_TABLE,
                names.ID_COLUMN,
                settings["delete_sleep_s"],
                stop_event,
            ),
        )
        proc_to_remove.start()
        logger.info("Starting remove replicated process")

        transform.process()

        stop_event.set()
        proc_to_remove.join()

        logger.info("Starting final cleanup")
        cleanup(src_conn, dst_conn)
        logger.info("Final cleanup completed successfully")

    except (KeyboardInterrupt, Exception) as err:
        logger.error(f"Error during execution: {err}")
        stop_event.set()

        logger.info("Starting cleanup after error")
        cleanup(src_conn, dst_conn, after_exception=True)
        logger.info("Cleanup after error completed successfully")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("settings", type=str, help="Path to the configuration file.")
    args = parser.parse_args()

    load_settings(args.settings)
    # load_settings('AnonymizePG/example/shuffle_settings.json')

    monitoring.metrics.LazyMetricsCollector()
    setup_logger_settings()
    process()


if __name__ == "__main__":
    main()
