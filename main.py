import logging
import multiprocessing
from pathlib import Path

from src.settings import load_settings, get_settings

load_settings()

from src.utils.db_connector import DatabaseConnector
import src.preparations.preparation
import src.preparations.cleanup_helpers
import src.monitoring.log_config
import src.replication_cleanup.replication_cleanup
from src.transform import shuffler, copier
from src.names import *


logger = None


def setup_settings():
    global logger

    settings = get_settings()
    if "logs_dir" in settings:
        logs_dir = settings["logs_dir"]
        dir_path = Path(logs_dir)
        dir_path.mkdir(parents=True, exist_ok=True)

        src.monitoring.log_config.setup_logging(
            logs_dir + "/logs.log", logs_dir + "/error_logs.log"
        )
    else:
        src.monitoring.log_config.setup_logging(None, None, True)

    logger = logging.getLogger(__name__)


def process():
    logger.info("Start of work")

    connector = DatabaseConnector()

    src_conn = connector.get_src_connection()
    dst_conn = connector.get_dst_connection()

    try:
        logger.info("Starting preparations")
        src.preparations.preparation.prepare_src_table(
            src_conn, SRC_TABLE, PROCCESED_COLUMN
        )
        src.preparations.preparation.prepare_transfer_table(
            src_conn, SRC_TABLE, TRANSFER_TABLE, ID_COLUMN, PUBLICATION
        )
        src.preparations.preparation.prepare_dst_table(
            src_conn,
            dst_conn,
            connector.get_src_conn_string(),
            TRANSFER_TABLE,
            PROCCESED_COLUMN,
            ID_COLUMN,
            PUBLICATION,
            SUBSCRIPTION,
        )
        logger.info("Preparations completed successfully")

        period_s = 1
        stop_event = multiprocessing.Event()

        proc_to_remove = multiprocessing.Process(
            target=src.replication_cleanup.replication_cleanup.remove_replicated_records,
            args=(
                connector.get_src_connection(),
                connector.get_dst_connection(),
                TRANSFER_TABLE,
                ID_COLUMN,
                period_s,
                stop_event,
            ),
        )
        proc_to_remove.start()

        batch_size = 5
        # groups = get_settings()["processing_settings"]["groups"]
        # trans = shuffler.Shuffler(
        #     src_conn,
        #     SRC_TABLE,
        #     TRANSFER_TABLE,
        #     PROCCESED_COLUMN,
        #     batch_size,
        #     1000,
        #     groups,
        # )

        trans = copier.Copier(
            src_conn,
            SRC_TABLE,
            TRANSFER_TABLE,
            PROCCESED_COLUMN,
            batch_size,
            1000,
        )

        trans()

        stop_event.set()
        proc_to_remove.join()

        src.preparations.cleanup_helpers.cleanup_script_helpers(
            src_conn,
            dst_conn,
            SRC_TABLE,
            ID_COLUMN,
            PROCCESED_COLUMN,
            TRANSFER_TABLE,
            PUBLICATION,
            SUBSCRIPTION,
        )

    except Exception as err:
        logger.error(f"{err}")
        stop_event.set()
        src.preparations.cleanup_helpers.cleanup_script_helpers(
            src_conn,
            dst_conn,
            SRC_TABLE,
            ID_COLUMN,
            PROCCESED_COLUMN,
            TRANSFER_TABLE,
            PUBLICATION,
            SUBSCRIPTION,
            after_except=True,
        )


if __name__ == "__main__":
    setup_settings()

    process()
