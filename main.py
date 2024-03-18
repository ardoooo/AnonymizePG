import logging
import multiprocessing

from src.db_connector import DatabaseConnector
import src.preparation
import src.cleanup_helpers
import src.log_config
import src.filling_transfer_table
import src.removing_replicated

src.log_config.setup_logging()

logger = logging.getLogger(__name__)


ID_COLUMN = "__id__"
PROCCESED_COLUMN = "__processed__"
SRC_TABLE = "workers"
TRANSFER_TABLE = "_transfer_" + SRC_TABLE
PUBLICATION = TRANSFER_TABLE + "_pub"
SUBSCRIPTION = TRANSFER_TABLE + "_sub"


logger.info("Start of work")

connector = DatabaseConnector()

src_conn = connector.get_src_connection()
dst_conn = connector.get_dst_connection()

try:
    logger.info("Starting preparations")
    src.preparation.prepare_src_table(src_conn, SRC_TABLE, PROCCESED_COLUMN)
    src.preparation.prepare_transfer_table(
        src_conn, SRC_TABLE, TRANSFER_TABLE, ID_COLUMN, PUBLICATION
    )
    src.preparation.prepare_dst_table(
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

    period_s = 0.05
    stop_event = multiprocessing.Event()

    proc_to_remove = multiprocessing.Process(
        target=src.removing_replicated.remove_replicated_records,
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
    src.filling_transfer_table.fill_transfer_table(
        src_conn, SRC_TABLE, TRANSFER_TABLE, batch_size, PROCCESED_COLUMN
    )

    stop_event.set()
    proc_to_remove.join()

    src.cleanup_helpers.cleanup_script_helpers(
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
    src.cleanup_helpers.cleanup_script_helpers(
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
