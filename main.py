import logging
import multiprocessing

from src.database_connector import DatabaseConnector
import src.preparation
import src.script_cleanup
import src.log_config
import src.filling_transfer_table
import src.removing_replicated

src.log_config.setup_logging()

logger = logging.getLogger(__name__)


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
        src_conn, SRC_TABLE, TRANSFER_TABLE, PUBLICATION
    )
    src.preparation.prepare_dst_table(
        src_conn,
        dst_conn,
        connector.get_src_conn_string(),
        TRANSFER_TABLE,
        PUBLICATION,
        SUBSCRIPTION,
    )
    logger.info("Preparations completed successfully")

    period_s = 2
    stop_event = multiprocessing.Event()

    proc_to_remove = multiprocessing.Process(
        target=src.removing_replicated.remove_replicated_records,
        args=(
            connector.get_src_connection(),
            connector.get_dst_connection(),
            TRANSFER_TABLE,
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

except Exception as err:
    logger.error(f"{err}")
    src.script_cleanup.cleanup_script_helpers(
        src_conn,
        dst_conn,
        SRC_TABLE,
        PROCCESED_COLUMN,
        TRANSFER_TABLE,
        PUBLICATION,
        SUBSCRIPTION,
    )

src.script_cleanup.cleanup_script_helpers(
    src_conn,
    dst_conn,
    SRC_TABLE,
    PROCCESED_COLUMN,
    TRANSFER_TABLE,
    PUBLICATION,
    SUBSCRIPTION,
)
