import logging
import psycopg2
import time

from src.monitoring.metrics import get_metrics_collector
from src.utils import db_connector


logger = logging.getLogger(__name__)


def remove_replicated_records(
    src_conn: psycopg2.extensions.connection,
    dst_conn: db_connector.MultiClusterConnection,
    transfer_table: str,
    id_column: str,
    period_s: int,
    stop_event,
):
    while True:
        try:
            dst_cur = dst_conn.cursor()
            src_cur = src_conn.cursor()

            dst_cur.execute(f"SELECT MAX({id_column}) FROM {transfer_table}")
            rpl_cnts = dst_cur.fetchone()

            metrics_array = [cnt[0] if cnt[0] is not None else 0 for cnt in rpl_cnts]
            get_metrics_collector().add_metrics_array('total_cnt', metrics_array, dst_conn.get_hosts())

            max_id = None
            if all([cnt[0] is not None for cnt in rpl_cnts]):
                max_id = min([cnt[0] for cnt in rpl_cnts])

            if max_id is not None:
                src_cur.execute(
                    f"DELETE FROM {transfer_table} WHERE {id_column} <= {max_id}"
                )
                deleted_count = src_cur.rowcount
                logger.debug(
                    f"Records in {transfer_table} with {id_column} <= {max_id} deleted. Count: {deleted_count}"
                )
                get_metrics_collector().increment_metric("total_deleted", deleted_count)

                if deleted_count == 0 and stop_event.is_set():
                    break
            else:
                logger.debug(f"No data to delete in the table {transfer_table}.")

            if period_s > 0:
                time.sleep(period_s)
        except psycopg2.Error as err:
            logger.error(f"Error deleting records: {err}")
            raise

        finally:
            dst_cur.close()
            src_cur.close()
