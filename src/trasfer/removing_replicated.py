import logging
import psycopg2
import time

from src.monitoring import metrics


logger = logging.getLogger(__name__)
metrics = metrics.MetricsCollector()


def remove_replicated_records(
    src_conn: psycopg2.extensions.connection,
    dst_conn: psycopg2.extensions.connection,
    transfer_table: str,
    id_column: str,
    period_s: int,
    stop_event,
):
    while not stop_event.is_set():
        try:
            dst_cur = dst_conn.cursor()
            src_cur = src_conn.cursor()

            dst_cur.execute(f"SELECT MAX({id_column}) FROM {transfer_table}")
            max_id = dst_cur.fetchone()[0]

            if max_id is not None:
                src_cur.execute(f"DELETE FROM {transfer_table} WHERE {id_column} <= {max_id}")
                deleted_count = src_cur.rowcount
                logger.debug(
                    f"Records in {transfer_table} with {id_column} <= {max_id} deleted. Count: {deleted_count}"
                )
                metrics.increment_metric("total_deleted", deleted_count)
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
