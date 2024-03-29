import logging
import psycopg2
import time
from abc import ABC, abstractmethod

from src.monitoring import metrics
from src.utils import utils


logger = logging.getLogger(__name__)
metrics = metrics.MetricsCollector()


class Transformer(ABC):
    def __init__(
        self,
        conn: psycopg2.extensions.connection,
        src_table: str,
        transfer_table: str,
        processed_column: str,
        batch_size: int,
        sleep_ms: int,
    ):
        self.conn = conn
        self.src_table = src_table
        self.transfer_table = transfer_table
        self.temp_table_name = "temp_ctid_holder"
        self.processed_column = processed_column
        self.batch_size = batch_size
        self.sleep_ms = sleep_ms

        self.func_names = None

    def __del__(self):
        if not self.conn.closed:
            self.conn.autocommit = True

    @abstractmethod
    def prepare(self):
        pass

    @abstractmethod
    def get_funcs(self):
        pass

    @abstractmethod
    def cleanup(self):
        pass

    def create_temp_table(self):
        try:
            create_temp_table_query = (
                f"CREATE TEMP TABLE {self.temp_table_name} (_ctid_ tid);"
            )
            with self.conn.cursor() as cur:
                cur.execute(create_temp_table_query)
            logger.debug(f"Temporary table '{self.temp_table_name}' created.")
        except psycopg2.Error as err:
            logger.error(
                f"Error creating temporary table '{self.temp_table_name}': {err}"
            )
            raise

    def select_ctids(self):
        try:
            select_ctids_query = f"""
            INSERT INTO {self.temp_table_name} (_ctid_)
            SELECT ctid
            FROM {self.src_table}
            WHERE {self.processed_column} IS NULL
            LIMIT {self.batch_size};
            """

            rowcount = None
            with self.conn.cursor() as cur:
                cur.execute(select_ctids_query)
                rowcount = cur.rowcount
            return rowcount
        except psycopg2.Error as err:
            logger.error(f"Error inserting CTIDs into '{self.temp_table_name}': {err}")
            raise

    def insert_into_transfer_table(self):
        try:
            func_names = [f"({name}()).*" for name in self.get_funcs()]
            funcs_str = utils.join_names(func_names, ", ")

            insert_query = f"""
                INSERT INTO {self.transfer_table}
                SELECT {funcs_str};
            """

            rowcount = None
            with self.conn.cursor() as cur:
                cur.execute(insert_query)
                rowcount = cur.rowcount
            return rowcount
        except psycopg2.Error as err:
            logger.error(f"Error transferring data to '{self.transfer_table}': {err}")
            raise

    def mark_processed(self):
        try:
            update_query = f"""
            UPDATE {self.src_table}
            SET {self.processed_column} = TRUE
            WHERE ctid IN (SELECT _ctid_ FROM {self.temp_table_name});
            """

            rowcount = None
            with self.conn.cursor() as cur:
                cur.execute(update_query)
                rowcount = cur.rowcount
            return rowcount
        except psycopg2.Error as err:
            logger.error(
                f"Error marking records as processed in '{self.src_table}': {err}"
            )
            raise

    def truncate_stids_table(self):
        try:
            truncate_temp_table_query = f"TRUNCATE TABLE {self.temp_table_name};"
            with self.conn.cursor() as cur:
                cur.execute(truncate_temp_table_query)
            logger.debug(f"Temporary table '{self.temp_table_name}' truncated")
        except psycopg2.Error as err:
            logger.error(
                f"Error truncating temporary table '{self.temp_table_name}': {err}"
            )
            raise

    def process(self):
        logger.info("Data transform process started")

        try:
            self.conn.autocommit = False

            self.prepare()
            self.create_temp_table()
            self.conn.commit()

            while True:
                selected = self.select_ctids()
                metrics.increment_metric("total_selected_ctids", selected)

                if selected == 0:
                    self.conn.commit()
                    break

                converted = self.insert_into_transfer_table()
                metrics.increment_metric("total_converted", converted)

                processed = self.mark_processed()
                self.conn.commit()
                metrics.increment_metric("total_mark_processed", processed)

                self.truncate_stids_table()
                self.conn.commit()
                logger.debug("Completed iteration")

            if self.sleep_ms > 0:
                logger.debug(f"Sleep {self.sleep_ms} ms")
                time.sleep(self.sleep_ms / 1000)

        except psycopg2.Error as err:
            self.conn.rollback()
            self.conn.autocommit = True
            self.cleanup()
            raise

        logger.info("Data transform process successfully completed")

        self.conn.autocommit = True
        self.cleanup()

    def __call__(self):
        return self.process()
