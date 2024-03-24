import logging
import psycopg2
import time

from src.monitoring import metrics


logger = logging.getLogger(__name__)
metrics = metrics.MetricsCollector()


def create_temp_table(cur: psycopg2.extensions.cursor, temp_table_name: str):
    try:
        create_temp_table_query = f"CREATE TEMP TABLE {temp_table_name} (_ctid_ tid);"
        cur.execute(create_temp_table_query)
        logger.debug(f"Temporary table '{temp_table_name}' created.")
    except psycopg2.Error as err:
        logger.error(f"Error creating temporary table '{temp_table_name}': {err}")
        raise


def select_ctids(
    cur: psycopg2.extensions.cursor,
    temp_table_name: str,
    src_table: str,
    processed_column: str,
    batch_size: int,
):
    try:
        select_ctids_query = f"""
        INSERT INTO {temp_table_name} (_ctid_)
        SELECT ctid
        FROM {src_table}
        WHERE {processed_column} IS NULL
        LIMIT {batch_size};
        """
        cur.execute(select_ctids_query)
        return cur.rowcount
    except psycopg2.Error as err:
        logger.error(f"Error inserting CTIDs into '{temp_table_name}': {err}")
        raise


def convert_data(
    cur: psycopg2.extensions.cursor,
    src_table: str,
    transfer_table: str,
    temp_table_name: str,
):
    try:
        insert_query = f"""
        INSERT INTO {transfer_table}
        SELECT s.*
        FROM {src_table} s
        JOIN {temp_table_name} t ON s.ctid = t._ctid_;
        """
        cur.execute(insert_query)
        return cur.rowcount
    except psycopg2.Error as err:
        logger.error(f"Error converting data to '{transfer_table}': {err}")
        raise


def mark_processed(
    cur: psycopg2.extensions.cursor,
    src_table: str,
    temp_table_name: str,
    processed_column: str,
):
    try:
        update_query = f"""
        UPDATE {src_table}
        SET {processed_column} = TRUE
        WHERE ctid IN (SELECT _ctid_ FROM {temp_table_name});
        """
        cur.execute(update_query)
        return cur.rowcount
    except psycopg2.Error as err:
        logger.error(f"Error mark processed in '{src_table}': {err}")
        raise


def clear_ctids(cur: psycopg2.extensions.cursor, temp_table_name: str):
    try:
        cur.execute(f"TRUNCATE TABLE {temp_table_name};")
        logger.debug(f"Temporary table '{temp_table_name}' truncated")
    except psycopg2.Error as err:
        logger.error(f"Error truncating temporary table '{temp_table_name}': {err}")
        raise


def fill_transfer_table(
    conn: psycopg2.extensions.connection,
    src_table: str,
    transfer_table: str,
    batch_size: str,
    processed_column: int,
    sleep_ms: int = 0,
):
    logger.info(
        f"Starting to fill from '{src_table}' to '{transfer_table}', batch_size: {batch_size}"
    )
    conn.autocommit = False
    cur = conn.cursor()

    temp_table_name = "temp_ctid_holder"
    create_temp_table(cur, temp_table_name)

    iteration = 0

    while True:
        selected = select_ctids(
            cur, temp_table_name, src_table, processed_column, batch_size
        )
        metrics.increment_metric('total_selected_ctids', selected)

        if selected == 0:
            conn.commit()
            logger.info("All records have been processed.")
            break

        converted = convert_data(cur, src_table, transfer_table, temp_table_name)
        metrics.increment_metric('total_converted', converted)

        processed = mark_processed(cur, src_table, temp_table_name, processed_column)
        metrics.increment_metric('total_mark_processed', processed)

        clear_ctids(cur, temp_table_name)

        conn.commit()

        iteration += 1
        logger.debug(
            f"Iteration: {iteration})"
        )

        if sleep_ms > 0:
            logger.debug(f"Sleep {sleep_ms} ms")
            time.sleep(sleep_ms / 1000)

    conn.commit()
    conn.autocommit = True

    logger.info("Successfully completed the copying of records")
