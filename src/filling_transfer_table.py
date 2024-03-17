import logging
import psycopg2
import time


logger = logging.getLogger(__name__)


def fill_transfer_table(
    conn: psycopg2.extensions.connection,
    src_table: str,
    transfer_table: str,
    batch_size: int,
    processed_column: str,
):
    logger.info(
        f"Starting to fill from '{src_table}' to '{transfer_table}', batch_size: {batch_size}"
    )
    conn.autocommit = False
    cur = conn.cursor()

    temp_table_name = "temp_ctid_holder"
    create_temp_table_query = f"CREATE TEMP TABLE {temp_table_name} (_ctid_ tid);"
    cur.execute(create_temp_table_query)
    conn.commit()
    logger.info(f"Temporary table '{temp_table_name}' is ready for processing.")

    while True:
        select_ctids_query = f"""
        INSERT INTO {temp_table_name} (_ctid_)
        SELECT ctid
        FROM {src_table}
        WHERE {processed_column} IS NULL
        LIMIT {batch_size};
        """
        cur.execute(select_ctids_query)
        selected_rows = cur.rowcount
        logger.info(f"Selected {selected_rows} records from source table '{src_table}'")

        insert_query = f"""
        INSERT INTO {transfer_table}
        SELECT s.*
        FROM {src_table} s
        JOIN {temp_table_name} t ON s.ctid = t._ctid_;
        """
        cur.execute(insert_query)

        inserted_rows = cur.rowcount
        if inserted_rows == 0:
            conn.commit()
            logger.info("All records have been processed.")
            break

        logger.info(
            f"Transfered {inserted_rows} records from source table '{src_table}'"
        )

        update_query = f"""
        UPDATE {src_table}
        SET {processed_column} = TRUE
        WHERE ctid IN (SELECT _ctid_ FROM {temp_table_name});
        """
        cur.execute(update_query)
        conn.commit()
        updated_rows = cur.rowcount

        cur.execute(f"TRUNCATE TABLE {temp_table_name};")
        conn.commit()

        logger.info(
            f"Processed {updated_rows} records from the source table '{src_table}'"
        )
        time.sleep(0.5)

    conn.autocommit = True
    logger.info("Successfully completed the copying of records")
