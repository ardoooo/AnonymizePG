import logging
import psycopg2
import typing

from src.utils import utils
from src.transform.transformer import Transformer


logger = logging.getLogger(__name__)


class Copier(Transformer):
    def __init__(
        self,
        conn: psycopg2.extensions.connection,
        src_table: str,
        transfer_table: str,
        processed_column: str,
        batch_size: int,
        sleep_ms: int,
    ):
        super().__init__(
            conn, src_table, transfer_table, processed_column, batch_size, sleep_ms
        )

        with self.conn.cursor() as cur:
            columns = utils.get_columns(cur, src_table)
            self.column_types = {column[0]: column[1] for column in columns}
            self.columns = [column[0] for column in columns]

        self.new_type = None
        self.new_func = None

    def get_funcs(self):
        return [self.new_func]

    def prepare(self):
        type_name = "_type_" + utils.join_names(self.columns, "_")
        self.new_type = type_name

        fields = [f"{column} {self.column_types[column]}" for column in self.columns]
        fields_str = ",\n".join(fields)

        create_type_query = f"""
            CREATE TYPE {type_name} AS (
            {fields_str}
        );
        """
        with self.conn.cursor() as cur:
            cur.execute(create_type_query)

        select_func_name = "select_" + utils.join_names(self.columns, "_")
        self.new_func = select_func_name

        select_func_query = f"""
            CREATE OR REPLACE FUNCTION {select_func_name}()
            RETURNS SETOF {type_name} AS $$
            BEGIN
                RETURN QUERY SELECT {utils.join_names(self.columns)} FROM {self.src_table} s
                JOIN {self.temp_table_name} t ON s.ctid = t._ctid_;
            END;
            $$ LANGUAGE plpgsql;"""

        with self.conn.cursor() as cur:
            cur.execute(select_func_query)

        logger.debug("Copier preparation successfully completed")

    def cleanup(self):
        drop_types_query = f"DROP TYPE IF EXISTS {self.new_type} CASCADE;"
        drop_funcs_query = f"DROP FUNCTION IF EXISTS {self.new_func} CASCADE;"

        with self.conn.cursor() as cur:
            cur.execute(drop_types_query)
            cur.execute(drop_funcs_query)

    logger.debug("Copier cleanup successfully completed")
