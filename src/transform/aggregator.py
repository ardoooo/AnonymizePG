import logging
import psycopg2
import typing

from src.utils import utils
from src.transform.transformer import Transformer


logger = logging.getLogger(__name__)


class Aggregator(Transformer):
    def __init__(
        self,
        conn: psycopg2.extensions.connection,
        src_table: str,
        transfer_table: str,
        processed_column: str,
        batch_size: int,
        sleep_ms: int,
        column_operations: typing.Dict[str, str],
    ):
        super().__init__(
            conn, src_table, transfer_table, processed_column, batch_size, sleep_ms
        )
        self.column_operations = column_operations

        self.new_types = []
        self.new_funcs = []

    def get_transfer_table_schema(self):
        column_names = self.column_operations.keys()
        return [(column, self.column_types[column]) for column in column_names]

    def get_funcs(self):
        return self.new_funcs

    def prepare(self):
        column_funcs = []
        columns = []
        for column, method in self.column_operations.items():
            columns.append(column)

            if method == "echo":
                column_funcs.append(column)
                continue

            func = f"({method}({column}) OVER())::{self.column_types[column]}"
            column_funcs.append(func)

        type_name = "_type_" + utils.join_names(columns, "_")
        self.new_types.append(type_name)

        fields = [f"{column} {self.column_types[column]}" for column in columns]
        fields_str = ",\n".join(fields)

        create_type_query = f"""
            CREATE TYPE {type_name} AS (
            {fields_str}
        );
        """
        with self.conn.cursor() as cur:
            cur.execute(create_type_query)

        func_name = "_aggregate_" + utils.join_names(columns, "_")
        self.new_funcs.append(func_name)

        create_func_query = f"""
            CREATE OR REPLACE FUNCTION {func_name}()
            RETURNS SETOF {type_name} AS $$
            BEGIN
                RETURN QUERY SELECT {utils.join_names(column_funcs)} FROM {self.src_table} s
                JOIN {self.temp_table_name} t ON s.ctid = t._ctid_;
            END;
            $$ LANGUAGE plpgsql;"""

        with self.conn.cursor() as cur:
            cur.execute(create_func_query)

        logger.debug("Aggregator preparation successfully completed")

    def cleanup(self):
        type_str = ", ".join(self.new_types)
        drop_types_query = f"DROP TYPE IF EXISTS {type_str} CASCADE;"

        funcs_str = ", ".join(self.new_funcs)
        drop_funcs_query = f"DROP FUNCTION IF EXISTS {funcs_str} CASCADE;"

        with self.conn.cursor() as cur:
            cur.execute(drop_types_query)
            cur.execute(drop_funcs_query)

        logger.debug("Aggregator cleanup successfully completed")
