import logging
import psycopg2
import typing

from src.utils import utils
from src.transform.transformer import Transformer


logger = logging.getLogger(__name__)


class UuidReplacer(Transformer):
    def __init__(
        self,
        conn: psycopg2.extensions.connection,
        src_table: str,
        transfer_table: str,
        processed_column: str,
        continuous_mode: bool,
        batch_size: int,
        sleep_ms: int,
        column_operations: typing.Dict[str, str],
    ):
        super().__init__(
            conn, src_table, transfer_table, processed_column, continuous_mode, batch_size, sleep_ms
        )
        self.column_operations = column_operations

        self.new_types = []
        self.new_funcs = []

        self.new_tables = []

        self.echo_columns = []
        self.uuid_columns = []
        for column, method in self.column_operations.items():
            if method == "echo":
                self.echo_columns.append(column)
            elif method == "uuid":
                self.uuid_columns.append(column)

    def get_transfer_table_schema(self):
        schema = []
        for column in self.echo_columns:
            schema.append((column, self.column_types[column]))
        for column in self.uuid_columns:
            schema.append((column, "UUID"))

        return schema

    def get_funcs(self):
        return self.new_funcs

    def prepare(self):
        echo_type = self.create_type_for_echo_columns()
        self.create_echo_func(echo_type)
        self.create_uuid_tables_and_functions()

        logger.info("UuidReplacer preparation successfully completed")

    def create_type_for_echo_columns(self):
        type_name = "_uuid_echo_" + utils.join_names(self.echo_columns, "_") + "_type"
        self.new_types.append(type_name)

        fields = [
            f"{column} {self.column_types[column]}" for column in self.echo_columns
        ]
        fields_str = ",\n".join(fields)

        create_type_query = f"CREATE TYPE {type_name} AS (\n{fields_str}\n);"
        with self.conn.cursor() as cur:
            cur.execute(create_type_query)

        return type_name

    def create_echo_func(self, ret_type: str):
        func_name = "_uuid_echo_" + utils.join_names(self.echo_columns, "_")
        self.new_funcs.append(func_name)

        create_func_query = f"""
            CREATE OR REPLACE FUNCTION {func_name}()
            RETURNS SETOF {ret_type} AS $$
            DECLARE
            rec RECORD;
            BEGIN
                RETURN QUERY
                    SELECT {utils.join_names(self.echo_columns)} FROM {self.src_table} s
                    JOIN {self.temp_table_name} t ON s.ctid = t._ctid_;
            END;
            $$ LANGUAGE plpgsql;
        """
        with self.conn.cursor() as cur:
            cur.execute(create_func_query)

    def create_uuid_tables_and_functions(self):
        for column in self.uuid_columns:
            self.create_uuid_table(column)
            type_name = self.create_uuid_type(column)
            self.create_uuid_function(column, type_name)

    def create_uuid_table(self, column):
        table_name = f"{self.transfer_table}_uuid_{column}"
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                uuid UUID NOT NULL,
                original_value {self.column_types[column]}
            );
        """
        with self.conn.cursor() as cur:
            cur.execute(create_table_query)
        self.new_tables.append(table_name)

    def create_uuid_type(self, column):
        type_name = f"{self.transfer_table}_uuid_{column}_type"
        self.new_types.append(type_name)

        create_type_query = f"CREATE TYPE {type_name} AS (\n{column} UUID\n);"
        with self.conn.cursor() as cur:
            cur.execute(create_type_query)

        return type_name

    def create_uuid_function(self, column, ret_type):
        func_name = f"{self.transfer_table}_uuid_{column}_function"
        self.new_funcs.append(func_name)
        table_name = f"{self.transfer_table}_uuid_{column}"

        create_func_query = f"""
            CREATE OR REPLACE FUNCTION {func_name}()
            RETURNS SETOF {ret_type} AS $$
            DECLARE
                new_uuid UUID;
                rec RECORD;
            BEGIN
                FOR rec IN
                    SELECT {column} FROM {self.src_table} s
                    JOIN {self.temp_table_name} t ON s.ctid = t._ctid_
                LOOP
                    INSERT INTO {table_name}(uuid, original_value)
                    VALUES (gen_random_uuid(), rec.{column})
                    RETURNING uuid INTO new_uuid;

                    RETURN NEXT new_uuid;
                END LOOP;
            END;
            $$ LANGUAGE plpgsql;
        """
        with self.conn.cursor() as cur:
            cur.execute(create_func_query)

    def cleanup(self):
        type_str = ", ".join(self.new_types)
        drop_types_query = f"DROP TYPE IF EXISTS {type_str} CASCADE;"

        funcs_str = ", ".join(self.new_funcs)
        drop_funcs_query = f"DROP FUNCTION IF EXISTS {funcs_str} CASCADE;"

        with self.conn.cursor() as cur:
            cur.execute(drop_types_query)
            cur.execute(drop_funcs_query)

    logger.info("UuidReplacer cleanup successfully completed")
