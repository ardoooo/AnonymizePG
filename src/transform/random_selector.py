import logging
import psycopg2
import typing

from src.utils import utils
from src.transform.transformer import Transformer


logger = logging.getLogger(__name__)


class RandomSelector(Transformer):
    def __init__(
        self,
        conn: psycopg2.extensions.connection,
        src_table: str,
        transfer_table: str,
        processed_column: str,
        continuous_mode: bool,
        batch_size: int,
        sleep_ms: int,
        groups: typing.List[typing.List[str]],
    ):
        super().__init__(
            conn, src_table, transfer_table, processed_column, continuous_mode, batch_size, sleep_ms
        )
        self.groups = groups

        self.new_types = []
        self.new_funcs = []

    def get_transfer_table_schema(self):
        column_names = []
        for group in self.groups:
            column_names.extend(group)
        return [(column, self.column_types[column]) for column in column_names]

    def skip_process_last_batch(self):
        return True

    def get_funcs(self):
        return self.new_funcs

    def prepare(self):
        for group in self.groups:
            type_name = "_type_" + utils.join_names(group, "_")
            self.new_types.append(type_name)

            fields = [f"{column} {self.column_types[column]}" for column in group]
            fields_str = ",\n".join(fields)

            create_type_query = f"""
                CREATE TYPE {type_name} AS (
                {fields_str}
            );
            """
            with self.conn.cursor() as cur:
                cur.execute(create_type_query)

            select_random_func_name = "_select_random_" + utils.join_names(group, "_")
            self.new_funcs.append(select_random_func_name)

            select_random_func_query = f"""
                CREATE OR REPLACE FUNCTION {select_random_func_name}()
                RETURNS SETOF {type_name} AS $$
                DECLARE
                    random_index INT;
                    counter INT := 0;
                    rec RECORD;
                BEGIN
                    random_index := (random() * ({self.batch_size} - 1))::INT;
                    FOR rec IN
                        SELECT {utils.join_names(group)} FROM {self.src_table} s
                        JOIN {self.temp_table_name} t ON s.ctid = t._ctid_
                    LOOP
                        IF counter = random_index THEN
                            RETURN NEXT rec;
                            EXIT;
                        END IF;
                        counter := counter + 1;
                    END LOOP;
                END;
                $$ LANGUAGE plpgsql;"""

            with self.conn.cursor() as cur:
                cur.execute(select_random_func_query)

        logger.debug("RandomSelector preparation successfully completed")

    def cleanup(self):
        type_str = ", ".join(self.new_types)
        drop_types_query = f"DROP TYPE IF EXISTS {type_str} CASCADE;"

        funcs_str = ", ".join(self.new_funcs)
        drop_funcs_query = f"DROP FUNCTION IF EXISTS {funcs_str} CASCADE;"

        with self.conn.cursor() as cur:
            cur.execute(drop_types_query)
            cur.execute(drop_funcs_query)

    logger.debug("RandomSelector cleanup successfully completed")
