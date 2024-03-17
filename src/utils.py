def get_columns(cur, table_name: str):
    cur.execute(
        f"SELECT column_name, data_type FROM information_schema.columns "
        f"WHERE table_name = '{table_name}' ORDER BY ordinal_position"
    )
    columns = cur.fetchall()
    return columns
