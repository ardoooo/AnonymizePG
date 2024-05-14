import pytest


@pytest.mark.parametrize(
    "cnt_dests,env_file",
    [
        (0, "0.env"),
        (0, "1.env"),
        (1, None),
        (1, "1.env"),
        (2, "2.env"),
    ],
)
@pytest.mark.parametrize(
    "settings_file,interrupt_time_ms",
    [
        ("shuffle_settings.json", None),
        ("shuffle_settings.json", 2000),
        ("shuffle_settings.json", 5000),
        ("shuffle_cont_settings.json", 2000),
        ("shuffle_cont_settings.json", 5000),
    ],
)
def test_cleanup_helpers_src(
    postgres_prod,
    run_script,
    run_script_with_interrupt,
    cnt_dests,
    env_file,
    settings_file,
    interrupt_time_ms,
):
    postgres_prod.execute("SELECT typname FROM pg_type;")
    types_before = list(postgres_prod.fetchall())
    postgres_prod.execute("SELECT proname FROM pg_proc;")
    funcs_before = list(postgres_prod.fetchall())
    postgres_prod.execute("SELECT indexname FROM pg_indexes;")
    indexes_before = list(postgres_prod.fetchall())
    postgres_prod.execute("SELECT tablename FROM pg_tables;")
    tables_before = list(postgres_prod.fetchall())

    postgres_prod.execute("SELECT * FROM workers;")
    columns_before = [desc[0] for desc in postgres_prod.description]
    prod_data_before = list(postgres_prod.fetchall())
    prod_data_before
    assert prod_data_before is not None
    assert len(prod_data_before) == 18

    if cnt_dests == 0:
        script_name = "depers_only.py"
    else:
        script_name = "depers_and_replicate.py"

    if interrupt_time_ms is not None:
        run_script_with_interrupt(
            script_name, settings_file, interrupt_time_ms, env_file
        )
    else:
        run_script(script_name, settings_file, env_file)

    postgres_prod.execute("SELECT typname FROM pg_type;")
    types_after = list(postgres_prod.fetchall())
    if cnt_dests == 0:
        types_before.append(
            ("_transfer_workers",),
        )
    assert types_before.sort() == types_after.sort()

    postgres_prod.execute("SELECT proname FROM pg_proc;")
    funcs_after = list(postgres_prod.fetchall())
    assert funcs_before.sort() == funcs_after.sort()

    postgres_prod.execute("SELECT indexname FROM pg_indexes;")
    indexes_after = list(postgres_prod.fetchall())
    assert indexes_before.sort() == indexes_after.sort()

    postgres_prod.execute("SELECT tablename FROM pg_tables;")
    tables_after = list(postgres_prod.fetchall())
    if cnt_dests == 0:
        tables_before.append(
            ("_transfer_workers",),
        )

    assert tables_before.sort() == tables_after.sort()

    postgres_prod.execute("SELECT pubname FROM pg_publication;")
    pubs_after = list(postgres_prod.fetchall())
    assert len(pubs_after) == 0

    postgres_prod.execute("SELECT * FROM workers;")
    columns_after = [desc[0] for desc in postgres_prod.description]
    prod_data_after = list(postgres_prod.fetchall())
    assert columns_before.sort() == columns_after.sort()
    assert prod_data_before.sort() == prod_data_after.sort()


@pytest.mark.parametrize(
    "cnt_dests,env_file,",
    [
        (1, None),
        (1, "1.env"),
        (2, "2.env"),
    ],
)
@pytest.mark.parametrize(
    "settings_file,interrupt_time_ms",
    [
        ("shuffle_settings.json", None),
        ("shuffle_settings.json", 2000),
        ("shuffle_settings.json", 5000),
        ("shuffle_cont_settings.json", 2000),
        ("shuffle_cont_settings.json", 5000),
    ],
)
def test_cleanup_helpers_dests(
    postgres_prod,
    postgres_test1,
    postgres_test2,
    run_script,
    run_script_with_interrupt,
    cnt_dests,
    env_file,
    settings_file,
    interrupt_time_ms,
):
    postgres_test1.execute("SELECT indexname FROM pg_indexes;")
    indexes_test1_before = list(postgres_test1.fetchall())
    if cnt_dests == 2:
        postgres_test2.execute("SELECT indexname FROM pg_indexes;")
        indexes_test2_before = list(postgres_test2.fetchall())

    if interrupt_time_ms is not None:
        run_script_with_interrupt(
            "depers_and_replicate.py", settings_file, interrupt_time_ms, env_file
        )
    else:
        run_script("depers_and_replicate.py", settings_file, env_file)

    postgres_test1.execute("SELECT indexname FROM pg_indexes;")
    indexes_test1_after = list(postgres_test1.fetchall())
    assert indexes_test1_before.sort() == indexes_test1_after.sort()

    if cnt_dests == 2:
        postgres_test2.execute("SELECT indexname FROM pg_indexes;")
        indexes_test2_after = list(postgres_test2.fetchall())
        assert indexes_test2_before.sort() == indexes_test2_after.sort()

    postgres_test1.execute("SELECT subname FROM pg_subscription;")
    subs_test1_after = list(postgres_test1.fetchall())
    assert len(subs_test1_after) == 0

    if cnt_dests == 2:
        postgres_test2.execute("SELECT subname FROM pg_subscription;")
        subs_test2_after = list(postgres_test2.fetchall())
        assert len(subs_test2_after) == 0
