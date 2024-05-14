import pytest
import os

import utils


@pytest.mark.parametrize(
    "metrics",
    [False, True],
)
@pytest.mark.parametrize(
    "logs",
    [False, True],
)
@pytest.mark.parametrize(
    "script_name",
    ["depers_only.py", "depers_and_replicate.py"],
)
def test_logs_metrics_enabling(
    run_script,
    create_tmp,
    find_file,
    postgres_prod,
    script_name,
    logs,
    metrics,
):
    settings_file = find_file("copy_settings.json")
    settings_dict = utils.load_json(settings_file)
    settings_dict.pop("logs_dir", None)
    settings_dict.pop("metrics_dir", None)

    if logs:
        settings_dict["logs_dir"] = os.getcwd() + "/tmp"
    if metrics:
        settings_dict["metrics_dir"] = os.getcwd() + "/tmp"
    utils.dump_json(settings_file, settings_dict)

    run_script(script_name, settings_file)

    if logs:
        assert os.path.exists("tmp/logs.log")
        assert os.path.exists("tmp/error_logs.log")

    if metrics:
        assert os.path.exists("tmp/metrics.db")

    settings_dict.pop("logs_dir", None)
    settings_dict.pop("metrics_dir", None)
    utils.dump_json(settings_file, settings_dict)
