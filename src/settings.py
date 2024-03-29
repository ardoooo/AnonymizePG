import json

SETTINGS = dict()


def load_settings(path="example/shuffle_settings.json"):
    with open(path, "r") as f:
        global SETTINGS
        SETTINGS = json.load(f)


def get_settings():
    global SETTINGS
    return SETTINGS
