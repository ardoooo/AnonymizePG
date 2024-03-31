import json

SETTINGS = dict()


def load_settings(path: str):
    with open(path, "r") as f:
        global SETTINGS
        SETTINGS = json.load(f)


def get_settings():
    return SETTINGS

def get_processing_settings():
    return SETTINGS.get('processing_settings')
