# Test using the Felis validation tools

import yaml
from pydantic import ValidationError
from astrodb_utils.loaders import DatabaseSettings
from felis.datamodel import Schema

db_settings=DatabaseSettings(settings_file="database.toml")
SCHEMA_PATH = db_settings.felis_path

def test_schema():
    data = yaml.safe_load(open(SCHEMA_PATH, "r"))

    try:
        schema = Schema.model_validate(data)  # noqa: F841
    except ValidationError as e:
        print(e)
