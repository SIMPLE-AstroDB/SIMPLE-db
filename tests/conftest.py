import logging
import os
import sys

import pytest
from astrodb_utils.loaders import build_db_from_json, DatabaseSettings
from astrodbkit.astrodb import Database, create_database

sys.path.append("./")  # needed for github actions to find the simple module

logger = logging.getLogger("AstroDB")

db_settings=DatabaseSettings(settings_file="database.toml")
SCHEMA_PATH = db_settings.felis_path

# Create a fresh SIMPLE database for the data and integrity tests
@pytest.fixture(scope="session", autouse=True)
def db():
    db = build_db_from_json(
        settings_file="database.toml"
    )

    return db


# Create a temp database with dummy data to test utility functions
@pytest.fixture(scope="session", autouse=True)
def temp_db():
    TEMP_DB_NAME = "tests/temp_utils.sqlite"

    if os.path.exists(TEMP_DB_NAME):
        os.remove(TEMP_DB_NAME)
    connection_string = "sqlite:///" + TEMP_DB_NAME
    create_database(connection_string, felis_schema=SCHEMA_PATH)
    temp_db = Database(connection_string)

    # Add some test data to the temp database
    ref_data = [
        {
            "reference": "Ref 1",
            "doi": "10.1093/mnras/staa1522",
            "bibcode": "2020MNRAS.496.1922B",
        },
        {"reference": "Ref 2", "doi": "Doi2", "bibcode": "2012yCat.2311....0C"},
        {"reference": "Burn08", "doi": "Doi3", "bibcode": "2008MNRAS.391..320B"},
    ]

    regime_data = [
        {"regime": "optical"},
        {"regime": "nir"},
    ]

    telescope_data = [{"telescope": "Keck I"}, {"telescope": "IRTF"}]

    instrument_data = [
        {"instrument": "LRIS", "mode": "OG570", "telescope": "Keck I"},
        {"instrument": "SpeX", "mode": "Prism", "telescope": "IRTF"},
    ]

    source_data = [
        {"source": "Fake 1", "ra": 9.0673755, "dec": 18.352889, "reference": "Ref 1"},
        {"source": "Fake 2", "ra": 9.0673755, "dec": 18.352889, "reference": "Ref 1"},
        {"source": "Fake 3", "ra": 9.0673755, "dec": 18.352889, "reference": "Ref 2"},
        {"source": "apple", "ra": 9.0673755, "dec": 18.352889, "reference": "Ref 1"},
        {"source": "orange", "ra": 90.0673755, "dec": 19.352889, "reference": "Ref 2"},
        {
            "source": "banana",
            "ra": 60.0673755,
            "dec": -18.352889,
            "reference": "Burn08",
        },
    ]

    with temp_db.engine.connect() as conn:
        conn.execute(temp_db.Publications.insert().values(ref_data))
        conn.execute(temp_db.Sources.insert().values(source_data))
        conn.execute(temp_db.RegimeList.insert().values(regime_data))
        conn.execute(temp_db.Telescopes.insert().values(telescope_data))
        conn.execute(temp_db.Instruments.insert().values(instrument_data))
        conn.commit()

    logger.info("Loaded temp database using temp_db function in conftest")

    return temp_db
