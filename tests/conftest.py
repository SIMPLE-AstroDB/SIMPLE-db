import logging
import os
import sys

import pytest
from astrodb_utils import load_astrodb
from astrodbkit.astrodb import Database, create_database

sys.path.append("./")  # needed for github actions to find the simple module
from simple import REFERENCE_TABLES

logger = logging.getLogger("AstroDB")

SCHEMA_PATH = "simple/schema.yaml"


# Create a fresh SIMPLE database for the data and integrity tests
@pytest.fixture(scope="session", autouse=True)
def db():
    DB_NAME = "tests/simple_tests.sqlite"
    DB_PATH = "data"
    
    if os.path.exists(DB_NAME):
        os.remove(DB_NAME)
    connection_string = "sqlite:///" + DB_NAME
    create_database(connection_string, felis_schema=SCHEMA_PATH)
    assert os.path.exists(DB_NAME)

    # Connect to the new database
    db = load_astrodb(DB_NAME,
                      data_path=DB_PATH,
                      reference_tables=REFERENCE_TABLES,
                      felis_schema=SCHEMA_PATH)

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
        conn.execute(temp_db.Regimes.insert().values(regime_data))
        conn.execute(temp_db.Telescopes.insert().values(telescope_data))
        conn.execute(temp_db.Instruments.insert().values(instrument_data))
        conn.commit()

    logger.info("Loaded temp database using temp_db function in conftest")

    return temp_db
