import pytest
import os
from astrodbkit2.astrodb import create_database, Database
from astrodb_scripts import load_astrodb
from simple import REFERENCE_TABLES
from simple.schema import *

# Create a fresh temporary SIMPLE database and assert it exists
# Because we've imported simple.schema, we will be using that schema for the database
# This is in the conftest.py file so temp database is only created once per test session

DB_NAME = "tests/simple_tests.sqlite"
DB_PATH = "data"


if os.path.exists(DB_NAME):
    os.remove(DB_NAME)
connection_string = "sqlite:///" + DB_NAME
create_database(connection_string)
assert os.path.exists(DB_NAME)

# Connect to the new database and confirm it has the Sources table
db = Database(connection_string, reference_tables=REFERENCE_TABLES)

# Load data into an in-memory sqlite database first, for performance
temp_db = Database(
    "sqlite://", reference_tables=REFERENCE_TABLES
)  # creates and connects to a temporary in-memory database
temp_db.load_database(
    DB_PATH, verbose=False
)  # loads the data from the data files into the database
temp_db.dump_sqlite(DB_NAME)  # dump in-memory database to file
db = Database(
    "sqlite:///" + DB_NAME, reference_tables=REFERENCE_TABLES
)  # replace database object with new file version


@pytest.fixture(scope="session", autouse=True)
def db():
    db = load_astrodb(DB_NAME, recreatedb=False)
    return db


# Create a temp database with dummy data to test utility functions
TEMP_DB_NAME = "tests/temp_utils.sqlite"

if os.path.exists(TEMP_DB_NAME):
    os.remove(TEMP_DB_NAME)
connection_string = "sqlite:///" + TEMP_DB_NAME
create_database(connection_string)

# Connect to the new database and confirm it has the Sources table
temp_db = Database(connection_string)

# Some setup tasks to ensure some data exists in the database first
ref_data = [
    {
        "reference": "Ref 1",
        "doi": "10.1093/mnras/staa1522",
        "bibcode": "2020MNRAS.496.1922B",
    },
    {"reference": "Ref 2", "doi": "Doi2", "bibcode": "2012yCat.2311....0C"},
    {"reference": "Burn08", "doi": "Doi3", "bibcode": "2008MNRAS.391..320B"},
]

source_data = [
    {"source": "Fake 1", "ra": 9.0673755, "dec": 18.352889, "reference": "Ref 1"},
    {"source": "Fake 2", "ra": 9.0673755, "dec": 18.352889, "reference": "Ref 1"},
    {"source": "Fake 3", "ra": 9.0673755, "dec": 18.352889, "reference": "Ref 2"},
    {"source": "apple", "ra": 9.0673755, "dec": 18.352889, "reference": "Ref 1"},
    {"source": "orange", "ra": 90.0673755, "dec": 19.352889, "reference": "Ref 2"},
    {
        "source": "banana",
        "ra": 360.0673755,
        "dec": -18.352889,
        "reference": "Burn08",
    },
]

with temp_db.engine.connect() as conn:
    conn.execute(temp_db.Publications.insert().values(ref_data))
    conn.execute(temp_db.Sources.insert().values(source_data))
    conn.commit()


@pytest.fixture(scope="session", autouse=True)
def temp_db():
    temp_db = load_astrodb(TEMP_DB_NAME, recreatedb=False)
    return temp_db
