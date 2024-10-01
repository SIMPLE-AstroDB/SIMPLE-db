import os
import sys

import pytest
import requests
from astrodb_utils.utils import internet_connection
from astrodbkit.astrodb import Database, create_database
from tqdm import tqdm

sys.path.append(".")
from simple.schema import *
from simple.schema import REFERENCE_TABLES

DB_NAME = "temp.sqlite"
DB_PATH = "data"


# Load the database for use in individual tests
@pytest.fixture(scope="module")
def db():
    # Create a fresh temporary database and assert it exists
    # Because we've imported simple.schema, we will be using that schema for the database

    if os.path.exists(DB_NAME):
        os.remove(DB_NAME)
    connection_string = "sqlite:///" + DB_NAME
    create_database(connection_string)
    assert os.path.exists(DB_NAME)

    # Connect to the new database and confirm it has the Sources table
    db = Database(connection_string, reference_tables=REFERENCE_TABLES)
    assert db
    assert "source" in [c.name for c in db.Sources.columns]

    # Load data into an in-memory sqlite database first, for performance

    # create and connects to a temporary in-memory database
    temp_db = Database("sqlite://", reference_tables=REFERENCE_TABLES)

    # load the data from the data files into the database
    temp_db.load_database(DB_PATH, verbose=False)

    # dump in-memory database to file
    temp_db.dump_sqlite(DB_NAME)
    # replace database object with new file version
    db = Database("sqlite:///" + DB_NAME, reference_tables=REFERENCE_TABLES)

    return db


def test_spectra_urls(db):
    spectra_urls = db.query(db.Spectra.c.access_url).astropy()
    broken_urls = []
    codes = []
    internet, _ = internet_connection()
    if internet:
        for spectrum_url in tqdm(spectra_urls["access_url"]):
            request_response = requests.head(spectrum_url)
            status_code = request_response.status_code
            # The website is up if the status code is 200
            # cuny academic commons links give 301 status code
            if status_code != 200 and status_code != 301:
                broken_urls.append(spectrum_url)
                codes.append(status_code)

    # Display broken spectra regardless if it's the number we expect or not
    print(f"found {len(broken_urls)} broken spectra urls: {broken_urls}, {codes}")

    assert 4 <= len(broken_urls) <= 4
