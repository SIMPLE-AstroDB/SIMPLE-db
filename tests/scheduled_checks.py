import os
import sys

import pytest
import requests
from astrodb_utils.utils import internet_connection
from astrodb_utils import load_astrodb
from tqdm import tqdm

sys.path.append(".")
from simple.schema import REFERENCE_TABLES

DB_NAME = "temp_SIMPLE.sqlite"
DB_PATH = "data"


# Load the SIMPLE database
@pytest.fixture(scope="module")
def db():

    # Connect to the new database and confirm it has the Sources table
    db = load_astrodb(DB_NAME, data_path=DB_PATH, reference_tables=REFERENCE_TABLES)

    return db


def test_db(db):
    assert os.path.exists(DB_NAME)
    assert db
    assert "source" in [c.name for c in db.Sources.columns]


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
    else:
        print("No internet connection to check URLs")

    # Display broken spectra regardless if it's the number we expect or not
    print(f"found {len(broken_urls)} broken spectra urls: {broken_urls}, {codes}")

    assert 5 <= len(broken_urls) <= 5

# Expected fails:
# 11123099-7653342.txt',
# L1_OPT_2MASS_J10595138-2113082_Cruz2003.txt
# 0000%252B2554_IRS_spectrum.fits'
# 0415-0935.fits',
# 2MASS+J22541892%2B3123498.fits'])
