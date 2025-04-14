import os
import sys

import pytest
import requests
from astrodb_utils import load_astrodb
from tqdm import tqdm
from astroquery.simbad import Simbad
from astrodbkit.utils import _name_formatter

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
    # internet, _ = internet_connection()
    # if not internet:
    #    assert False, "No internet connection to check spectra urls"

    for spectrum_url in tqdm(spectra_urls["access_url"]):
        request_response = requests.head(spectrum_url)
        status_code = request_response.status_code
        # The website is up if the status code is 200
        # cuny academic commons links give 301 status code
        if status_code not in (200, 301):
            broken_urls.append(spectrum_url)
            codes.append(status_code)

    # Display broken spectra regardless if it's the number we expect or not
    print(f"found {len(broken_urls)} broken spectra urls: {broken_urls}, {codes}")

    assert 5 == len(broken_urls)

# Expected fails:
# 11123099-7653342.txt',
# L1_OPT_2MASS_J10595138-2113082_Cruz2003.txt
# 0000%252B2554_IRS_spectrum.fits'
# 0415-0935.fits',
# 2MASS+J22541892%2B3123498.fits'])


def test_source_simbad(db):
    # Query Simbad and confirm that there are no duplicates with different names

    # Get list of all source names
    results = db.query(db.Sources.c.source).all()
    name_list = [s[0] for s in results]

    # Add all IDS to the Simbad output as well as the user-provided id
    Simbad.add_votable_fields("ids")

    print("Querying SIMBAD for all SIMPLE sources")
    simbad_results = Simbad.query_objects(name_list)

    duplicate_count = 0
    not_in_simbad = []
    in_simbad = []

    print("Checking all SIMPLE sources for Simbad names")
    for row in tqdm(simbad_results[["main_id", "ids", "user_specified_id"]].iterrows()):
        simple_name = row[2]
        try:
            simbad_ids = row[1].decode("utf-8")
        except AttributeError:
            # Catch decoding error
            simbad_ids = row[1]

        # Get a nicely formatted list of Simbad names for each input row
        simbad_names = [
            _name_formatter(s)
            for s in simbad_ids.split("|")
            if _name_formatter(s) != "" and _name_formatter(s) is not None
        ]

        if len(simbad_names) == 0:
            not_in_simbad.append(simple_name)
            continue
        else:
            in_simbad.append(simple_name)

        # Examine DB for each input, displaying results when more than one source matches
        t = db.search_object(
            simbad_names,
            output_table="Sources",
            fmt="astropy",
            fuzzy_search=False,
            verbose=False,
        )
        if len(t) > 1:
            print(f"Multiple matches for {simple_name}: {simbad_names}")
            print(
                db.query(db.Names).filter(db.Names.c.source.in_(t["source"])).astropy()
            )
            duplicate_count += 1

    assert duplicate_count == 0, "Duplicate sources identified via Simbad queries"
    assert (
        len(not_in_simbad) == 425
    ), f"Expecting {len(not_in_simbad)} sources not found in Simbad"

    assert len(in_simbad) == 3012, "Sources found in Simbad"
    print(f"Found {len(in_simbad)} SIMPLE sources in Simbad")

    assert len(not_in_simbad) + len(in_simbad) == len(
        name_list
    ), "Not all sources checked"

    print(f"Found {len(not_in_simbad)} SIMPLE sources not in Simbad")
    print("\n".join(not_in_simbad))
