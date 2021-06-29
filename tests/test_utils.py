# Test to verify funtions in utils

import os
import pytest
import sys
sys.path.append('.')
from scripts.ingests.utils import *
from simple.schema import *
from astrodbkit2.astrodb import create_database, Database
from astropy.table import Table

DB_NAME = 'temp.db'
DB_PATH = 'data'


# Load the database for use in individual tests
@pytest.fixture(scope="module")
def db():
    # Create a fresh temporary database and assert it exists
    # Because we've imported simple.schema, we will be using that schema for the database

    if os.path.exists(DB_NAME):
        os.remove(DB_NAME)
    connection_string = 'sqlite:///' + DB_NAME
    create_database(connection_string)
    assert os.path.exists(DB_NAME)

    # Connect to the new database and confirm it has the Sources table
    db = Database(connection_string)
    assert db
    assert 'source' in [c.name for c in db.Sources.columns]

    return db


# Create fake astropy Table of data to load
@pytest.fixture(scope="module")
def t():
    t = Table([{'source': 'Fake 1', 'plx': 113, 'plx_err': 0.3, 'plx_ref': 'Ref 1'},
               {'source': 'Fake 2', 'plx': 145, 'plx_err': 0.5, 'plx_ref': 'Ref 1'},
               {'source': 'Fake 3', 'plx': 155, 'plx_err': 0.6, 'plx_ref': 'Ref 2'},
               ])
    return t


def test_setup_db(db):
    # Some setup tasks to ensure some data exists in the database first
    ref_data = [{'name': 'Ref 1'}, {'name': 'Ref 2'}]
    db.Publications.insert().execute(ref_data)

    source_data = [{'source': 'Fake 1', 'reference': 'Ref 1'},
                   {'source': 'Fake 2', 'reference': 'Ref 1'},
                   {'source': 'Fake 3', 'reference': 'Ref 1'},
                   ]
    db.Sources.insert().execute(source_data)


@pytest.mark.xfail()
def test_check_names_simbad():
    # TODO: set up Simbad mocking to avoid real calls; ideally also avoid user input
    assert False


def test_convert_spt_string_to_code():
    # Test conversion of spectral types into numeric values
    assert convert_spt_string_to_code(['M5.6']) == [65.6]
    assert convert_spt_string_to_code(['T0.1']) == [80.1]
    assert convert_spt_string_to_code(['Y2pec']) == [92]


def test_ingest_parallaxes(db, t):
    # Test ingest of parallax data
    ingest_parallaxes(db, t['source'], t['plx'], t['plx_err'], t['plx_ref'], verbose=False, norun=False)

    results = db.query(db.Parallaxes).filter(db.Parallaxes.c.reference == 'Ref 1').table()
    assert len(results) == 2
    results = db.query(db.Parallaxes).filter(db.Parallaxes.c.reference == 'Ref 2').table()
    assert len(results) == 1
    assert results['source'][0] == 'Fake 3'
    assert results['parallax'][0] == 155
    assert results['parallax_error'][0] == 0.6


def test_add_publication(db):
    add_publication(db, name='blah', doi='blah', bibcode='blah', dryrun=False)
    results = db.query(db.Publications).filter(db.Publications.c.name == 'blah').table()
    assert len(results) == 1


def test_search_publication(db):
    # TODO: have to add records first and then test them.
    assert search_publication(db, name='blah')
