# Test to verify database integrity

import os
import pytest
from simple.schema import *
from astrodbkit2.astrodb import create_database, Database
from astropy.table import unique

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


def test_data_load(db):
    # Test that all data can be loaded into the database
    # This should take care of finding serious issues (key/column violations)
    db.load_database(DB_PATH, verbose=True)


@pytest.mark.xfail
def test_reference_uniqueness(db):
    # Verify that all Publications.name values are unique
    assert False


@pytest.mark.xfail
def test_references(db):
    # Verify that all sources point to an existing Publication
    assert False


@pytest.mark.xfail
def test_coordinates(db):
    # Verify that all sources have valid coordinates
    assert False


def test_source_names(db):
    # Verify that all sources have at least one entry in Names table
    sql_text = "SELECT Sources.source	FROM Sources LEFT JOIN Names " \
             "ON Names.source=Sources.source WHERE Names.source IS NULL"
    missing_names = db.sql_query(sql_text, format='astropy')
    assert len(missing_names) == 0


def test_source_uniqueness1(db):
    # Verify that all Sources.source values are unique
    source_names = db.query(db.Sources.c.source).astropy()
    unique_source_names = unique(source_names)
    assert len(source_names) == len(unique_source_names)


def test_source_uniqueness2(db):
    # Verify that all Sources.source values are unique and find the duplicates
    sql_text = "SELECT Sources.source FROM Sources GROUP BY source " \
               "HAVING (Count(*) > 1)"
    duplicate_names = db.sql_query(sql_text, format='astropy')
    # if duplicate_names is non_zero, print out duplicate names
    assert len(duplicate_names) == 0

# Clean up temporary database
#def test_remove_database(db):
#    db.session.close()
#    db.engine.dispose()
#    if os.path.exists(DB_NAME):
#        os.remove(DB_NAME)
