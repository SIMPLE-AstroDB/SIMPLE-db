# Test to verify database integrity

import os
import pytest
from sqlalchemy import func
from simple.schema import *
from astrodbkit2.astrodb import create_database, Database, or_
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
    db.load_database(DB_PATH, verbose=False)


def test_reference_uniqueness(db):
    # Verify that all Publications.name values are unique
    t = db.query(db.Publications.c.name).astropy()
    assert len(t) == len(unique(t, keys='name')), 'Duplicated Publications found'

    # Verify that DOI are supplied
    t = db.query(db.Publications.c.name).filter(db.Publications.c.doi.is_(None)).astropy()
    if len(t) > 0:
        print(f'\n{len(t)} publications lacking DOI:')
        print(t)

    # Verify that Bibcodes are supplied
    t = db.query(db.Publications.c.name).filter(db.Publications.c.bibcode.is_(None)).astropy()
    if len(t) > 0:
        print(f'\n{len(t)} publications lacking Bibcodes:')
        print(t)


def test_references(db):
    # Verify that all data point to an existing Publication

    ref_list = []
    table_list = ['Sources', 'Photometry']
    for table in table_list:
        # Get list of unique references
        t = db.query(db.metadata.tables[table].c.reference).distinct().astropy()
        ref_list = ref_list + t['reference'].tolist()

    # Getting unique set
    ref_list = list(set(ref_list))

    # Confirm that all are in Publications
    t = db.query(db.Publications.c.name).filter(db.Publications.c.name.in_(ref_list)).astropy()
    assert len(t) == len(ref_list), 'Some references were not matched'

    # List out publications that have not been used
    t = db.query(db.Publications.c.name).filter(db.Publications.c.name.notin_(ref_list)).astropy()
    if len(t) > 0:
        print(f'\n{len(t)} publications not referenced by {table_list}')
        print(t)


def test_coordinates(db):
    # Verify that all sources have valid coordinates
    t = db.query(db.Sources.c.source, db.Sources.c.ra, db.Sources.c.dec).filter(
        or_(db.Sources.c.ra.is_(None), db.Sources.c.ra < 0, db.Sources.c.ra > 360,
            db.Sources.c.dec.is_(None), db.Sources.c.dec < -90, db.Sources.c.dec > 90)).astropy()

    if len(t) > 0:
        print(f'\n{len(t)} Sources failed coordinate checks')
        print(t)

    assert len(t) == 0, f'{len(t)} Sources failed coordinate checks'


def test_source_names(db):
    # Verify that all sources have at least one entry in Names table
    sql_text = "SELECT Sources.source	FROM Sources LEFT JOIN Names " \
             "ON Names.source=Sources.source WHERE Names.source IS NULL"
    missing_names = db.sql_query(sql_text, format='astropy')
    assert len(missing_names) == 0


def test_source_uniqueness(db):
    # Verify that all Sources.source values are unique
    source_names = db.query(db.Sources.c.source).astropy()
    unique_source_names = unique(source_names)
    assert len(source_names) == len(unique_source_names)

    # Another method to find the duplicates
    sql_text = "SELECT Sources.source FROM Sources GROUP BY source " \
               "HAVING (Count(*) > 1)"
    duplicate_names = db.sql_query(sql_text, format='astropy')

    # if duplicate_names is non_zero, print out duplicate names
    if len(duplicate_names) > 0:
        print(f'\n{len(duplicate_names)} duplicated names')
        print(duplicate_names)

    assert len(duplicate_names) == 0


def test_names_table(db):
    # Verify that all Sources contain at least one entry in the Names table
    name_list = db.query(db.Sources.c.source).astropy()
    name_list = name_list['source'].tolist()
    source_name_counts = db.query(db.Names.c.source).\
        filter(db.Names.c.source.in_(name_list)).\
        distinct().\
        count()
    assert len(name_list) == source_name_counts, 'ERROR: There are Sources without entries in the Names table'

    # Verify that each Source contains an entry in Names with Names.source = Names.other_source
    valid_name_counts = db.query(db.Names.c.source).\
        filter(db.Names.c.source == db.Names.c.other_name).\
        distinct().\
        count()

    # If the number of valid names don't match the number of sources, then there are cases that are missing
    # The script below will gather them and print them out
    if len(name_list) != valid_name_counts:
        # Create a temporary table that groups entries in the Names table by their source name
        # with a column containing a concatenation of all known names
        t = db.query(db.Names.c.source,
                     func.group_concat(db.Names.c.other_name).label('names')).\
            group_by(db.Names.c.source).\
            astropy()

        # Get the list of entries whose source name are not present in the 'other_names' column
        # Then return the Names table results so we can see what the DB has for these entries
        results = [row['source'] for row in t if row['source'] not in row['names'].split(',')]
        print('\nEntries in Names without Names.source == Names.other_name:')
        print(db.query(db.Names).
              filter(db.Names.c.source.in_(results)).
              astropy())

    assert len(name_list) == valid_name_counts, \
        'ERROR: There are entries in Names without Names.source == Names.other_name'


# Clean up temporary database
def test_remove_database(db):
    db.session.close()
    db.engine.dispose()
    if os.path.exists(DB_NAME):
        os.remove(DB_NAME)
