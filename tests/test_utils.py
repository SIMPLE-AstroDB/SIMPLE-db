# Test to verify functions in utils
import pytest
import sys

sys.path.append('.')
from scripts.ingests.utils import *
from simple.schema import *
from astrodbkit2.astrodb import create_database, Database
from astropy.table import Table

DB_NAME = 'temp.db'
DB_PATH = 'data'
logger.setLevel(logging.DEBUG)


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
def t_plx():
    t_plx = Table([{'source': 'Fake 1', 'plx': 113, 'plx_err': 0.3, 'plx_ref': 'Ref 1'},
                   {'source': 'Fake 2', 'plx': 145, 'plx_err': 0.5, 'plx_ref': 'Ref 1'},
                   {'source': 'Fake 3', 'plx': 155, 'plx_err': 0.6, 'plx_ref': 'Ref 2'},
                   ])
    return t_plx


# Create fake astropy Table of data to load
@pytest.fixture(scope="module")
def t_pm():
    t_pm = Table(
        [{'source': 'Fake 1', 'mu_ra': 113, 'mu_ra_err': 0.3, 'mu_dec': 113, 'mu_dec_err': 0.3, 'reference': 'Ref 1'},
         {'source': 'Fake 2', 'mu_ra': 145, 'mu_ra_err': 0.5, 'mu_dec': 113, 'mu_dec_err': 0.3, 'reference': 'Ref 1'},
         {'source': 'Fake 3', 'mu_ra': 55, 'mu_ra_err': 0.23, 'mu_dec': 113, 'mu_dec_err': 0.3, 'reference': 'Ref 2'},
         ])
    return t_pm


def test_setup_db(db):
    # Some setup tasks to ensure some data exists in the database first
    ref_data = [{'name': 'Ref 1', 'doi': '10.1093/mnras/staa1522', 'bibcode': '2020MNRAS.496.1922B'},
                {'name': 'Ref 2', 'doi': 'Doi2', 'bibcode': '2012yCat.2311....0C'}]
    db.Publications.insert().execute(ref_data)

    source_data = [{'source': 'Fake 1', 'reference': 'Ref 1'},
                   {'source': 'Fake 2', 'reference': 'Ref 1'},
                   {'source': 'Fake 3', 'reference': 'Ref 2'},
                   ]
    db.Sources.insert().execute(source_data)

    return db


def test_convert_spt_string_to_code():
    # Test conversion of spectral types into numeric values
    assert convert_spt_string_to_code(['M5.6']) == [65.6]
    assert convert_spt_string_to_code(['T0.1']) == [80.1]
    assert convert_spt_string_to_code(['Y2pec']) == [92]


def test_ingest_parallaxes(db, t_plx):
    # Test ingest of parallax data
    ingest_parallaxes(db, t_plx['source'], t_plx['plx'], t_plx['plx_err'], t_plx['plx_ref'])

    results = db.query(db.Parallaxes).filter(db.Parallaxes.c.reference == 'Ref 1').table()
    assert len(results) == 2
    results = db.query(db.Parallaxes).filter(db.Parallaxes.c.reference == 'Ref 2').table()
    assert len(results) == 1
    assert results['source'][0] == 'Fake 3'
    assert results['parallax'][0] == 155
    assert results['parallax_error'][0] == 0.6


def test_ingest_proper_motions(db, t_pm):
    ingest_proper_motions(db, t_pm['source'], t_pm['mu_ra'], t_pm['mu_ra_err'],
                          t_pm['mu_dec'], t_pm['mu_dec_err'], t_pm['reference'])

    results = db.query(db.ProperMotions).filter(db.ProperMotions.c.reference == 'Ref 1').table()
    assert len(results) == 2
    results = db.query(db.ProperMotions).filter(db.ProperMotions.c.reference == 'Ref 2').table()
    assert len(results) == 1
    assert results['source'][0] == 'Fake 3'
    assert results['mu_ra'][0] == 55
    assert results['mu_ra_error'][0] == 0.23


def test_search_publication(db):
    assert not search_publication(db)[0]  # False
    assert search_publication(db, name='Ref 1')[0]  # True
    assert search_publication(db, name='Ref 1', doi='10.1093/mnras/staa1522')[0]  # True
    doi_search = search_publication(db, doi='10.1093/mnras/staa1522')
    assert doi_search[0]  # True
    assert doi_search[1] == 1
    bibcode_search = search_publication(db, bibcode='2020MNRAS.496.1922B')
    assert bibcode_search[0]  # True
    assert bibcode_search[1] == 1
    multiple_matches = search_publication(db, name='Ref')
    assert not multiple_matches[0]  # False, multiple matches
    assert multiple_matches[1] == 2  # multiple matches
    assert not search_publication(db, name='Ref 2', doi='10.1093/mnras/staa1522')[0]  # False
    assert not search_publication(db, name='Ref 2', bibcode='2020MNRAS.496.1922B')[0]  # False


def test_add_publication(db):
    # should fail if trying to add a duplicate record
    with pytest.raises(SimpleError):
        add_publication(db, name='Ref 1', bibcode='2020MNRAS.496.1922B')
    # TODO - Mock environment  where ADS_TOKEN is not set. #117


def test_add_names(db):
    sources_2 = ['Fake 1', 'Fake 2']
    other_names_2 = ['Fake 1 alt', 'Fake 2 alt']
    other_names_3 = ['Fake 1 alt', 'Fake 2 alt', 'Fake 3 alt']
    alt_names_table = Table(names=('db_name', 'ingest_name'), dtype=('str', 'str'))
    alt_names_table.add_row(('Fake 1', 'Fake 1 alternate'))
    alt_names_table.add_row(('Fake 2', 'Fake 2 alternate'))

    alt_names_table3 = [('Fake 1', 'Fake 1 alternate', '3rd column')]

    # Add names using two lists
    add_names(db, sources=sources_2, other_names=other_names_2)
    results = db.query(db.Names).filter(db.Names.c.source == 'Fake 1').table()
    assert results['other_name'][0] == 'Fake 1 alt'
    results = db.query(db.Names).filter(db.Names.c.source == 'Fake 2').table()
    assert results['other_name'][0] == 'Fake 2 alt'

    # Add names using a table
    add_names(db, names_table=alt_names_table)
    results = db.query(db.Names).filter(db.Names.c.source == 'Fake 1').table()
    assert results['other_name'][1] == 'Fake 1 alternate'
    results = db.query(db.Names).filter(db.Names.c.source == 'Fake 2').table()
    assert results['other_name'][1] == 'Fake 2 alternate'

    # should fail if lists are different length
    with pytest.raises(RuntimeError):
        add_names(db, sources=sources_2, other_names=other_names_3)

    # should fail if both table and sources list are given
    with pytest.raises(RuntimeError):
        add_names(db, sources=sources_2, other_names=other_names_2, names_table=alt_names_table)

        # should fail if table has three columns
        with pytest.raises(RuntimeError):
            add_names(db, names_table=alt_names_table3)
