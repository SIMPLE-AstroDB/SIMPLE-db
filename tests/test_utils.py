# Test to verify functions in utils
import pytest
import sys
import math

sys.path.append('.')
from scripts.ingests.utils import *
from scripts.ingests.ingest_utils import *
from simple.schema import *
from astrodbkit2.astrodb import create_database, Database
from astropy.table import Table
from sqlalchemy import and_

DB_NAME = 'temp.db'
DB_PATH = 'data'
logger.setLevel(logging.INFO)


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
    t_plx = Table([{'source': 'Fake 1', 'plx': 113., 'plx_err': 0.3, 'plx_ref': 'Ref 1'},
                   {'source': 'Fake 2', 'plx': 145., 'plx_err': 0.5, 'plx_ref': 'Ref 1'},
                   {'source': 'Fake 3', 'plx': 155., 'plx_err': 0.6, 'plx_ref': 'Ref 2'},
                   ])
    return t_plx


# Create fake astropy Table of data to load
@pytest.fixture(scope="module")
def t_pm():
    t_pm = Table(
        [{'source': 'Fake 1', 'mu_ra': 113., 'mu_ra_err': 0.3, 'mu_dec': 113., 'mu_dec_err': 0.3, 'reference': 'Ref 1'},
         {'source': 'Fake 2', 'mu_ra': 145., 'mu_ra_err': 0.5, 'mu_dec': 113., 'mu_dec_err': 0.3, 'reference': 'Ref 1'},
         {'source': 'Fake 3', 'mu_ra': 55., 'mu_ra_err': 0.23, 'mu_dec': 113., 'mu_dec_err': 0.3, 'reference': 'Ref 2'},
         ])
    return t_pm


def test_setup_db(db):
    # Some setup tasks to ensure some data exists in the database first
    ref_data = [{'reference': 'Ref 1', 'doi': '10.1093/mnras/staa1522', 'bibcode': '2020MNRAS.496.1922B'},
                {'reference': 'Ref 2', 'doi': 'Doi2', 'bibcode': '2012yCat.2311....0C'},
                {'reference': 'Burn08', 'doi': 'Doi3', 'bibcode': '2008MNRAS.391..320B'}]

    source_data = [{'source': 'Fake 1', 'ra': 9.0673755, 'dec': 18.352889, 'reference': 'Ref 1'},
                   {'source': 'Fake 2', 'ra': 9.0673755, 'dec': 18.352889, 'reference': 'Ref 1'},
                   {'source': 'Fake 3', 'ra': 9.0673755, 'dec': 18.352889, 'reference': 'Ref 2'},
                   ]

    with db.engine.connect() as conn:
        conn.execute(db.Publications.insert().values(ref_data))
        conn.execute(db.Sources.insert().values(source_data))
        conn.commit()

    return db


def test_ingest_sources(db):
    # TODO: Test adding an alt name
    source_data1 = Table([{'source': 'Fake 1', 'ra': 9.0673755, 'dec': 18.352889, 'reference': 'Ref 1'},
                          {'source': 'Fake 6', 'ra': 10.0673755, 'dec': 18.352889, 'reference': 'Ref 2'},
                          {'source': 'Fake 7', 'ra': 11.0673755, 'dec': 18.352889, 'reference': 'Ref 1'}])
    source_data5 = Table([{'source': 'Fake 5', 'ra': 9.06799, 'dec': 18.352889, 'reference': ''}])
    source_data8 = Table([{'source': 'Fake 8', 'ra': 9.06799, 'dec': 18.352889, 'reference': 'Ref 4'}])

    ingest_sources(db, source_data1['source'], ras=source_data1['ra'], decs=source_data1['dec'],
                   references=source_data1['reference'], raise_error=True)

    ingest_sources(db, ['Barnard Star'], references='Ref 2', raise_error=True)
    Barnard_star = db.query(db.Sources).filter(db.Sources.c.source == 'Barnard Star').astropy()
    assert len(Barnard_star) == 1
    assert math.isclose(Barnard_star['ra'], 269.452, abs_tol=0.001)
    assert math.isclose(Barnard_star['dec'], 4.6933, abs_tol=0.001)

    assert db.query(db.Sources).filter(db.Sources.c.source == 'Fake 1').count() == 1
    assert db.query(db.Sources).filter(db.Sources.c.source == 'Fake 6').count() == 1
    assert db.query(db.Sources).filter(db.Sources.c.source == 'Fake 7').count() == 1

    with pytest.raises(SimpleError) as error_message:
        ingest_sources(db, source_data8['source'], ras=source_data8['ra'], decs=source_data5['dec'],
                       references=source_data8['reference'], raise_error=True)
        assert 'not in Publications table' in str(error_message.value)

    with pytest.raises(SimpleError) as error_message:
        ingest_sources(db, source_data5['source'], ras=source_data5['ra'], decs=source_data5['dec'],
                       references=source_data5['reference'], raise_error=True)
        assert 'blank' in str(error_message.value)

    with pytest.raises(SimpleError) as error_message:
        ingest_sources(db, ['NotinSimbad'], references='Ref 1', raise_error=True)
        assert 'Coordinates are needed' in str(error_message.value)


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
    assert db.query(db.ProperMotions).filter(db.ProperMotions.c.reference == 'Ref 1').count() == 2
    results = db.query(db.ProperMotions).filter(db.ProperMotions.c.reference == 'Ref 2').table()
    assert len(results) == 1
    assert results['source'][0] == 'Fake 3'
    assert results['mu_ra'][0] == 55
    assert results['mu_ra_error'][0] == 0.23


def test_ingest_spectral_types(db):
    data1 = Table([{'source': 'Fake 1', 'spectral_type': 'M5.6', 'regime': 'nir', 'reference': 'Ref 1'},
                   {'source': 'Fake 2', 'spectral_type': 'T0.1', 'regime': 'nir', 'reference': 'Ref 1'},
                   {'source': 'Fake 3', 'spectral_type': 'Y2pec', 'regime': 'nir', 'reference': 'Ref 2'},
                   ])

    data2 = Table([{'source': 'Fake 1', 'spectral_type': 'M5.6', 'reference': 'Ref 1'},
                   {'source': 'Fake 2', 'spectral_type': 'T0.1', 'reference': 'Ref 1'},
                   {'source': 'Fake 3', 'spectral_type': 'Y2pec', 'reference': 'Ref 2'},
                   ])

    data3 = Table([{'source': 'Fake 1', 'spectral_type': 'M5.6', 'regime': 'nir', 'reference': 'Ref 1'},
                   {'source': 'Fake 2', 'spectral_type': 'T0.1', 'regime': 'nir', 'reference': 'Ref 1'},
                   {'source': 'Fake 3', 'spectral_type': 'Y2pec', 'regime': 'nir', 'reference': 'Ref 4'},
                   ])
    ingest_spectral_types(db, data1['source'], data1['spectral_type'], data1['reference'], data1['regime'])
    assert db.query(db.SpectralTypes).filter(db.SpectralTypes.c.reference == 'Ref 1').count() == 2
    results = db.query(db.SpectralTypes).filter(db.SpectralTypes.c.reference == 'Ref 2').table()
    assert len(results) == 1
    assert results['source'][0] == 'Fake 3'
    assert results['spectral_type_string'][0] == 'Y2pec'
    assert results['spectral_type_code'][0] == [92]
    # testing for publication error
    with pytest.raises(SimpleError) as error_message:
        ingest_spectral_types(db, data3['source'], data3['spectral_type'], data3['regime'], data3['reference'])
        assert 'The publication does not exist in the database' in str(error_message.value)


def test_find_publication(db):
    assert not find_publication(db)[0]  # False
    assert find_publication(db, name='Ref 1')[0]  # True
    assert find_publication(db, name='Ref 1', doi='10.1093/mnras/staa1522')[0]  # True
    doi_search = find_publication(db, doi='10.1093/mnras/staa1522')
    assert doi_search[0]  # True
    assert doi_search[1] == 'Ref 1'
    bibcode_search = find_publication(db, bibcode='2020MNRAS.496.1922B')
    assert bibcode_search[0]  # True
    assert bibcode_search[1] == 'Ref 1'
    multiple_matches = find_publication(db, name='Ref')
    assert not multiple_matches[0]  # False, multiple matches
    assert multiple_matches[1] == 2  # multiple matches
    assert not find_publication(db, name='Ref 2', doi='10.1093/mnras/staa1522')[0]  # False
    assert not find_publication(db, name='Ref 2', bibcode='2020MNRAS.496.1922B')[0]  # False
    assert find_publication(db, name='Burningham_2008') == (1, 'Burn08')

def test_ingest_publication(db):
    # should fail if trying to add a duplicate record
    with pytest.raises(SimpleError) as error_message:
        ingest_publication(db, publication='Ref 1', bibcode='2020MNRAS.496.1922B')
    assert ' similar publication already exists' in str(error_message.value)
    # TODO - Mock environment  where ADS_TOKEN is not set. #117


def test_ingest_instrument(db):
    #  TESTS WHICH SHOULD WORK

    #  test adding just telescope
    ingest_instrument(db, telescope='test')
    telescope_db = db.query(db.Telescopes).filter(db.Telescopes.c.telescope == 'test').table()
    assert len(telescope_db) == 1
    assert telescope_db['telescope'][0] == 'test'

    #  test adding telescope and instrument
    tel_test = 'test2'
    inst_test = 'test3'
    ingest_instrument(db, telescope=tel_test, instrument=inst_test)
    telescope_db = db.query(db.Telescopes).filter(db.Telescopes.c.telescope == tel_test).table()
    instrument_db = db.query(db.Instruments).filter(db.Instruments.c.instrument == inst_test).table()
    assert len(telescope_db) == 1
    assert telescope_db['telescope'][0] == tel_test
    assert len(instrument_db) == 1
    assert instrument_db['instrument'][0] == inst_test

    #  test adding new telescope, instrument, and mode
    tel_test = 'test4'
    inst_test = 'test5'
    mode_test = 'test6'
    ingest_instrument(db, telescope=tel_test, instrument=inst_test, mode=mode_test)
    telescope_db = db.query(db.Telescopes).filter(db.Telescopes.c.telescope == tel_test).table()
    instrument_db = db.query(db.Instruments).filter(db.Instruments.c.instrument == inst_test).table()
    mode_db = db.query(db.Modes).filter(db.Modes.c.mode == mode_test).table()
    assert len(telescope_db) == 1
    assert telescope_db['telescope'][0] == tel_test
    assert len(instrument_db) == 1
    assert instrument_db['instrument'][0] == inst_test
    assert len(mode_db) == 1
    assert mode_db['mode'][0] == mode_test

    #  test adding common mode name for new telescope, instrument
    tel_test = 'test4'
    inst_test = 'test5'
    mode_test = 'Prism'
    ingest_instrument(db, telescope=tel_test, instrument=inst_test, mode=mode_test)
    mode_db = db.query(db.Modes).filter(and_(db.Modes.c.mode == mode_test,
                                             db.Modes.c.instrument == inst_test,
                                             db.Modes.c.telescope == tel_test)).table()
    assert len(mode_db) == 1
    assert mode_db['mode'][0] == mode_test

    #  TESTS WHICH SHOULD FAIL
    #  test with no variables provided
    with pytest.raises(SimpleError) as error_message:
        ingest_instrument(db)
    assert 'Telescope, Instrument, and Mode must be provided' in str(error_message.value)

    #  test with mode but no instrument or telescope
    with pytest.raises(SimpleError) as error_message:
        ingest_instrument(db, mode='test')
    assert 'Telescope and instrument must be provided to ingest a mode' in str(error_message.value)

# TODO: test for ingest_photometry

# TODO: test for ingest_spectra
