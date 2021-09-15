# Test to verify database integrity

import os
import pytest
from . import REFERENCE_TABLES
from sqlalchemy import func
from simple.schema import *
from astrodbkit2.astrodb import create_database, Database, or_
from astropy.table import unique
from astroquery.simbad import Simbad
from astrodbkit2.utils import _name_formatter

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
    db = Database(connection_string, reference_tables=REFERENCE_TABLES)
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
    missing_names = db.sql_query(sql_text, fmt='astropy')
    assert len(missing_names) == 0


def test_source_uniqueness(db):
    # Verify that all Sources.source values are unique
    source_names = db.query(db.Sources.c.source).astropy()
    unique_source_names = unique(source_names)
    assert len(source_names) == len(unique_source_names)

    # Another method to find the duplicates
    sql_text = "SELECT Sources.source FROM Sources GROUP BY source " \
               "HAVING (Count(*) > 1)"
    duplicate_names = db.sql_query(sql_text, fmt='astropy')

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


def test_source_uniqueness2(db):
    # Verify that all Sources.source values are unique and find the duplicates
    sql_text = "SELECT Sources.source FROM Sources GROUP BY source " \
               "HAVING (Count(*) > 1)"
    duplicate_names = db.sql_query(sql_text, fmt='astropy')
    # if duplicate_names is non_zero, print out duplicate names
    assert len(duplicate_names) == 0


@pytest.mark.skip(reason="SIMBAD unreliable")
def test_source_simbad(db):
    # Query Simbad and confirm that there are no duplicates with different names

    # Get list of all source names
    results = db.query(db.Sources.c.source).all()
    name_list = [s[0] for s in results]

    # Add all IDS to the Simbad output as well as the user-provided id
    Simbad.add_votable_fields('ids')
    Simbad.add_votable_fields('typed_id')

    simbad_results = Simbad.query_objects(name_list)
    # Get a nicely formatted list of Simbad names for each input row
    duplicate_count = 0
    for row in simbad_results[['TYPED_ID', 'IDS']].iterrows():
        try:
            name, ids = row[0].decode("utf-8"), row[1].decode("utf-8")
        except AttributeError:
            # Catch decoding error
            name, ids = row[0], row[1]

        simbad_names = [_name_formatter(s) for s in ids.split('|')
                        if _name_formatter(s) != '' and _name_formatter(s) is not None]

        if len(simbad_names) == 0:
            print(f'No Simbad names for {name}')
            continue

        # Examine DB for each input, displaying results when more than one source matches
        t = db.search_object(simbad_names, output_table='Sources', fmt='astropy', fuzzy_search=False)
        if len(t) > 1:
            print(f'Multiple matches for {name}: {simbad_names}')
            print(db.query(db.Names).filter(db.Names.c.source.in_(t['source'])).astropy())
            duplicate_count += 1

    assert duplicate_count == 0, 'Duplicate sources identified via Simbad queries'


def test_photometry(db):
    # Tests for Photometry table

    # Check that no negative magnitudes have been provided,
    # nor any that are larger than 99 (if missing/limits, just use None)
    t = db.query(db.Photometry). \
        filter(or_(db.Photometry.c.magnitude < 0,
                   db.Photometry.c.magnitude >= 99)). \
        astropy()
    if len(t) > 0:
        print('\nInvalid magnitudes present')
        print(t)
    assert len(t) == 0


def test_parallaxes(db):
    # Tests against the Parallaxes table

    # While there may be many parallax measurements for a single source,
    # there should be only one marked as adopted
    t = db.query(db.Parallaxes.c.source,
                 func.sum(db.Parallaxes.c.adopted).label('adopted_counts')). \
        group_by(db.Parallaxes.c.source). \
        having(func.sum(db.Parallaxes.c.adopted) > 1). \
        astropy()
    if len(t) > 0:
        print("\nParallax entries with incorrect 'adopted' labels")
        print(t)
    assert len(t) == 0


def test_propermotions(db):
    # Tests against the ProperMotions table

    # There should be no entries in the ProperMotions table without both mu_ra and mu_dec
    t = db.query(db.ProperMotions.c.source).\
        filter(or_(db.ProperMotions.c.mu_ra.is_(None),
                   db.ProperMotions.c.mu_dec.is_(None))).\
        astropy()
    if len(t) > 0:
        print('\nEntries found without proper motion values')
        print(t)
    assert len(t) == 0

    # While there may be many proper motion measurements for a single source,
    # there should be only one marked as adopted
    t = db.query(db.ProperMotions.c.source,
                 func.sum(db.ProperMotions.c.adopted).label('adopted_counts')). \
        group_by(db.ProperMotions.c.source). \
        having(func.sum(db.ProperMotions.c.adopted) > 1). \
        astropy()
    if len(t) > 0:
        print("\nProper Motion measurements with incorrect 'adopted' labels")
        print(t)
    assert len(t) == 0

def test_radialvelocities(db):
    # Tests against the RadialVelocities table

    # There should be no entries in the RadialVelocities table without rv values
    t = db.query(db.RadialVelocities.c.source).\
        filter(db.RadialVelocities.c.radial_velocity.is_(None)).\
        astropy()
    if len(t) > 0:
        print('\nEntries found without radial velocity values')
        print(t)
    assert len(t) == 0

    # While there may be many radial velocity measurements for a single source,
    # there should be only one marked as adopted
    t = db.query(db.RadialVelocities.c.source,
                 func.sum(db.RadialVelocities.c.adopted).label('adopted_counts')). \
        group_by(db.RadialVelocities.c.source). \
        having(func.sum(db.RadialVelocities.c.adopted) > 1). \
        astropy()
    if len(t) > 0:
        print("\nRadial velocity measurements with incorrect 'adopted' labels")
        print(t)
    assert len(t) == 0

def test_spectraltypes(db):
    # Tests against the SpectralTypes table

    # There should be no entries in the SpectralTypes table without a spectral type string
    t = db.query(db.SpectralTypes.c.source). \
        filter(db.SpectralTypes.c.spectral_type_string.is_(None)). \
        astropy()
    if len(t) > 0:
        print('\nEntries found without spectral type strings')
        print(t)
    assert len(t) == 0

    # There should be no entries in the SpectralTypes table without a spectral type code
    t = db.query(db.SpectralTypes.c.source). \
        filter(db.SpectralTypes.c.spectral_type_code.is_(None)). \
        astropy()
    if len(t) > 0:
        print('\nEntries found without spectral type codes')
        print(t)
    assert len(t) == 0

    # While there may be many spectral type measurements for a single source,
    # there should be only one marked as adopted
    t = db.query(db.SpectralTypes.c.source,
                 func.sum(db.SpectralTypes.c.adopted).label('adopted_counts')). \
        group_by(db.SpectralTypes.c.source). \
        having(func.sum(db.SpectralTypes.c.adopted) > 1). \
        astropy()
    if len(t) > 0:
        print("\nSpectral Type entries with incorrect 'adopted' labels")
        print(t)
    assert len(t) == 0


def test_gravities(db):
    # Tests against the Gravities table

    # There should be no entries in the Gravities table without a gravity measurement
    t = db.query(db.Gravities.c.source). \
        filter(db.Gravities.c.gravity.is_(None)). \
        astropy()
    if len(t) > 0:
        print('\nEntries found without gravity values')
        print(t)
    assert len(t) == 0


def test_spectra(db):
    # Tests against the Spectra table

    # There should be no entries in the Spectra table without a spectrum
    t = db.query(db.Spectra.c.source). \
        filter(db.Spectra.c.spectrum.is_(None)). \
        astropy()
    if len(t) > 0:
        print('\nEntries found without spectrum')
        print(t)
    assert len(t) == 0

    # TODO: Consider testing that units are astropy.units resolvable?


def test_remove_database(db):
    # Clean up temporary database
    db.session.close()
    db.engine.dispose()
    if os.path.exists(DB_NAME):
        os.remove(DB_NAME)
