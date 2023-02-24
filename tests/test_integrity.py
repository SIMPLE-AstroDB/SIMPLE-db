# Test to verify database integrity

import os
from operator import and_

import pytest
from . import REFERENCE_TABLES
from sqlalchemy import func, select, except_
from simple.schema import *
from astrodbkit2.astrodb import create_database, Database, or_
from astropy.table import unique
from astropy import units as u
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

    # Load data into an in-memory sqlite database first, for performance
    temp_db = Database('sqlite://', reference_tables=REFERENCE_TABLES)  # creates and connects to a temporary in-memory database
    temp_db.load_database(DB_PATH, verbose=False)  # loads the data from the data files into the database
    temp_db.dump_sqlite(DB_NAME)  # dump in-memory database to file
    db = Database('sqlite:///' + DB_NAME, reference_tables=REFERENCE_TABLES)  # replace database object with new file version

    return db


def test_reference_uniqueness(db):
    # Verify that all Publications.name values are unique
    t = db.query(db.Publications.c.publication).astropy()
    assert len(t) == len(unique(t, keys='publication')), 'Duplicated Publications found'

    # Verify that DOI are supplied
    t = db.query(db.Publications.c.publication).filter(db.Publications.c.doi.is_(None)).astropy()
    if len(t) > 0:
        print(f'\n{len(t)} publications lacking DOI:')
        print(t)

    # Verify that Bibcodes are supplied
    t = db.query(db.Publications.c.publication).filter(db.Publications.c.bibcode.is_(None)).astropy()
    if len(t) > 0:
        print(f'\n{len(t)} publications lacking Bibcodes:')
        print(t)


def test_references(db):
    # Verify that all data point to an existing Publication

    ref_list = []
    table_list = ['Sources', 'Photometry', 'Parallaxes', 'ProperMotions', 'Spectra']
    for table in table_list:
        # Get list of unique references
        t = db.query(db.metadata.tables[table].c.reference).distinct().astropy()
        ref_list = ref_list + t['reference'].tolist()

    # Getting unique set
    ref_list = list(set(ref_list))

    # Confirm that all are in Publications
    t = db.query(db.Publications.c.publication).filter(db.Publications.c.publication.in_(ref_list)).astropy()
    assert len(t) == len(ref_list), 'Some references were not matched'

    # List out publications that have not been used
    t = db.query(db.Publications.c.publication).filter(db.Publications.c.publication.notin_(ref_list)).astropy()
    assert len(t) <= 606, f'{len(t)} unused references'


def test_publications(db):
    # Find unused references in the Sources Table
    # stm = except_(select([db.Publications.c.publication]), select([db.Sources.c.reference]))
    # result = db.session.execute(stm)
    # s = result.scalars().all()
    # assert len(s) == 720, f'found {len(s)} unused references'

    # Find references with no doi or bibcode
    t = db.query(db.Publications.c.publication).filter(
        or_(and_(db.Publications.c.doi.is_(None), db.Publications.c.bibcode.is_(None)),
            and_(db.Publications.c.doi.is_(''), db.Publications.c.bibcode.is_(None)),
            and_(db.Publications.c.doi.is_(None), db.Publications.c.bibcode.is_('')),
            and_(db.Publications.c.doi.is_(''), db.Publications.c.bibcode.is_('')))).astropy()
    assert len(t) == 27, f'found {len(t)} publications with missing bibcode and doi'


def test_parameters(db):
    """
    Test the Parameters table exists and has data
    """

    t = db.query(db.Parameters).astropy()
    assert len(t) > 0, 'Parameters table is empty'

    # Check usage of Parameters
    param_list = db.query(db.ModeledParameters.c.parameter).astropy()
    if len(param_list) > 0:
        # Get unique values
        param_list = param_list['parameter'].to_list()
        param_list = list(set(param_list))
        t = db.query(db.Parameters).filter(db.Parameters.c.parameter.notin_(param_list)).astropy()
        if len(t) > 0:
            print('The following parameters are not being used:')
            print(t)
        # Skipping actual assertion test
        # assert len(t) == 0, f'{len(t)} unused parameters'


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
    source_name_counts = db.query(db.Names.c.source). \
        filter(db.Names.c.source.in_(name_list)). \
        distinct(). \
        count()
    assert len(name_list) == source_name_counts, 'ERROR: There are Sources without entries in the Names table'

    # Verify that each Source contains an entry in Names with Names.source = Names.other_source
    valid_name_counts = db.query(db.Names.c.source). \
        filter(db.Names.c.source == db.Names.c.other_name). \
        distinct(). \
        count()

    # If the number of valid names don't match the number of sources, then there are cases that are missing
    # The script below will gather them and print them out
    if len(name_list) != valid_name_counts:
        # Create a temporary table that groups entries in the Names table by their source name
        # with a column containing a concatenation of all known names
        t = db.query(db.Names.c.source,
                     func.group_concat(db.Names.c.other_name).label('names')). \
            group_by(db.Names.c.source). \
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

    # Verify that there are no empty strings as other_names in Names
    blank_names = db.query(db.Names).filter(db.Names.c.other_name == '').astropy()
    assert len(blank_names) == 0, \
        'ERROR: There are entries in Names which are empty strings'


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


def test_photometry_filters(db):
    bands_in_use = db.query(db.Photometry.c.band).distinct().astropy()
    for band_in_use in bands_in_use['band']:
        check = db.query(db.PhotometryFilters).filter(db.PhotometryFilters.c.band == band_in_use).astropy()
        assert len(check) == 1, f'{band_in_use} not in PhotometryFilters'


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
    t = db.query(db.ProperMotions.c.source). \
        filter(or_(db.ProperMotions.c.mu_ra.is_(None),
                   db.ProperMotions.c.mu_dec.is_(None))). \
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
    t = db.query(db.RadialVelocities.c.source). \
        filter(db.RadialVelocities.c.radial_velocity.is_(None)). \
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


def test_sources(db):
    # Counting the top 20 references in the Sources Table
    spec_ref_count = db.query(Sources.reference, func.count(Sources.reference)). \
        group_by(Sources.reference).order_by(func.count(Sources.reference).desc()).limit(20).all()

    # Top 20 References in the Sources Table

    ref = 'Schm10.1808'
    t = db.query(db.Sources).filter(db.Sources.c.reference == ref).astropy()
    assert len(t) == 208, f'found {len(t)} sources from {ref}'

    ref = 'Reid08b'
    t = db.query(db.Sources).filter(db.Sources.c.reference == ref).astropy()
    assert len(t) == 206, f'found {len(t)} sources from {ref}'

    ref = 'West08'
    t = db.query(db.Sources).filter(db.Sources.c.reference == ref).astropy()
    assert len(t) == 192, f'found {len(t)} sources from {ref}'

    ref = 'Cruz03'
    t = db.query(db.Sources).filter(db.Sources.c.reference == ref).astropy()
    assert len(t) == 165, f'found {len(t)} sources from {ref}'

    ref = 'Maro15'
    t = db.query(db.Sources).filter(db.Sources.c.reference == ref).astropy()
    assert len(t) == 113, f'found {len(t)} sources from {ref}'

    ref = 'Best15'
    t = db.query(db.Sources).filter(db.Sources.c.reference == ref).astropy()
    assert len(t) == 101, f'found {len(t)} sources from {ref}'

    ref = 'Kirk11'
    t = db.query(db.Sources).filter(db.Sources.c.reference == ref).astropy()
    assert len(t) == 100, f'found {len(t)} sources from {ref}'

    ref = 'Mace13.6'
    t = db.query(db.Sources).filter(db.Sources.c.reference == ref).astropy()
    assert len(t) == 93, f'found {len(t)} sources from {ref}'

    ref = 'Cruz07'
    t = db.query(db.Sources).filter(db.Sources.c.reference == ref).astropy()
    assert len(t) == 91, f'found {len(t)} sources from {ref}'

    ref = 'Burn13'
    t = db.query(db.Sources).filter(db.Sources.c.reference == ref).astropy()
    assert len(t) == 69, f'found {len(t)} sources from {ref}'

    ref = 'Gagn15b'
    t = db.query(db.Sources).filter(db.Sources.c.reference == ref).astropy()
    assert len(t) == 68, f'found {len(t)} sources from {ref}'

    ref = 'Chiu06'
    t = db.query(db.Sources).filter(db.Sources.c.reference == ref).astropy()
    assert len(t) == 62, f'found {len(t)} sources from {ref}'

    ref = 'Kirk00'
    t = db.query(db.Sources).filter(db.Sources.c.reference == ref).astropy()
    assert len(t) == 61, f'found {len(t)} sources from {ref}'

    ref = 'DayJ13'
    t = db.query(db.Sources).filter(db.Sources.c.reference == ref).astropy()
    assert len(t) == 61, f'found {len(t)} sources from {ref}'

    ref = 'Kirk10'
    t = db.query(db.Sources).filter(db.Sources.c.reference == ref).astropy()
    assert len(t) == 56, f'found {len(t)} sources from {ref}'

    ref = 'Deac14b'
    t = db.query(db.Sources).filter(db.Sources.c.reference == ref).astropy()
    assert len(t) == 52, f'found {len(t)} sources from {ref}'

    ref = 'Hawl02'
    t = db.query(db.Sources).filter(db.Sources.c.reference == ref).astropy()
    assert len(t) == 51, f'found {len(t)} sources from {ref}'

    ref = 'Card15'
    t = db.query(db.Sources).filter(db.Sources.c.reference == ref).astropy()
    assert len(t) == 45, f'found {len(t)} sources from {ref}'

    ref = 'Burn10.1885'
    t = db.query(db.Sources).filter(db.Sources.c.reference == ref).astropy()
    assert len(t) == 43, f'found {len(t)} sources from {ref}'

    ref = 'Albe11'
    t = db.query(db.Sources).filter(db.Sources.c.reference == ref).astropy()
    assert len(t) == 37, f'found {len(t)} sources from {ref}'


# importing table 9 for data in test parameters
from astropy.table import Table

def test_ingest_modeledparameters(db):
    # organized data
    radius_data = Table([ {'source': '0000+2554', 'value': '0.99 +or- 0.15', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0003-2822', 'value': '1.32 +or- 0.05', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0004-4044BC', 'value': 'None', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0004-6410', 'value': '1.63 +or- 0.06', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0024-0158', 'value': '1.09 +or- 0.05', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0025+4759', 'value': 'None', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0027+0503', 'value': '1.44 +or- 0.23', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0027+2219', 'value': 'None', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0030-1450', 'value': '0.98 +or- 0.10', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0033-1521', 'value': '1.43 +or- 0.22', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0034+0523', 'value': '0.94 +or- 0.16', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0036+1821', 'value': '1.01 +or- 0.07', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0039+2115', 'value': '0.86 +or- 0.07', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0045+1634', 'value': '1.62 +or- 0.06', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0047+6803', 'value': '1.30 +or- 0.04', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0050-3322', 'value': '0.94 +or- 0.16', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0058-0651', 'value': '1.43 +or- 0.22', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0102-3737', 'value': '1.17 +or- 0.04', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0103+1935', 'value': '1.34 +or- 0.13', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0107+0041', 'value': '0.98 +or- 0.11', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0112+1703', 'value': '1.24 +or- 0.04', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0117-3403', 'value': '1.62 +or- 0.11', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0141-4633', 'value': '1.61 +or- 0.10', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0151+1244', 'value': '0.97 +or- 0.16', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0207+0000', 'value': '0.97 +or- 0.17', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0210-3015', 'value': '1.53 +or- 0.09', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0234-6442', 'value': '1.62 +or- 0.11', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0241-0326', 'value': '1.41 +or- 0.21', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0243-2453', 'value': '0.94 +or- 0.16', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0248-1651', 'value': '1.08 +or- 0.06', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0251+0047', 'value': '1.28 +or- 0.09', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0253+1652', 'value': '1.20 +or- 0.09', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0253+3206', 'value': '1.90 +or- 0.45', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0254+0223', 'value': '0.97 +or- 0.17', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0255-4700', 'value': '0.97 +or- 0.12', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0318-3421', 'value': '0.97 +or- 0.12', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0320+1854', 'value': '1.15 +or- 0.04', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0323-4631', 'value': '1.66 +or- 0.15', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0325+0425', 'value': '0.95 +or- 0.17', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0326-2102', 'value': '1.30 +or- 0.07', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0328+2302', 'value': '0.98 +or- 0.10', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0334-4953', 'value': '1.04 +or- 0.06', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0339-3525', 'value': '1.43 +or- 0.21', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0345+2540', 'value': '1.05 +or- 0.05', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0351-0052', 'value': '1.27 +or- 0.04', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0355+1133', 'value': '1.32 +or- 0.09', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0357-4417', 'value': 'None', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0415-0935', 'value': '0.95 +or- 0.16', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0423-0414', 'value': 'None', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0428-2253', 'value': '1.09 +or- 0.05', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0435-1606', 'value': '1.26 +or- 0.03', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0436-4114', 'value': '1.97 +or- 0.39', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0439-2353', 'value': '0.97 +or- 0.12', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0443+0002', 'value': '1.78 +or- 0.12', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0445-3048', 'value': '1.00 +or- 0.08', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0451-3402', 'value': '1.04 +or- 0.06', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0500+0330', 'value': '1.00 +or- 0.08', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0501-0010', 'value': '1.38 +or- 0.18', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0516-0445', 'value': '0.97 +or- 0.17', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0523-1403', 'value': '1.01 +or- 0.07', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0539-0058', 'value': '0.99 +or- 0.08', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0559-1404', 'value': '0.97 +or- 0.11', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0602+3910', 'value': '1.41 +or- 0.20', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0608-2753', 'value': '1.51 +or- 0.27', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0610-2152B', 'value': '0.94 +or- 0.15', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0611-0410AB', 'value': 'None', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0619-5803b', 'value': '1.55 +or- 0.05', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0624-4521', 'value': '0.99 +or- 0.10', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0641-4322', 'value': '1.01 +or- 0.07', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0652-2534', 'value': '1.05 +or- 0.05', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0700+3157', 'value': 'None', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0707-4900', 'value': '1.03 +or- 0.07', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0722-0540', 'value': '0.98 +or- 0.16', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0727+1710', 'value': '0.94 +or- 0.16', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0729-3954', 'value': '0.94 +or- 0.16', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0742+2055', 'value': '0.94 +or- 0.16', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0746+2000', 'value': 'None', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0751-2530', 'value': '1.03 +or- 0.06', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0752+1612', 'value': '2.01 +or- 0.35', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0817-6155', 'value': '0.94 +or- 0.16', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0825+2115', 'value': '0.98 +or- 0.11', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0829+2646', 'value': '1.23 +or- 0.03', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0830+0128', 'value': '0.96 +or- 0.17', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0830+0947', 'value': '1.15 +or- 0.06', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0830+4828', 'value': '0.99 +or- 0.15', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0835-0819', 'value': '1.00 +or- 0.08', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0847-1532', 'value': '1.00 +or- 0.08', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0853-0329', 'value': '1.07 +or- 0.05', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0859-1949', 'value': '0.98 +or- 0.11', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0912+1459', 'value': 'None', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0920+3517', 'value': 'None', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0937+2931', 'value': '0.94 +or- 0.16', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0939-2448', 'value': '0.95 +or- 0.16', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '0949-1545', 'value': '0.96 +or- 0.17', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1004+5022', 'value': '1.36 +or- 0.23', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1007-4555', 'value': '0.94 +or- 0.16', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1010-0406', 'value': '0.98 +or- 0.11', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1017+1308', 'value': 'None', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1021-0304', 'value': 'None', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1022+4114', 'value': '1.04 +or- 0.06', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1022+5825', 'value': '1.41 +or- 0.20', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1036-3441', 'value': '0.97 +or- 0.12', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1047+2124', 'value': '0.94 +or- 0.16', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1048-3956', 'value': '1.07 +or- 0.05', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1049-3519A', 'value': '1.01 +or- 0.07', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1049-3519B', 'value': '1.02 +or- 0.07', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1056+0700', 'value': '1.56 +or- 0.10', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1058-1548', 'value': '1.00 +or- 0.07', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1102-3430', 'value': '2.39 +or- 0.14', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1110+0116', 'value': '1.24 +or- 0.04', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1112+3548', 'value': 'None', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1114-2618', 'value': '0.96 +or- 0.16', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1139-3159', 'value': '2.20 +or- 0.22', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1146+2230', 'value': 'None', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1154-3400', 'value': '1.50 +or- 0.26', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1155-3727', 'value': '1.00 +or- 0.08', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1207-3900', 'value': '1.71 +or- 0.09', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1207-3932', 'value': 'None', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1207-3932A', 'value': '2.33 +or- 0.14', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1207-3932B', 'value': '1.36 +or- 0.02', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1217-0311', 'value': '0.94 +or- 0.16', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1225-2739', 'value': 'None', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1228-1547', 'value': 'None', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1237+6526', 'value': '0.94 +or- 0.16', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1239+5515', 'value': 'None', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1245-4429', 'value': '2.00 +or- 0.23', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1254-0122', 'value': '0.98 +or- 0.15', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1300+1221', 'value': '1.06 +or- 0.28', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1305-2541', 'value': 'None', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1320+0409', 'value': '1.01 +or- 0.07', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1326-0038', 'value': '0.97 +or- 0.11', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1346-0031', 'value': '0.94 +or- 0.16', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1359-4034', 'value': '1.00 +or- 0.08', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1411-2119', 'value': '1.98 +or- 0.44', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1416+1348B', 'value': '0.96 +or- 0.16', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1416+5006', 'value': '0.99 +or- 0.09', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1424+0917', 'value': '1.00 +or- 0.08', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1425-3650', 'value': '1.32 +or- 0.09', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1428+3310', 'value': '1.06 +or- 0.05', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1439+1839', 'value': '1.24 +or- 0.04', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1439+1929', 'value': '1.03 +or- 0.06', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1440+1339', 'value': '1.19 +or- 0.04', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1448+1031', 'value': '0.99 +or- 0.09', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1456-2809', 'value': '1.12 +or- 0.05', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1457-2121', 'value': '0.94 +or- 0.16', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1501+2250', 'value': '1.05 +or- 0.05', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1503+2525', 'value': '0.94 +or- 0.16', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1504+1027', 'value': '0.94 +or- 0.16', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1506+1321', 'value': '1.02 +or- 0.07', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1507-1627', 'value': '0.99 +or- 0.09', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1510-0241', 'value': '1.08 +or- 0.06', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1511+0607', 'value': 'None', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1515+4847', 'value': '0.99 +or- 0.10', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1523+3014', 'value': '0.97 +or- 0.11', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1526+2043', 'value': '0.98 +or- 0.10', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1539-0520', 'value': '1.00 +or- 0.08', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1546-3325', 'value': '0.94 +or- 0.16', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1552+2948', 'value': '1.43 +or- 0.22', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1555-0956', 'value': '1.03 +or- 0.06', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1615+1340', 'value': '0.94 +or- 0.16', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1624+0029', 'value': '0.94 +or- 0.15', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1626+3925', 'value': '1.03 +or- 0.06', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1632+1904', 'value': '0.97 +or- 0.12', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1647+5632', 'value': '0.94 +or- 0.16', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1655-0823', 'value': '1.17 +or- 0.03', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1658+7026', 'value': '1.04 +or- 0.06', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1726+1538', 'value': '1.40 +or- 0.20', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1728+3948', 'value': 'None', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1750+1759', 'value': '0.97 +or- 0.16', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1828-4849', 'value': '0.95 +or- 0.16', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1835+3259', 'value': '1.07 +or- 0.05', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1841+3117', 'value': '1.01 +or- 0.07', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1843+4040', 'value': '1.22 +or- 0.03', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '1916+0508', 'value': '1.13 +or- 0.04', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '2000-7523', 'value': '1.88 +or- 0.11', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '2047-0718', 'value': '0.96 +or- 0.17', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '2057-0252', 'value': '1.02 +or- 0.07', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '2101+1756', 'value': 'None', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '2104-1037', 'value': '1.02 +or- 0.07', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '2114-2251', 'value': '1.41 +or- 0.03', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '2126-8140', 'value': '1.39 +or- 0.19', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '2127-4215', 'value': '1.21 +or- 0.13', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '2139+0220', 'value': '0.96 +or- 0.16', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '2144+1446', 'value': '1.17 +or- 0.05', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '2148+4003', 'value': '0.99 +or- 0.10', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '2206-4217', 'value': '1.33 +or- 0.10', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '2208+2921', 'value': '1.41 +or- 0.20', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '2224-0158', 'value': '0.99 +or- 0.08', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '2228-4310', 'value': '0.94 +or- 0.16', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '2234+4041', 'value': 'None', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '2237+3922', 'value': '1.04 +or- 0.06', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '2244+2043', 'value': '1.29 +or- 0.03', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '2306-0502', 'value': '1.14 +or- 0.04', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '2322-3133', 'value': '1.39 +or- 0.19', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '2322-6151', 'value': '1.59 +or- 0.05', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '2351-2537', 'value': '1.16 +or- 0.11', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ,
        {'source': '2354-3316', 'value': '1.09 +or- 0.05', 'parameter': 'radius', 'publication': 'Fili15', 'unit': '(R_Jup)'} ])  # change to csv

def test_modeledparameters(db):


    # There should be no entries in the modeled parameters table without parameter

    t = db.query(db.ModeledParameters.c.source). \
         filter(db.ModeledParameters.c.parameter.is_(None)). \
         astropy()
    #
    if len(t) > 0:
         print('\nEntries found without a parameter')
         print(t)
    assert len(t) == 0

    # Test units are astropy.unit resolvable
    t = db.query(db.ModeledParameters.c.unit).filter(db.ModeledParameters.c.unit.is_not(None)).distinct().astropy()
    unit_fail = []
    for x in t:
        unit = x['unit']

        try:
            assert u.Unit(unit, parse_strict='raise')
        except ValueError:
            print(f'{unit} is not a recognized astropy unit')
            counts = db.query(db.ModeledParameters).filter(db.ModeledParameters.c.unit == unit).count()
            unit_fail.append({unit: counts})  # count of how many of that unit there is

    assert len(unit_fail) == 0, f'Some parameter units did not resolve: {unit_fail}'


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

    # Test units are astropy.unit resolvable
    t = db.query(db.Spectra.c.wavelength_units).filter(db.Spectra.c.wavelength_units.is_not(None)).distinct().astropy()
    wave_unit_fail = []
    for x in t:
        unit = x['wavelength_units']
        try:
            assert u.Unit(unit, parse_strict='raise')
        except ValueError:
            print(f'{unit} is not a recognized astropy unit')
            counts = db.query(db.Spectra).filter(db.Spectra.c.wavelength_units == unit).count()
            wave_unit_fail.append({unit: counts})
    assert len(wave_unit_fail) == 0, f'Some wavelength units did not resolve: {wave_unit_fail}'

    t = db.query(db.Spectra.c.flux_units).filter(db.Spectra.c.flux_units.is_not(None)).distinct().astropy()
    flux_unit_fail = []
    for x in t:
        unit = x['flux_units']

        # Special exception for the 'normalized' unit
        if unit == 'normalized':
            continue

        try:
            assert u.Unit(unit, parse_strict='raise')
        except ValueError:
            print(f'{unit} is not a recognized astropy unit')
            counts = db.query(db.Spectra).filter(db.Spectra.c.flux_units == unit).count()
            flux_unit_fail.append({unit: counts})
    assert len(flux_unit_fail) == 0, f'Some flux units did not resolve: {flux_unit_fail}'



def test_special_characters(db):
    # This test asserts that no special unicode characters are in the database
    # This can be expanded with additional characters we want to avoid
    bad_characters = ['\u2013', '\u00f3', '\u00e9', '\u00ed', '\u00e1', '\u00fa']
    for char in bad_characters:
        data = db.search_string(char)
        # Check tables individually, want to make sure primary/foreign keys are verified but not comments/descriptions
        if len(data) > 0:
            for table_name in data.keys():
                if table_name == 'Publications':
                    check = [char not in data[table_name]['publication']]
                    assert all(check), f'{char} in {table_name}'
                elif table_name == 'Spectra':
                    check = [char not in data[table_name]['spectrum']]
                    assert all(check), f'{char} in {table_name}'
                elif table_name == 'Names':
                    check = [char not in data[table_name]['other_name']]
                    assert all(check), f'{char} in {table_name}'
                else:
                    check = [char not in data[table_name]['source']]
                    assert all(check), f'{char} in {table_name}'


def test_remove_database(db):
    # Clean up temporary database
    db.session.close()
    db.engine.dispose()
    if os.path.exists(DB_NAME):
        os.remove(DB_NAME)
