# Tests to verify database contents

import os
import pytest
import sys

sys.path.append('.')
from simple.schema import *
from astrodbkit2.astrodb import create_database, Database
from sqlalchemy import *
from . import REFERENCE_TABLES

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

    # Load the database contents
    # This should take care of finding serious issues (key/column violations)
    db.load_database(DB_PATH, verbose=False)

    return db


# Utility functions
# -----------------------------------------------------------------------------------------
def reference_verifier(t, name, bibcode, doi):
    # Utility function to verify reference values in a table
    ind = t['publication'] == name
    assert t[ind]['bibcode'][0] == bibcode, f'{name} did not match bibcode'
    assert t[ind]['doi'][0] == doi, f'{name} did not match doi'


def test_discovery_references(db):
    '''
    Values found with this SQL query:
        SELECT reference, count(*)
        FROM Sources
        GROUP BY reference
        ORDER By 2 DESC

    '''

    ref = 'Schm10.1808'
    t = db.query(db.Sources).filter(db.Sources.c.reference == ref).astropy()
    assert len(t) == 208, f'found {len(t)} discovery reference entries for {ref}'

    ref = 'West08'
    t = db.query(db.Sources).filter(db.Sources.c.reference == ref).astropy()
    assert len(t) == 192, f'found {len(t)} discovery reference entries for {ref}'

    ref = 'Reid08b'
    t = db.query(db.Sources).filter(db.Sources.c.reference == ref).astropy()
    assert len(t) == 206, f'found {len(t)} discovery reference entries for {ref}'

    ref = 'Cruz03'
    t = db.query(db.Sources).filter(db.Sources.c.reference == ref).astropy()
    assert len(t) == 165, f'found {len(t)} discovery reference entries for {ref}'

    ref = 'Maro15'
    t = db.query(db.Sources).filter(db.Sources.c.reference == ref).astropy()
    assert len(t) == 113, f'found {len(t)} discovery reference entries for {ref}'

    ref = 'Best15'
    t = db.query(db.Sources).filter(db.Sources.c.reference == ref).astropy()
    assert len(t) == 101, f'found {len(t)} discovery reference entries for {ref}'

    ref = 'Kirk11'
    t = db.query(db.Sources).filter(db.Sources.c.reference == ref).astropy()
    assert len(t) == 98, f'found {len(t)} discovery reference entries for {ref}'

    ref = 'Mace13'
    t = db.query(db.Sources).filter(db.Sources.c.reference == ref).astropy()
    assert len(t) == 93, f'found {len(t)} discovery reference entries for {ref}'

    ref = 'Burn13'
    t = db.query(db.Sources).filter(db.Sources.c.reference == ref).astropy()
    assert len(t) == 69, f'found {len(t)} discovery reference entries for {ref}'

    ref = 'Gagn15b'
    t = db.query(db.Sources).filter(db.Sources.c.reference == ref).astropy()
    assert len(t) == 68, f'found {len(t)} discovery reference entries for {ref}'

    ref = 'Chiu06'
    t = db.query(db.Sources).filter(db.Sources.c.reference == ref).astropy()
    assert len(t) == 62, f'found {len(t)} discovery reference entries for {ref}'

    ref = 'DayJ13'
    t = db.query(db.Sources).filter(db.Sources.c.reference == ref).astropy()
    assert len(t) == 61, f'found {len(t)} discovery reference entries for {ref}'

    ref = 'Kirk10'
    t = db.query(db.Sources).filter(db.Sources.c.reference == ref).astropy()
    assert len(t) == 56, f'found {len(t)} discovery reference entries for {ref}'

    ref = 'Cruz07'
    t = db.query(db.Sources).filter(db.Sources.c.reference == ref).astropy()
    assert len(t) == 91, f'found {len(t)} discovery reference entries for {ref}'


def test_proper_motion_refs(db):
    """
    Values found with this SQL query:
        SELECT reference, count(*)
        FROM ProperMotions
        GROUP BY reference
        ORDER By 2 DESC

    from sqlalchemy import func
    proper_motion_mearsurements = db.query(ProperMotions.reference, func.count(ProperMotions.reference)).\
        group_by(ProperMotions.reference).order_by(func.count(ProperMotions.reference).desc()).limit(20).all()
    """
    ref = 'GaiaEDR3'
    t = db.query(db.ProperMotions).filter(db.ProperMotions.c.reference == ref).astropy()
    assert len(t) == 1133, f'found {len(t)} proper motion reference entries for {ref}'

    ref = 'GaiaDR2'
    t = db.query(db.ProperMotions).filter(db.ProperMotions.c.reference == ref).astropy()
    assert len(t) == 1076, f'found {len(t)} proper motion reference entries for {ref}'

    ref = 'Best20a'
    t = db.query(db.ProperMotions).filter(db.ProperMotions.c.reference == ref).astropy()
    assert len(t) == 348, f'found {len(t)} proper motion reference entries for {ref}'

    ref = 'Gagn15a'
    t = db.query(db.ProperMotions).filter(db.ProperMotions.c.reference == ref).astropy()
    assert len(t) == 325, f'found {len(t)} proper motion reference entries for {ref}'

    ref = 'Fahe09'
    t = db.query(db.ProperMotions).filter(db.ProperMotions.c.reference == ref).astropy()
    assert len(t) == 216, f'found {len(t)} proper motion reference entries for {ref}'

    # Kirk19 tested below.

    ref = 'Best15'
    t = db.query(db.ProperMotions).filter(db.ProperMotions.c.reference == ref).astropy()
    assert len(t) == 120, f'found {len(t)} proper motion reference entries for {ref}'

    ref = 'Burn13'
    t = db.query(db.ProperMotions).filter(db.ProperMotions.c.reference == ref).astropy()
    assert len(t) == 97, f'found {len(t)} proper motion reference entries for {ref}'

    ref = 'Dahn17'
    t = db.query(db.ProperMotions).filter(db.ProperMotions.c.reference == ref).astropy()
    assert len(t) == 79, f'found {len(t)} proper motion reference entries for {ref}'

    ref = 'Jame08'
    t = db.query(db.ProperMotions).filter(db.ProperMotions.c.reference == ref).astropy()
    assert len(t) == 73, f'found {len(t)} proper motion reference entries for {ref}'

    ref = 'vanL07'
    t = db.query(db.ProperMotions).filter(db.ProperMotions.c.reference == ref).astropy()
    assert len(t) == 68, f'found {len(t)} proper motion reference entries for {ref}'

    ref = 'Smar18'
    t = db.query(db.ProperMotions).filter(db.ProperMotions.c.reference == ref).astropy()
    assert len(t) == 68, f'found {len(t)} proper motion reference entries for {ref}'

    ref = 'Schm10.1808'
    t = db.query(db.ProperMotions).filter(db.ProperMotions.c.reference == ref).astropy()
    assert len(t) == 44, f'found {len(t)} proper motion reference entries for {ref}'


def test_parallax_refs(db):
    # Test total odopted measuruments
    t = db.query(db.Parallaxes).filter(db.Parallaxes.c.adopted == 1).astropy()
    assert len(t) == 1441, f'found {len(t)} adopted parallax measuruments.'

    ref = 'GaiaDR2'
    t = db.query(db.Parallaxes).filter(db.Parallaxes.c.reference == ref).astropy()
    assert len(t) == 1076, f'found {len(t)} parallax reference entries for {ref}'

    t = db.query(db.Parallaxes).filter(and_(db.Parallaxes.c.reference == ref,
                                            db.Parallaxes.c.adopted == 1)).astropy()
    assert len(t) == 36, f'found {len(t)} adopted parallax reference entries for {ref}'

    ref = 'GaiaEDR3'
    t = db.query(db.Parallaxes).filter(db.Parallaxes.c.reference == ref).astropy()
    assert len(t) == 1133, f'found {len(t)} parallax reference entries for {ref}'

    t = db.query(db.Parallaxes).filter(and_(db.Parallaxes.c.reference == ref,
                                            db.Parallaxes.c.adopted == 1)).astropy()
    assert len(t) == 1104, f'found {len(t)} adopted parallax reference entries for {ref}'


def test_photometry_bands(db):
    band = 'GAIA2.G'
    t = db.query(db.Photometry).filter(db.Photometry.c.band == band).astropy()
    assert len(t) == 1266, f'found {len(t)} photometry measurements for {band}'

    band = 'GAIA2.Grp'
    t = db.query(db.Photometry).filter(db.Photometry.c.band == band).astropy()
    assert len(t) == 1106, f'found {len(t)} photometry measurements for {band}'

    band = 'GAIA3.G'
    t = db.query(db.Photometry).filter(db.Photometry.c.band == band).astropy()
    assert len(t) == 1256, f'found {len(t)} photometry measurements for {band}'

    band = 'GAIA3.Grp'
    t = db.query(db.Photometry).filter(db.Photometry.c.band == band).astropy()
    assert len(t) == 1261, f'found {len(t)} photometry measurements for {band}'

    band = 'WISE.W1'
    t = db.query(db.Photometry).filter(db.Photometry.c.band == band).astropy()
    assert len(t) == 349, f'found {len(t)} photometry measurements for {band}'

    band = 'WISE.W2'
    t = db.query(db.Photometry).filter(db.Photometry.c.band == band).astropy()
    assert len(t) == 349, f'found {len(t)} photometry measurements for {band}'

    band = 'WISE.W3'
    t = db.query(db.Photometry).filter(db.Photometry.c.band == band).astropy()
    assert len(t) == 347, f'found {len(t)} photometry measurements for {band}'

    band = 'WISE.W4'
    t = db.query(db.Photometry).filter(db.Photometry.c.band == band).astropy()
    assert len(t) == 340, f'found {len(t)} photometry measurements for {band}'

    band = '2MASS.J'
    t = db.query(db.Photometry).filter(db.Photometry.c.band == band).astropy()
    assert len(t) == 1789, f'found {len(t)} photometry measurements for {band}'

    band = '2MASS.H'
    t = db.query(db.Photometry).filter(db.Photometry.c.band == band).astropy()
    assert len(t) == 1778, f'found {len(t)} photometry measurements for {band}'

    band = '2MASS.Ks'
    t = db.query(db.Photometry).filter(db.Photometry.c.band == band).astropy()
    assert len(t) == 1751, f'found {len(t)} photometry measurements for {band}'


# TODO: Tests of Gaia and 2MASS designations
def test_missions(db):
    # If 2MASS designation in Names, 2MASS photometry should exist
    stm = except_(select([db.Names.c.source]).where(db.Names.c.other_name.like("2MASS J%")),
                  select([db.Photometry.c.source]).where(db.Photometry.c.band.like("2MASS%")))
    result = db.session.execute(stm)
    s = result.scalars().all()
    assert len(s) == 265, f'found {len(stm)} spectra with 2MASS designation that have no 2MASS photometry'

    # If 2MASS photometry, 2MASS designation should be in Names
    stm = except_(select([db.Photometry.c.source]).where(db.Photometry.c.band.like("2MASS%")),
                  select([db.Names.c.source]).where(db.Names.c.other_name.like("2MASS J%")))
    result = db.session.execute(stm)
    s = result.scalars().all()
    assert len(s) == 0, f'found {len(stm)} spectra with 2MASS photometry that have no 2MASS designation '

    # If Gaia designation in Names, Gaia photometry and astrometry should exist
    stm = except_(select([db.Names.c.source]).where(db.Names.c.other_name.like("Gaia%")),
                  select([db.Photometry.c.source]).where(db.Photometry.c.band.like("GAIA%")))
    result = db.session.execute(stm)
    s = result.scalars().all()
    assert len(s) == 1, f'found {len(stm)} spectra with Gaia designation that have no GAIA photometry'

    # If Gaia photometry, Gaia designation should be in Names
    stm = except_(select([db.Photometry.c.source]).where(db.Photometry.c.band.like("GAIA%")),
                  select([db.Names.c.source]).where(db.Names.c.other_name.like("Gaia%")))
    result = db.session.execute(stm)
    s = result.scalars().all()
    assert len(s) == 0, f'found {len(stm)} spectra with Gaia photometry and no Gaia designation in Names'

    # If Wise designation in Names, Wise phot should exist
    stm = except_(select([db.Names.c.source]).where(db.Names.c.other_name.like("WISE%")),
                  select([db.Photometry.c.source]).where(db.Photometry.c.band.like("WISE%")))
    result = db.session.execute(stm)
    s = result.scalars().all()
    assert len(s) == 392, f'found {len(stm)} spectra with WISE designation that have no WISE photometry'

    # If Wise photometry, Wise designation should be in Names
    stm = except_(select([db.Photometry.c.source]).where(db.Photometry.c.band.like("WISE%")),
                  select([db.Names.c.source]).where(db.Names.c.other_name.like("WISE%")))
    result = db.session.execute(stm)
    s = result.scalars().all()
    assert len(s) == 278, f'found {len(stm)} spectra with Wise photometry and no Wise designation in Names'

    # If Gaia DR2 pm, Gaia DR2 designation should be in Names
    stm = except_(select([db.ProperMotions.c.source]).where(db.ProperMotions.c.reference.like("GaiaDR2%")),
                  select([db.Names.c.source]).where(db.Names.c.other_name.like("Gaia DR2%")))
    result = db.session.execute(stm)
    s = result.scalars().all()
    assert len(s) == 0, f'found {len(stm)} spectra with Gaia DR2 proper motion and no Gaia DR2 designation in Names'

    # If Gaia EDR3 pm, Gaia EDR3 designation should be in Names
    stm = except_(select([db.ProperMotions.c.source]).where(db.ProperMotions.c.reference.like("GaiaEDR3%")),
                  select([db.Names.c.source]).where(db.Names.c.other_name.like("Gaia EDR3%")))
    result = db.session.execute(stm)
    s = result.scalars().all()
    assert len(s) == 0, f'found {len(stm)} spectra with Gaia EDR3 proper motion and no Gaia EDR3 designation in Names'

    # If Gaia DR2 parallax, Gaia DR2 designation should be in Names
    stm = except_(select([db.Parallaxes.c.source]).where(db.Parallaxes.c.reference.like("GaiaDR2%")),
                  select([db.Names.c.source]).where(db.Names.c.other_name.like("Gaia DR2%")))
    result = db.session.execute(stm)
    s = result.scalars().all()
    assert len(s) == 0, f'found {len(stm)} spectra with Gaia DR2 proper motion and no Gaia DR2 designation in Names'

    # If Gaia EDR3 parallax, Gaia EDR3 designation should be in Names
    stm = except_(select([db.Parallaxes.c.source]).where(db.Parallaxes.c.reference.like("GaiaEDR3%")),
                  select([db.Names.c.source]).where(db.Names.c.other_name.like("Gaia EDR3%")))
    result = db.session.execute(stm)
    s = result.scalars().all()
    assert len(s) == 0, f'found {len(stm)} spectra with Gaia EDR3 proper motion and no Gaia EDR3 designation in Names'


def test_spectra(db):
    regime = 'optical'
    t = db.query(db.Spectra).filter(db.Spectra.c.regime == regime).astropy()
    assert len(t) == 718, f'found {len(t)} spectra in the {regime} regime'

    regime = 'em.IR.NIR'
    t = db.query(db.Spectra).filter(db.Spectra.c.regime == regime).astropy()
    assert len(t) == 118, f'found {len(t)} spectra in the {regime} regime'

    regime = 'em.opt'
    t = db.query(db.Spectra).filter(db.Spectra.c.regime == regime).astropy()
    assert len(t) == 21, f'found {len(t)} spectra in the {regime} regime'

    regime = 'nir'
    t = db.query(db.Spectra).filter(db.Spectra.c.regime == regime).astropy()
    assert len(t) == 456, f'found {len(t)} spectra in the {regime} regime'

    regime = 'mir'
    t = db.query(db.Spectra).filter(db.Spectra.c.regime == regime).astropy()
    assert len(t) == 91, f'found {len(t)} spectra in the {regime} regime'

    telescope = 'IRTF'
    t = db.query(db.Spectra).filter(db.Spectra.c.telescope == telescope).astropy()
    assert len(t) == 436, f'found {len(t)} spectra from {telescope}'

    telescope = 'HST'
    instrument = 'WFC3'
    t = db.query(db.Spectra).filter(
        and_(db.Spectra.c.telescope == telescope, db.Spectra.c.instrument == instrument)).astropy()
    assert len(t) == 77, f'found {len(t)} spectra from {telescope}/{instrument}'

    ref = 'Reid08b'
    t = db.query(db.Spectra).filter(db.Spectra.c.reference == ref).astropy()
    assert len(t) == 280, f'found {len(t)} spectra from {ref}'

    ref = 'Cruz03'
    t = db.query(db.Spectra).filter(db.Spectra.c.reference == ref).astropy()
    assert len(t) == 191, f'found {len(t)} spectra from {ref}'

    ref = 'Cruz18'
    t = db.query(db.Spectra).filter(db.Spectra.c.reference == ref).astropy()
    assert len(t) == 186, f'found {len(t)} spectra from {ref}'

    ref = 'Cruz07'
    t = db.query(db.Spectra).filter(db.Spectra.c.reference == ref).astropy()
    assert len(t) == 158, f'found {len(t)} spectra from {ref}'

    ref = 'Bard14'
    t = db.query(db.Spectra).filter(db.Spectra.c.reference == ref).astropy()
    assert len(t) == 57, f'found {len(t)} spectra from {ref}'

    ref = 'Burg10a'
    t = db.query(db.Spectra).filter(db.Spectra.c.reference == ref).astropy()
    assert len(t) == 46, f'found {len(t)} spectra from {ref}'

    ref = 'Manj20'
    t = db.query(db.Spectra).filter(db.Spectra.c.reference == ref).astropy()
    assert len(t) == 20, f'found {len(t)} spectra from {ref}'


# Individual ingest tests
# -----------------------------------------------------------------------------------------
def test_Manj19_data(db):
    """
    Tests for data ingested from Manjavacas+2019

    Data file(s):
        ATLAS_table.vot
        Manj19_spectra - Sheet1.csv

    Ingest script(s):
        ingest_ATLAS_sources.py
        ingest_ATLAS_spectral_types.py
        Manja_ingest_spectra19.py
    """

    pub = 'Manj19'

    # Check DOI and Bibcode values are correctly set for new publications added
    manj19_pub = db.query(db.Publications).filter(db.Publications.c.publication == pub).astropy()
    reference_verifier(manj19_pub, 'Manj19', '2019AJ....157..101M', '10.3847/1538-3881/aaf88f')

    # Test total spectral types added
    n_Manj19_types = db.query(db.SpectralTypes).filter(db.SpectralTypes.c.reference == pub).count()
    assert n_Manj19_types == 40, f'found {n_Manj19_types} sources for {pub}'

    # Test number of L types added
    n_Manj19_Ltypes = db.query(db.SpectralTypes).filter(and_(db.SpectralTypes.c.spectral_type_code >= 70,
                                                             db.SpectralTypes.c.spectral_type_code < 80,
                                                             db.SpectralTypes.c.reference == pub)).count()
    assert n_Manj19_Ltypes == 19, f'found {n_Manj19_Ltypes} L type dwarfs for {pub}'

    # Test number of T types added
    n_Manj19_Ttypes = db.query(db.SpectralTypes).filter(and_(db.SpectralTypes.c.spectral_type_code >= 80,
                                                             db.SpectralTypes.c.spectral_type_code < 90,
                                                             db.SpectralTypes.c.reference == pub)).count()
    assert n_Manj19_Ttypes == 21, f'found {n_Manj19_Ttypes} T type dwarfs for {pub}'

    # Test spectra added
    n_Manj19_spectra = db.query(db.Spectra).filter(db.Spectra.c.reference == pub).astropy()
    assert len(n_Manj19_spectra) == 77, f'found {len(n_Manj19_spectra)} spectra from {pub}'


def test_Kirk19_ingest(db):
    """
    Tests for Y-dwarf data ingested from Kirkpartick+2019

    Data file(s):
        Y-dwarf_table.csv

    Ingest script(s):
        Y_dwarf_source_ingest.py
        Y-dwarf_SpT_ingest.py
        Y-dwarf_astrometry-ingest.py
        Y_dwarf_pm_ingest.py

    """

    # Reference tests
    # -----------------------------------------------------------------------------------------

    # Test refereces added
    ref_list = ['Tinn18', 'Pinf14', 'Mace13', 'Kirk12', 'Cush11', 'Kirk13',
                'Schn15', 'Luhm14b', 'Tinn14', 'Tinn12', 'Cush14', 'Kirk19']
    t = db.query(db.Publications).filter(db.Publications.c.publication.in_(ref_list)).astropy()

    if len(ref_list) != len(t):
        missing_ref = list(set(ref_list) - set(t['name']))
        assert len(ref_list) == len(t), f'Missing references: {missing_ref}'

    # Check DOI and Bibcode values are correctly set for new references added
    reference_verifier(t, 'Kirk19', '2019ApJS..240...19K', '10.3847/1538-4365/aaf6af')
    reference_verifier(t, 'Pinf14', '2014MNRAS.444.1931P', '10.1093/mnras/stu1540')
    reference_verifier(t, 'Tinn18', '2018ApJS..236...28T', '10.3847/1538-4365/aabad3')

    # Data tests
    # -----------------------------------------------------------------------------------------

    # Test sources added
    ref = 'Pinf14'
    t = db.query(db.Sources).filter(db.Sources.c.reference == ref).astropy()
    assert len(t) == 1, f'found {len(t)} sources for {ref}'

    # Test spectral types added

    # Test parallaxes 
    ref = 'Kirk19'
    t = db.query(db.Parallaxes).filter(db.Parallaxes.c.reference == ref).astropy()
    assert len(t) == 23, f'found {len(t)} parallax entries for {ref}'

    # Test proper motions added
    ref = 'Kirk19'
    t = db.query(db.ProperMotions).filter(db.ProperMotions.c.reference == ref).astropy()
    assert len(t) == 182, f'found {len(t)} proper motion entries for {ref}'

    # Test photometry added
    telescope = 'Spitzer'
    t = db.query(db.Photometry).filter(db.Photometry.c.telescope == telescope).astropy()
    assert len(t) == 46, f'found {len(t)} photometry entries for {telescope}'

    ref = 'Kirk19'
    t = db.query(db.Photometry).filter(db.Photometry.c.reference == ref).astropy()
    assert len(t) == 18, f'found {len(t)} photometry entries for {ref}'

    ref = 'Schn15'
    t = db.query(db.Photometry).filter(db.Photometry.c.reference == ref).astropy()
    assert len(t) == 28, f'found {len(t)} photometry entries for {ref}'

    # Test parallaxes added for ATLAS
    ref = 'Mart18'
    t = db.query(db.Parallaxes).filter(db.Parallaxes.c.reference == ref).astropy()
    assert len(t) == 15, f'found {len(t)} parallax entries for {ref}'


def test_Best2020_ingest(db):
    # Test for Best20a proper motions added
    ref = 'Best20a'
    t = db.query(db.ProperMotions).filter(db.ProperMotions.c.reference == ref).astropy()
    assert len(t) == 348, f'found {len(t)} proper motion entries for {ref}'

    # Test for Best2020a parallaxes added
    ref = 'Best20a'
    t = db.query(db.Parallaxes).filter(db.Parallaxes.c.reference == ref).astropy()
    assert len(t) == 348, f'found {len(t)} parallax entries for {ref}'
    # Test for number of Best20a parallaxes that are adopted
    t = db.query(db.Parallaxes).filter(and_(db.Parallaxes.c.reference == ref,
                                            db.Parallaxes.c.adopted == 1)).astropy()
    assert len(t) == 255, f'found {len(t)} adopted parallax entries for {ref}'
