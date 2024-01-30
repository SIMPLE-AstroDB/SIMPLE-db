import pytest
import os
from astrodbkit2.astrodb import create_database, Database
from astropy.table import Table
from sqlalchemy import and_
import logging
from astrodb_scripts import (
    AstroDBError,
)
from scripts.ingests.simple_utils import (
    convert_spt_string_to_code,
    ingest_companion_relationships,
    ingest_parallaxes,
    ingest_spectral_types,
    ingest_proper_motions,
)
from schema.schema import *


logger = logging.getLogger("SIMPLE")
logger.setLevel(logging.DEBUG)


DB_NAME = "simple_temp.sqlite"
DB_PATH = "data"


# Load the database for use in individual tests
@pytest.fixture(scope="module")
def db():
    # Create a fresh temporary database and assert it exists
    # Because we've imported simple.schema, we will be using that schema for the database

    if os.path.exists(DB_NAME):
        os.remove(DB_NAME)
    connection_string = "sqlite:///" + DB_NAME
    create_database(connection_string)
    assert os.path.exists(DB_NAME)

    # Connect to the new database and confirm it has the Sources table
    db = Database(connection_string)
    assert db
    assert "source" in [c.name for c in db.Sources.columns]

    return db


# Create fake astropy Table of data to load
@pytest.fixture(scope="module")
def t_plx():
    t_plx = Table(
        [
            {"source": "Fake 1", "plx": 113.0, "plx_err": 0.3, "plx_ref": "Ref 1"},
            {"source": "Fake 2", "plx": 145.0, "plx_err": 0.5, "plx_ref": "Ref 1"},
            {"source": "Fake 3", "plx": 155.0, "plx_err": 0.6, "plx_ref": "Ref 2"},
        ]
    )
    return t_plx


# Create fake astropy Table of data to load
@pytest.fixture(scope="module")
def t_pm():
    t_pm = Table(
        [
            {
                "source": "Fake 1",
                "mu_ra": 113.0,
                "mu_ra_err": 0.3,
                "mu_dec": 113.0,
                "mu_dec_err": 0.3,
                "reference": "Ref 1",
            },
            {
                "source": "Fake 2",
                "mu_ra": 145.0,
                "mu_ra_err": 0.5,
                "mu_dec": 113.0,
                "mu_dec_err": 0.3,
                "reference": "Ref 1",
            },
            {
                "source": "Fake 3",
                "mu_ra": 55.0,
                "mu_ra_err": 0.23,
                "mu_dec": 113.0,
                "mu_dec_err": 0.3,
                "reference": "Ref 2",
            },
        ]
    )
    return t_pm


def test_setup_db(db):
    # Some setup tasks to ensure some data exists in the database first
    ref_data = [
        {
            "reference": "Ref 1",
            "doi": "10.1093/mnras/staa1522",
            "bibcode": "2020MNRAS.496.1922B",
        },
        {"reference": "Ref 2", "doi": "Doi2", "bibcode": "2012yCat.2311....0C"},
        {"reference": "Burn08", "doi": "Doi3", "bibcode": "2008MNRAS.391..320B"},
    ]

    source_data = [
        {"source": "Fake 1", "ra": 9.0673755, "dec": 18.352889, "reference": "Ref 1"},
        {"source": "Fake 2", "ra": 9.0673755, "dec": 18.352889, "reference": "Ref 1"},
        {"source": "Fake 3", "ra": 9.0673755, "dec": 18.352889, "reference": "Ref 2"},
    ]

    with db.engine.connect() as conn:
        conn.execute(db.Publications.insert().values(ref_data))
        conn.execute(db.Sources.insert().values(source_data))
        conn.commit()

    return db


def test_convert_spt_string_to_code():
    # Test conversion of spectral types into numeric values
    assert convert_spt_string_to_code(["M5.6"]) == [65.6]
    assert convert_spt_string_to_code(["T0.1"]) == [80.1]
    assert convert_spt_string_to_code(["Y2pec"]) == [92]


def test_ingest_parallaxes(db, t_plx):
    # Test ingest of parallax data
    ingest_parallaxes(
        db, t_plx["source"], t_plx["plx"], t_plx["plx_err"], t_plx["plx_ref"]
    )

    results = (
        db.query(db.Parallaxes).filter(db.Parallaxes.c.reference == "Ref 1").table()
    )
    assert len(results) == 2
    results = (
        db.query(db.Parallaxes).filter(db.Parallaxes.c.reference == "Ref 2").table()
    )
    assert len(results) == 1
    assert results["source"][0] == "Fake 3"
    assert results["parallax"][0] == 155
    assert results["parallax_error"][0] == 0.6


def test_ingest_proper_motions(db, t_pm):
    ingest_proper_motions(
        db,
        t_pm["source"],
        t_pm["mu_ra"],
        t_pm["mu_ra_err"],
        t_pm["mu_dec"],
        t_pm["mu_dec_err"],
        t_pm["reference"],
    )
    assert (
        db.query(db.ProperMotions)
        .filter(db.ProperMotions.c.reference == "Ref 1")
        .count()
        == 2
    )
    results = (
        db.query(db.ProperMotions)
        .filter(db.ProperMotions.c.reference == "Ref 2")
        .table()
    )
    assert len(results) == 1
    assert results["source"][0] == "Fake 3"
    assert results["mu_ra"][0] == 55
    assert results["mu_ra_error"][0] == 0.23


def test_ingest_spectral_types(db):
    data1 = Table(
        [
            {
                "source": "Fake 1",
                "spectral_type": "M5.6",
                "regime": "nir",
                "reference": "Ref 1",
            },
            {
                "source": "Fake 2",
                "spectral_type": "T0.1",
                "regime": "nir",
                "reference": "Ref 1",
            },
            {
                "source": "Fake 3",
                "spectral_type": "Y2pec",
                "regime": "nir",
                "reference": "Ref 2",
            },
        ]
    )

    # data2 = Table(
    #     [
    #         {"source": "Fake 1", "spectral_type": "M5.6", "reference": "Ref 1"},
    #         {"source": "Fake 2", "spectral_type": "T0.1", "reference": "Ref 1"},
    #         {"source": "Fake 3", "spectral_type": "Y2pec", "reference": "Ref 2"},
    #     ]
    # )

    data3 = Table(
        [
            {
                "source": "Fake 1",
                "spectral_type": "M5.6",
                "regime": "nir",
                "reference": "Ref 1",
            },
            {
                "source": "Fake 2",
                "spectral_type": "T0.1",
                "regime": "nir",
                "reference": "Ref 1",
            },
            {
                "source": "Fake 3",
                "spectral_type": "Y2pec",
                "regime": "nir",
                "reference": "Ref 4",
            },
        ]
    )
    ingest_spectral_types(
        db, data1["source"], data1["spectral_type"], data1["reference"], data1["regime"]
    )
    assert (
        db.query(db.SpectralTypes)
        .filter(db.SpectralTypes.c.reference == "Ref 1")
        .count()
        == 2
    )
    results = (
        db.query(db.SpectralTypes)
        .filter(db.SpectralTypes.c.reference == "Ref 2")
        .table()
    )
    assert len(results) == 1
    assert results["source"][0] == "Fake 3"
    assert results["spectral_type_string"][0] == "Y2pec"
    assert results["spectral_type_code"][0] == [92]
    # testing for publication error
    with pytest.raises(AstroDBError) as error_message:
        ingest_spectral_types(
            db,
            data3["source"],
            data3["spectral_type"],
            data3["regime"],
            data3["reference"],
        )
        assert "The publication does not exist in the database" in str(
            error_message.value
        )


# TODO: test for ingest_photometry

# TODO: test for ingest_spectra


def test_companion_relationships(db):
    # testing companion ingest
    # trying no companion
    with pytest.raises(AstroDBError) as error_message:
        ingest_companion_relationships(db, "Fake 1", None, "Sibling")
    assert "Make sure all require parameters are provided." in str(error_message.value)

    # trying companion == source
    with pytest.raises(AstroDBError) as error_message:
        ingest_companion_relationships(db, "Fake 1", "Fake 1", "Sibling")
    assert "Source cannot be the same as companion name" in str(error_message.value)

    # trying negative separation
    with pytest.raises(AstroDBError) as error_message:
        ingest_companion_relationships(
            db, "Fake 1", "Bad Companion", "Sibling", projected_separation_arcsec=-5
        )
    assert "cannot be negative" in str(error_message.value)

    # trying negative separation error
    with pytest.raises(AstroDBError) as error_message:
        ingest_companion_relationships(
            db, "Fake 1", "Bad Companion", "Sibling", projected_separation_error=-5
        )
    assert "cannot be negative" in str(error_message.value)
