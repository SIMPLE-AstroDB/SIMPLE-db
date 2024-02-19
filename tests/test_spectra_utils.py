import pytest
import os
import logging
from astrodbkit2.astrodb import create_database, Database
from astrodb_scripts.utils import (
    AstroDBError,
)
from scripts.utils.ingest_spectra_utils import (
    ingest_spectrum,
    ingest_spectrum_from_fits,
    spectrum_plottable,
)
from schema.schema import *


logger = logging.getLogger("SIMPLE")
logger.setLevel(logging.DEBUG)


DB_NAME = "simple_test_spectra.sqlite"
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
        {"source": "apple", "ra": 9.0673755, "dec": 18.352889, "reference": "Ref 1"},
        {"source": "orange", "ra": 90.0673755, "dec": 19.352889, "reference": "Ref 2"},
        {
            "source": "banana",
            "ra": 360.0673755,
            "dec": -18.352889,
            "reference": "Burn08",
        },
    ]

    with db.engine.connect() as conn:
        conn.execute(db.Publications.insert().values(ref_data))
        conn.execute(db.Sources.insert().values(source_data))
        conn.commit()

    return db


def test_ingest_spectrum(db):
    spectrum = "https://bdnyc.s3.amazonaws.com/tests/U10176.fits"
    with pytest.raises(AstroDBError) as error_message:
        ingest_spectrum(db, source="apple", spectrum=spectrum)
    assert "Regime is required" in str(error_message.value)
    result = ingest_spectrum(db, source="apple", spectrum=spectrum, raise_error=False)
    assert result["added"] is False
    assert result["skipped"] is True

    with pytest.raises(AstroDBError) as error_message:
        ingest_spectrum(db, source="apple", regime="nir", spectrum=spectrum)
    assert "Reference is required" in str(error_message.value)
    ingest_spectrum(
        db, source="apple", regime="nir", spectrum=spectrum, raise_error=False
    )
    assert result["added"] is False
    assert result["skipped"] is True

    with pytest.raises(AstroDBError) as error_message:
        ingest_spectrum(
            db, source="apple", regime="nir", spectrum=spectrum, reference="Ref 5"
        )
    assert "not in Publications table" in str(error_message.value)
    ingest_spectrum(
        db,
        source="apple",
        regime="nir",
        spectrum=spectrum,
        reference="Ref 5",
        raise_error=False,
    )
    assert result["added"] is False
    assert result["skipped"] is True

    with pytest.raises(AstroDBError) as error_message:
        ingest_spectrum(
            db, source="kiwi", regime="nir", spectrum=spectrum, reference="Ref 1"
        )
    assert "No unique source match for kiwi in the database" in str(error_message.value)
    result = ingest_spectrum(
        db,
        source="kiwi",
        regime="nir",
        spectrum=spectrum,
        reference="Ref 1",
        raise_error=False,
    )
    assert result["added"] is False
    assert result["skipped"] is True

    with pytest.raises(AstroDBError) as error_message:
        ingest_spectrum(
            db, source="apple", regime="nir", spectrum=spectrum, reference="Ref 1"
        )
    assert "missing observation date" in str(error_message.value)
    result = ingest_spectrum(
        db,
        source="apple",
        regime="nir",
        spectrum=spectrum,
        reference="Ref 1",
        raise_error=False,
    )
    assert result["added"] is False
    assert result["skipped"] is False
    assert result["no_obs_date"] is True

    # with pytest.raises(AstroDBError) as error_message:
    #     ingest_spectrum(
    #         db,
    #         source="orange",
    #         regime="nir",
    #         spectrum=spectrum,
    #         reference="Ref 1",
    #         obs_date="Jan20",
    #     )
    # assert "Can't convert obs date to Date Time object" in str(error_message.value)
    # result = ingest_spectrum(
    #     db,
    #     source="orange",
    #     regime="nir",
    #     spectrum=spectrum,
    #     reference="Ref 1",
    #     obs_date="Jan20",
    #     raise_error=False,
    # )
    # assert result["added"] is False
    # assert result["skipped"] is False
    # assert result["no_obs_date"] is True

    with pytest.raises(AstroDBError) as error_message:
        result = ingest_spectrum(
            db,
            source="orange",
            regime="far-uv",
            spectrum=spectrum,
            reference="Ref 1",
            obs_date="1/1/2024",
        )
    assert "Regime provided is not in schema" in str(error_message.value)
    result = ingest_spectrum(
        db,
        source="orange",
        regime="far-uv",
        spectrum=spectrum,
        reference="Ref 1",
        obs_date="1/1/2024",
        raise_error=False,
    )
    assert result["added"] is False
    assert result["skipped"] is True


def test_ingest_spectrum_works(db):
    spectrum = "https://bdnyc.s3.amazonaws.com/tests/U10176.fits"
    result = ingest_spectrum(
        db,
        source="banana",
        regime="nir",
        spectrum=spectrum,
        reference="Ref 1",
        obs_date="2020-01-01",
    )
    assert result["added"] is True


@pytest.mark.parametrize(
    "file",
    [
        "https://s3.amazonaws.com/bdnyc/optical_spectra/2MASS1538-1953_tell.fits",
        "https://s3.amazonaws.com/bdnyc/spex_prism_lhs3003_080729.txt",
        "https://bdnyc.s3.amazonaws.com/IRS/2351-2537_IRS_spectrum.dat",
    ],
)
def test_spectrum_plottable_false(file):
    with pytest.raises(AstroDBError) as error_message:
        spectrum_plottable(file)
        assert "unable to load file as Spectrum1D object" in str(error_message.value)

    result = spectrum_plottable(file, raise_error=False)
    assert result is False


@pytest.mark.parametrize(
    "file",
    [
        "https://bdnyc.s3.amazonaws.com/SpeX/Prism/2MASS+J04510093-3402150_2012-09-27.fits",
        "https://bdnyc.s3.amazonaws.com/IRS/2MASS+J23515044-2537367.fits",
        "https://bdnyc.s3.amazonaws.com/optical_spectra/vhs1256b_opt_Osiris.fits",
    ],
)
def test_spectrum_plottable_true(file):
    result = spectrum_plottable(file)
    assert result is True
