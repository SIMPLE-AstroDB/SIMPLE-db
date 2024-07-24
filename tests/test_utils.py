import pytest
from astropy.table import Table
from astrodb_utils.utils import (
    AstroDBError,
)
from simple.utils.companions import ingest_companion_relationships


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


def test_companion_relationships(temp_db):
    # testing companion ingest
    # trying no companion
    with pytest.raises(AstroDBError) as error_message:
        ingest_companion_relationships(temp_db, "Fake 1", None, "Sibling")
    assert "Make sure all required parameters are provided." in str(error_message.value)

    # trying companion == source
    with pytest.raises(AstroDBError) as error_message:
        ingest_companion_relationships(temp_db, "Fake 1", "Fake 1", "Sibling")
    assert "Source cannot be the same as companion name" in str(error_message.value)

    # trying negative separation
    with pytest.raises(AstroDBError) as error_message:
        ingest_companion_relationships(
            temp_db,
            "Fake 1",
            "Bad Companion",
            "Sibling",
            projected_separation_arcsec=-5,
        )
    assert "cannot be negative" in str(error_message.value)

    # trying negative separation error
    with pytest.raises(AstroDBError) as error_message:
        ingest_companion_relationships(
            temp_db, "Fake 1", "Bad Companion", "Sibling", projected_separation_error=-5
        )
    assert "cannot be negative" in str(error_message.value)
