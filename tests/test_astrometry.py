import pytest
from astropy.table import Table
from astrodb_utils import AstroDBError
from simple.utils.astrometry import (
    ingest_parallax,
    ingest_proper_motions,
    ingest_radial_velocity,
)


# Create fake astropy Table of data to load
@pytest.fixture(scope="module")
def t_plx():
    t_plx = Table(
        [
            {"source": "Fake 1", "plx": 113.0, "plx_err": 0.3, "plx_ref": "Ref 1"},
            {"source": "Fake 2", "plx": 145.0, "plx_err": 0.5, "plx_ref": "Ref 1"},
            {"source": "Fake 1", "plx": 113.0, "plx_err": 0.2, "plx_ref": "Ref 2"},
            {"source": "Fake 3", "plx": 155.0, "plx_err": 0.6, "plx_ref": "Ref 2"},
        ]
    )
    return t_plx


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


@pytest.fixture(scope="module")
def t_rv():
    t_rv = Table(
        [
            {"source": "Fake 1", "rv": 113.0, "rv_err": 0.3, "rv_ref": "Ref 1"},
            {"source": "Fake 2", "rv": 145.0, "rv_err": 0.5, "rv_ref": "Ref 1"},
            {"source": "Fake 3", "rv": "155.0", "rv_err": "0.6", "rv_ref": "Ref 2"},
        ]
    )
    return t_rv


def test_ingest_parallaxes(temp_db, t_plx):
    # Test ingest of parallax data

    for row in t_plx:
        ingest_parallax(
            temp_db,
            row["source"],
            row["plx"],
            row["plx_err"],
            row["plx_ref"],
        )

    results = (
        temp_db.query(temp_db.Parallaxes)
        .filter(temp_db.Parallaxes.c.reference == "Ref 1")
        .table()
    )
    assert len(results) == 2
    assert not results["adopted"][0]  # 1st source with ref 1 should not be adopted
    results = (
        temp_db.query(temp_db.Parallaxes)
        .filter(temp_db.Parallaxes.c.reference == "Ref 2")
        .table()
    )
    assert len(results) == 2
    assert results["source"][1] == "Fake 3"
    assert results["parallax"][1] == 155
    assert results["parallax_error"][1] == 0.6
    assert results["adopted"][0]  # 1st source with ref 2 should be adopted


def test_parallax_exceptions(temp_db):
    with pytest.raises(AstroDBError) as error_message:
        ingest_parallax(temp_db, "bad source", 1, 1, "Ref 1")
    assert "The source may not exist in Sources table" in str(error_message.value)

    with pytest.raises(AstroDBError) as error_message:
        ingest_parallax(temp_db, "Fake 1", 1, 1, "bad ref")
    assert "The parallax reference may not exist in Publications table" in str(
        error_message.value
    )

    ingest_parallax(temp_db, "Fake 2", 1, 1, "Ref 2")
    with pytest.raises(AstroDBError) as error_message:
        ingest_parallax(temp_db, "Fake 2", 1, 1, "Ref 2")
    assert "Duplicate measurement exists with same reference" in str(
        error_message.value
    )


def test_ingest_proper_motions(temp_db, t_pm):
    ingest_proper_motions(
        temp_db,
        t_pm["source"],
        t_pm["mu_ra"],
        t_pm["mu_ra_err"],
        t_pm["mu_dec"],
        t_pm["mu_dec_err"],
        t_pm["reference"],
    )
    assert (
        temp_db.query(temp_db.ProperMotions)
        .filter(temp_db.ProperMotions.c.reference == "Ref 1")
        .count()
        == 2
    )
    results = (
        temp_db.query(temp_db.ProperMotions)
        .filter(temp_db.ProperMotions.c.reference == "Ref 2")
        .table()
    )
    assert len(results) == 1
    assert results["source"][0] == "Fake 3"
    assert results["mu_ra"][0] == 55
    assert results["mu_ra_error"][0] == 0.23


def test_ingest_radial_velocities_works(temp_db, t_rv):
    for ind in range(3):
        ingest_radial_velocity(
            temp_db,
            source=t_rv["source"][ind],
            rv=t_rv["rv"][ind],
            rv_err=t_rv["rv_err"][ind],
            reference=t_rv["rv_ref"][ind],
        )

    results = (
        temp_db.query(temp_db.RadialVelocities)
        .filter(temp_db.RadialVelocities.c.reference == "Ref 1")
        .table()
    )
    assert len(results) == 2
    results = (
        temp_db.query(temp_db.RadialVelocities)
        .filter(temp_db.RadialVelocities.c.reference == "Ref 2")
        .table()
    )
    assert len(results) == 1
    assert results["source"][0] == "Fake 3"
    assert results["radial_velocity_km_s"][0] == 155
    assert results["radial_velocity_error_km_s"][0] == 0.6


@pytest.mark.filterwarnings("ignore::UserWarning")
def test_ingest_radial_velocities_errors(temp_db):
    with pytest.raises(AstroDBError) as error_message:
        ingest_radial_velocity(
            temp_db, source="not a source", rv=12.5, rv_err=0.5, reference="Ref 1"
        )
    assert "No unique source match" in str(error_message.value)
    # flag['skipped']  = True, flag['added']  = False

    with pytest.raises(AstroDBError) as error_message:
        ingest_radial_velocity(
            temp_db, source="Fake 1", rv=12.5, rv_err=0.5, reference="Ref 1"
        )
    assert "Duplicate radial velocity measurement" in str(error_message.value)
    # flag['skipped']  = True,  flag['added']  = False

    with pytest.raises(AstroDBError) as error_message:
        ingest_radial_velocity(
            temp_db, source="Fake 1", rv=12.5, rv_err=0.5, reference="not a ref"
        )
    assert "not found in Publications table" in str(error_message.value)
    # flag['skipped']  = True,  flag['added']  = False
