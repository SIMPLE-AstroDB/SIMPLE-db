import pytest
from astropy.table import Table
from simple.utils.astrometry import (
    ingest_parallaxes,
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
            {"source": "Fake 3", "rv": 155.0, "rv_err": 0.6, "rv_ref": "Ref 2"},
        ]
    )
    return t_rv


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


def test_ingest_radial_velocities_works(db, t_rv):
    for ind in range(3):
        ingest_radial_velocity(
            db,
            t_rv["source"][ind],
            t_rv["rv"][ind],
            t_rv["rv_err"][ind],
            t_rv["rv_ref"][ind],
        )

    results = (
        db.query(db.RadialVelocities)
        .filter(db.RadialVelocities.c.reference == "Ref 1")
        .table()
    )
    assert len(results) == 2
    results = (
        db.query(db.RadialVelocities)
        .filter(db.RadialVelocities.c.reference == "Ref 2")
        .table()
    )
    assert len(results) == 1
    assert results["source"][0] == "Fake 3"
    assert results["radial_velocity"][0] == 155
    assert results["radial_velocity_error"][0] == 0.6
