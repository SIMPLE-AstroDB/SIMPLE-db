import pytest
from sqlalchemy import and_


def test_parallax_refs(db):
    # Test total adopted measuruments
    t = db.query(db.Parallaxes).filter(db.Parallaxes.c.adopted == 1).astropy()
    assert len(t) == 1876, f"found {len(t)} adopted parallax measuruments."

    ref = "GaiaDR3"
    t = db.query(db.Parallaxes).filter(db.Parallaxes.c.reference == ref).astropy()
    assert len(t) == 1, f"found {len(t)} parallax reference entries for {ref}"

    ref = "GaiaDR2"
    t = db.query(db.Parallaxes).filter(db.Parallaxes.c.reference == ref).astropy()
    assert len(t) == 1076, f"found {len(t)} parallax reference entries for {ref}"

    t = (
        db.query(db.Parallaxes)
        .filter(and_(db.Parallaxes.c.reference == ref, db.Parallaxes.c.adopted == 1))
        .astropy()
    )
    assert len(t) == 33, f"found {len(t)} adopted parallax reference entries for {ref}"

    ref = "GaiaEDR3"
    t = db.query(db.Parallaxes).filter(db.Parallaxes.c.reference == ref).astropy()
    assert len(t) == 1134, f"found {len(t)} parallax reference entries for {ref}"

    t = (
        db.query(db.Parallaxes)
        .filter(and_(db.Parallaxes.c.reference == ref, db.Parallaxes.c.adopted == 1))
        .astropy()
    )
    assert (
        len(t) == 1077
    ), f"found {len(t)} adopted parallax reference entries for {ref}"

    ref = "Kirk21"
    t = db.query(db.Parallaxes).filter(db.Parallaxes.c.reference == ref).astropy()
    assert len(t) == 325, f"found {len(t)} parallax reference entries for {ref}"

    t = (
        db.query(db.Parallaxes)
        .filter(and_(db.Parallaxes.c.reference == ref, db.Parallaxes.c.adopted == 1))
        .astropy()
    )
    assert len(t) == 248, f"found {len(t)} adopted parallax reference entries for {ref}"

    ref = "Kirk19"
    t = db.query(db.Parallaxes).filter(db.Parallaxes.c.reference == ref).astropy()
    assert len(t) == 64, f"found {len(t)} parallax entries for {ref}"

    ref = "Mart18"
    t = db.query(db.Parallaxes).filter(db.Parallaxes.c.reference == ref).astropy()
    assert len(t) == 15, f"found {len(t)} parallax entries for {ref}"

def test_propermotion_GaiaEDR3_Best18_Best20_Kirk19(db):
    """
    Test for proper motions added and adoptions from GaiaEDR3, Kirk19, Best18, and Best20.
    """
    ref = "GaiaEDR3"
    t = db.query(db.ProperMotions).filter(db.ProperMotions.c.reference == ref).astropy()
    assert len(t) == 1133, f"found {len(t)} proper motion entries for {ref}"

    t = (
        db.query(db.ProperMotions)
        .filter(and_(db.ProperMotions.c.reference == ref, db.ProperMotions.c.adopted == 1))
        .astropy()
    )
    assert len(t) == 1057, f"found {len(t)} adopted proper motion for {ref}"

    # Test for Best18 proper motions added from Pan-STARRS catalog
    ref = "Best18"
    t = db.query(db.ProperMotions).filter(db.ProperMotions.c.reference == ref).astropy()
    assert len(t) == 1966, f"found {len(t)} proper motion entries for {ref}"

    t = (
        db.query(db.ProperMotions)
        .filter(and_(db.ProperMotions.c.reference == ref, db.ProperMotions.c.adopted == 1))
        .astropy()
    )
    assert len(t) == 734, f"found {len(t)} adopted proper motion for {ref}"

    # Test for Best20.257 proper motions added and adoptions
    ref = "Best20.257"
    t = db.query(db.ProperMotions).filter(db.ProperMotions.c.reference == ref).astropy()
    assert len(t) == 348, f"found {len(t)} proper motion entries for {ref}"

    t = (
        db.query(db.ProperMotions)
        .filter(and_(db.ProperMotions.c.reference == ref, db.ProperMotions.c.adopted == 1))
        .astropy()
    )
    assert len(t) == 145, f"found {len(t)} adopted proper motion for {ref}"

    ref = "Kirk19"
    t = db.query(db.ProperMotions).filter(db.ProperMotions.c.reference == ref).astropy()
    assert len(t) == 182, f"found {len(t)} proper motion entries for {ref}"

    t = (
        db.query(db.ProperMotions)
        .filter(and_(db.ProperMotions.c.reference == ref, db.ProperMotions.c.adopted == 1))
        .astropy()
    )
    assert len(t) == 142, f"found {len(t)} adopted proper motion for {ref}"
    
    
@pytest.mark.parametrize(
    ("ref", "n_proper_motions"),
    [
        ("GaiaEDR3", 1133),
        ("GaiaDR2", 1076),
        ("Best20.257", 348),
        ("Gagn15.73", 325),
        ("Fahe09", 216),
        ("Best15", 120),
        ("Burn13", 97),
        ("Dahn17", 79),
        ("Jame08", 73),
        ("vanL07", 68),
        ("Smar18", 68),
        ("Schm10.1808", 44),
    ],
)
def test_proper_motion_refs(db, ref, n_proper_motions):
    """
    Values found with this SQL query:
        SELECT reference, count(*)
        FROM ProperMotions
        GROUP BY reference
        ORDER By 2 DESC

    from sqlalchemy import func
    proper_motion_mearsurements = db.query(ProperMotions.reference, func.count(
        ProperMotions.reference)).\
        group_by(ProperMotions.reference).order_by(
            func.count(ProperMotions.reference).desc()).limit(20).all()
    """
    t = db.query(db.ProperMotions).filter(db.ProperMotions.c.reference == ref).astropy()
    assert (
        len(t) == n_proper_motions
    ), f"found {len(t)} proper motion reference entries for {ref}"


def test_radial_velocities(db):
    t = db.query(db.RadialVelocities).astropy()
    assert len(t) == 1015, f"found {len(t)} radial velociies"

    ref = "Abaz09"
    t = (
        db.query(db.RadialVelocities)
        .filter(db.RadialVelocities.c.reference == ref)
        .astropy()
    )
    assert len(t) == 445, f"found {len(t)} radial velociies with {ref} reference"

    ref = "Fahe16"
    t = (
        db.query(db.RadialVelocities)
        .filter(db.RadialVelocities.c.reference == ref)
        .astropy()
    )
    assert len(t) == 47, f"found {len(t)} radial velociies with {ref} reference"

    t = (
        db.query(db.RadialVelocities)
        .filter(db.RadialVelocities.c.radial_velocity_error_km_s == None)  # noqa: E711
        .astropy()
    )
    assert len(t) == 89, f"found {len(t)} radial velociies with no uncertainty"
