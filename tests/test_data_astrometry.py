from sqlalchemy import except_, select, and_


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
