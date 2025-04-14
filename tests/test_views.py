# Testing view creation in the database
import sqlalchemy as sa
from astrodbkit.views import view


def test_parallax_view(db):
    # Tests to verify views exist and work as intended

    # Explicitly define the view
    ParallaxView = view(
        "ParallaxView",
        db.metadata,
        sa.select(
            db.Parallaxes.c.source.label("source"),
            db.Parallaxes.c.parallax.label("parallax"),
            db.Parallaxes.c.parallax_error.label("parallax_error"),
            (1000.0 / db.Parallaxes.c.parallax).label("distance"),  # distance in parsecs
            db.Parallaxes.c.comments.label("comments"),
            db.Parallaxes.c.reference.label("reference"),
        )
        .select_from(db.Parallaxes)
        .where(sa.and_(db.Parallaxes.c.adopted == 1, db.Parallaxes.c.parallax > 0)),
    )

    # Create the view in the database
    with db.engine.begin() as conn:
        db.metadata.create_all(conn)

    # Views do not exist as attributes to db so db.ViewName does not work
    # TODO: Figure out other ways to refer to it in db.metadata info
    t = db.query(ParallaxView).table()
    print(t)
    assert len(t) > 0

    # Check view is not part of inventory
    assert "ParallaxView" not in db.inventory("2MASSI J0019457+521317").keys()


def test_photometry_view(db):
    # Test creating the photometry view

    # Explicitly define the view
    PhotometryView = view(
        "PhotometryView",
        db.metadata,
        sa.select(
            db.Photometry.c.source.label("source"),
            sa.func.avg(sa.case((db.Photometry.c.band == "2MASS.J", db.Photometry.c.magnitude))).label("2MASS.J"),
            sa.func.avg(sa.case((db.Photometry.c.band == "2MASS.H", db.Photometry.c.magnitude))).label("2MASS.H"),
            sa.func.avg(sa.case((db.Photometry.c.band == "2MASS.Ks", db.Photometry.c.magnitude))).label("2MASS.Ks"),
            sa.func.avg(sa.case((db.Photometry.c.band == "WISE.W1", db.Photometry.c.magnitude))).label("WISE.W1"),
            sa.func.avg(sa.case((db.Photometry.c.band == "WISE.W2", db.Photometry.c.magnitude))).label("WISE.W2"),
            sa.func.avg(sa.case((db.Photometry.c.band == "WISE.W3", db.Photometry.c.magnitude))).label("WISE.W3"),
            sa.func.avg(sa.case((db.Photometry.c.band == "WISE.W4", db.Photometry.c.magnitude))).label("WISE.W4"),
            sa.func.avg(sa.case((db.Photometry.c.band == "IRAC.I1", db.Photometry.c.magnitude))).label("IRAC.I1"),
            sa.func.avg(sa.case((db.Photometry.c.band == "IRAC.I2", db.Photometry.c.magnitude))).label("IRAC.I2"),
            sa.func.avg(sa.case((db.Photometry.c.band == "IRAC.I3", db.Photometry.c.magnitude))).label("IRAC.I3"),
            sa.func.avg(sa.case((db.Photometry.c.band == "IRAC.I4", db.Photometry.c.magnitude))).label("IRAC.I4"),
        )
        .select_from(db.Photometry)
        .group_by(db.Photometry.c.source),
    )

    # Create the view in the database
    with db.engine.begin() as conn:
        db.metadata.create_all(conn)

    # Views do not exist as attributes to db so db.ViewName does not work
    t = db.query(PhotometryView).table()
    print(t)
    assert len(t) > 0

    # Check view is not part of inventory
    assert "PhotometryView" not in db.inventory("2MASSI J0019457+521317").keys()
