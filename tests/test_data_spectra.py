import pytest
from sqlalchemy import and_


def test_spectra_count(db):
    n_spectra = db.query(db.Spectra).count()
    assert n_spectra == 1560, f"found {n_spectra} sources"


@pytest.mark.parametrize(
    ("regime", "n_spectra"),
    [
        ("optical", 743),
        ("nir", 613),
        ("mir", 204),
        ("unknown", 0),
    ],
)
def test_spectra_regimes(db, regime, n_spectra):
    t = db.query(db.Spectra).filter(db.Spectra.c.regime == regime).astropy()
    assert len(t) == n_spectra, f"found {len(t)} spectra in the {regime} regime"


@pytest.mark.parametrize(
    ("telescope", "n_spectra"),
    [
        ("IRTF", 457),
        ("Keck I", 65),
        ("Keck II", 8),
        ("Magellan I Baade", 9),
        ("Magellan II Clay", 11),
        ("SOAR", 2),
        ("Lick Shane 3m", 1),
        ("HST", 77),
        ("Gemini North", 27),
        ("Gemini South", 34),
        ("ESO VLT", 62),
        ("Spitzer", 203),
        ("KPNO 2.1m", 93),
        ("KPNO 4m", 251),
    ],
)
def test_spectra_telescope(db, telescope, n_spectra):
    t = db.query(db.Spectra).filter(db.Spectra.c.telescope == telescope).astropy()
    assert len(t) == n_spectra, f"found {len(t)} spectra from {telescope}"


@pytest.mark.parametrize(
    ("telescope", "instrument", "n_spectra"),
    [
        ("JWST", "NIRSpec", 2),
        ("HST", "WFC3", 77),
    ],
)
def test_spectra_instrument(db, telescope, instrument, n_spectra):
    t = (
        db.query(db.Spectra)
        .filter(
            and_(
                db.Spectra.c.telescope == telescope,
                db.Spectra.c.instrument == instrument,
            )
        )
        .astropy()
    )
    assert len(t) == n_spectra, f"found {len(t)} spectra from {telescope}/{instrument}"


@pytest.mark.parametrize(
    ("ref", "n_spectra"),
    [
        ("Reid08.1290", 280),
        ("Cruz03", 191),
        ("Cruz18", 186),
        ("Cruz07", 158),
        ("Bard14", 57),
        ("Burg10.1142", 46),
        ("Manj20", 20),
        ("Roth24", 34),
    ],
)
def test_spectra_references(db, ref, n_spectra):
    t = db.query(db.Spectra).filter(db.Spectra.c.reference == ref).astropy()
    assert len(t) == n_spectra, f"found {len(t)} spectra from {ref}"
