import pytest
from sqlalchemy import and_


def test_spectra_count(db):
    n_spectra = db.query(db.Spectra).count()
    assert n_spectra == 1728, f"found {n_spectra} sources"


@pytest.mark.parametrize(
    ("regime", "n_spectra"),
    [
        ("optical", 838),
        ("nir", 663),
        ("mir", 227),
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
        ("Magellan I Baade", 12),
        ("Magellan II Clay", 11),
        ("SOAR", 2),
        ("Lick Shane 3m", 1),
        ("HST", 77),
        ("Gemini North", 27),
        ("Gemini South", 34),
        ("GTC", 67),
        ("ESO VLT", 114),
        ("SDSS", 1),
        ("Spitzer", 203),
        ("KPNO 2.1m", 93),
        ("KPNO 4m", 251),
        ("JWST", 49),
    ],
)
def test_spectra_telescope(db, telescope, n_spectra):
    t = db.query(db.Spectra).filter(db.Spectra.c.telescope == telescope).astropy()
    assert len(t) == n_spectra, f"found {len(t)} spectra from {telescope}"


@pytest.mark.parametrize(
    ("telescope", "instrument", "n_spectra"),
    [
        ("JWST", "NIRSpec", 25),
        ("JWST", "MIRI", 24),
        ("HST", "WFC3", 77),
        ("ESO VLT", "XShooter", 88),
        ("GTC", "OSIRIS", 66),
        ("Magellan I Baade", "IMACS", 1),
        ("Magellan I Baade", "FIRE", 11),
        ("SDSS", "BOSS", 1),
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
        ("Zhan17.3040", 19),
        ("Zhan17", 4),
        ("Zhan18.1352", 14),
        ("Zhan18.2054", 83),
    ],
)
def test_spectra_references(db, ref, n_spectra):
    t = db.query(db.Spectra).filter(db.Spectra.c.reference == ref).astropy()
    assert len(t) == n_spectra, f"found {len(t)} spectra from {ref}"
