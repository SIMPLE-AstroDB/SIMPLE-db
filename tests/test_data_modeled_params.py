import pytest
from sqlalchemy import except_, select, and_
from astropy.table import Table

def test_model(db):
    t = (
        db.query(db.ModeledParameters)
        .filter(db.ModeledParameters.c.model.is_(None))
        .astropy()
    )
    assert len(t) == 705


@pytest.mark.parametrize(
    ("param", "n_counts"),
    [
        ("T eff", 2282),
        ("log g", 2282),
        ("mass", 1229),
        ("radius", 2281),
        ("metallicity", 2),
        ("L bol", 1053)
    ],
)
def test_modeledparameters_params(db, param, n_counts):
    # Test to verify existing counts of modeled parameters
    t = (
        db.query(db.ModeledParameters)
        .filter(db.ModeledParameters.c.parameter == param)
        .astropy()
    )
    assert (
        len(t) == n_counts
    ), f"found {len(t)} modeled parameters with {param} parameter"