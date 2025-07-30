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

@pytest.mark.parametrize(
    ("ref", "param", "n_counts"),
    [
        ("Sang23", "T eff", 2106),
        ("Sang23", "log g", 2106),
        ("Sang23", "mass", 1053),
        ("Sang23", "radius", 2106),
        ("Sang23", "metallicity", 0),
        ("Sang23", "L bol", 1053),
        ("Fili15", "T eff", 174),
        ("Fili15", "log g", 174),
        ("Fili15", "mass", 174),
        ("Fili15", "radius", 174),
        ("Fili15", "metallicity", 0),
        ("Fili15", "L bol", 0),
        ("Zhan19.1423", "T eff", 1),
        ("Zhan19.1423", "log g", 1),
        ("Zhan19.1423", "mass", 1),
        ("Zhan19.1423", "radius", 0),
        ("Zhan19.1423", "metallicity", 1),
        ("Zhan19.1423", "L bol", 0),
        ("Lodi22", "T eff", 1),
        ("Lodi22", "log g", 1),
        ("Lodi22", "mass", 1),
        ("Lodi22", "radius", 1),
        ("Lodi22", "metallicity", 1),
        ("Lodi22", "L bol", 0),
    ],
)
def test_modeledparameters_param_refs(db, ref, param, n_counts):
    t = (
        db.query(db.ModeledParameters)
        .filter(
            db.ModeledParameters.c.parameter == param,
            db.ModeledParameters.c.reference == ref
        )
        .astropy()
    )

    assert (
        len(t) == n_counts
    ), f"found {len(t)} {param} parameters derived from {ref} reference"