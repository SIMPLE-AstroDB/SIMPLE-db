import pytest
from sqlalchemy import except_, select, and_
from astropy.table import Table

def test_model(db):
    t = (
        db.query(db.ModeledParameters)
        .filter(db.ModeledParameters.c.model.is_(None))
        .astropy()
    )
    assert len(t) == 939

def test_adopted(db):
    t = (
        db.query(db.ModeledParameters)
        .filter(db.ModeledParameters.c.adopted.is_(1))
        .astropy()
    )
    assert len(t) == 0

@pytest.mark.parametrize(
    ("param", "n_counts"),
    [
        ("T eff", 2339),
        ("log g", 2339),
        ("mass", 1285),
        ("radius", 2297),
        ("metallicity", 2),
        ("L bol", 1109)
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
        ("Sang23", "T eff", 2108),
        ("Sang23", "log g", 2108),
        ("Sang23", "mass", 1054),
        ("Sang23", "radius", 2108),
        ("Sang23", "metallicity", 0),
        ("Sang23", "L bol", 1054),
        ("Fili15", "T eff", 174),
        ("Fili15", "log g", 174),
        ("Fili15", "mass", 174),
        ("Fili15", "radius", 174),
        ("Fili15", "metallicity", 0),
        ("Fili15", "L bol", 5),
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
        ("Zhan20", "T eff", 41),
        ("Zhan20", "log g", 41),
        ("Zhan20", "mass", 41),
        ("Zhan20", "radius", 0),
        ("Zhan20", "metallicity", 0),
        ("Zhan20", "L bol", 21),
        ("Zapa18", "T eff", 0),
        ("Zapa18", "log g", 0),
        ("Zapa18", "mass", 0),
        ("Zapa18", "radius", 0),
        ("Zapa18", "metallicity", 0),
        ("Zapa18", "L bol", 0),
        ("Marl12", "T eff", 0),
        ("Marl12", "log g", 0),
        ("Marl12", "mass", 0),
        ("Marl12", "radius", 0),
        ("Marl12", "metallicity", 0),
        ("Marl12", "L bol", 1),
        ("Gree18", "T eff", 0),
        ("Gree18", "log g", 0),
        ("Gree18", "mass", 0),
        ("Gree18", "radius", 0),
        ("Gree18", "metallicity", 0),
        ("Gree18", "L bol", 3),
        ("Bowl18", "T eff", 0),
        ("Bowl18", "log g", 0),
        ("Bowl18", "mass", 0),
        ("Bowl18", "radius", 0),
        ("Bowl18", "metallicity", 0),
        ("Bowl18", "L bol", 1),
        ("Zhan21.53", "T eff", 0),
        ("Zhan21.53", "log g", 0),
        ("Zhan21.53", "mass", 0),
        ("Zhan21.53", "radius", 0),
        ("Zhan21.53", "metallicity", 0),
        ("Zhan21.53", "L bol", 3),
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