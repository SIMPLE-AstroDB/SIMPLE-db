"""Tests for the schema itself and any validating functions"""

from datetime import datetime

import pytest

from simple.schema import Parallaxes, PhotometryFilters, Publications, Sources, Spectra


def schema_tester(table, values, error_state):
    """Helper function to handle the basic testing of the schema classes"""
    if error_state is None:
        _ = table(**values)
    else:
        with pytest.raises(error_state):
            _ = table(**values)

@pytest.mark.parametrize("values, error_state",
                         [
                             ({"band": "2MASS.J", "effective_wavelength": 1.2, "ucd": "phot;em.IR.J"}, None),
                             ({"band": "2MASS.J", "effective_wavelength": 1.2, "ucd": "bad"}, ValueError),
                             ({"band": "bad", "effective_wavelength": 1.2, "ucd": "phot;em.IR.J"}, ValueError),
                             ({"band": "2MASS.J", "effective_wavelength": -99, "ucd": "phot;em.IR.J"}, ValueError),
                          ])
def test_photometryfilters(values, error_state):
    """Validating PhotometryFilters"""
    schema_tester(PhotometryFilters, values, error_state)


@pytest.mark.parametrize("values, error_state",
                         [
                             ({"source": "FAKE", "ra": 1.2, "dec": 3.4, "reference": "Ref1"}, None),
                             ({"source": "FAKE", "ra": 999, "dec": 3.4, "reference": "Ref1"}, ValueError),
                             ({"source": "FAKE", "ra": -999, "dec": 3.4, "reference": "Ref1"}, ValueError),
                             ({"source": "FAKE", "ra": 1.2, "dec": 999, "reference": "Ref1"}, ValueError),
                             ({"source": "FAKE", "ra": 1.2, "dec": -999, "reference": "Ref1"}, ValueError),
                          ])
def test_sources(values, error_state):
    """Validating Sources"""
    schema_tester(Sources, values, error_state)


@pytest.mark.parametrize("values, error_state",
                         [
                             ({"reference": "Ref1"}, None),
                             ({"reference": None}, ValueError),
                             ({"reference": "THIS-REFERENCE-IS-REALLY-REALLY-LONG"}, ValueError),
                          ])
def test_publications(values, error_state):
    """Validating Publications"""
    schema_tester(Publications, values, error_state)


@pytest.mark.parametrize("values, error_state",
                         [
                             ({"parallax": 0.1}, None),
                             ({"parallax": -999}, ValueError),
                             ({"parallax": None}, ValueError),
                          ])
def test_parallaxes(values, error_state):
    """Validating Parallaxes"""
    schema_tester(Parallaxes, values, error_state)


@pytest.mark.parametrize("values, error_state",
                         [
                             ({"access_url": None}, ValueError),
                             ({"source": None}, ValueError),
                             ({"regime": None}, ValueError),
                             ({"telescope": None}, ValueError),
                             ({"instrument": None}, ValueError),
                             ({"mode": None}, ValueError),
                             ({"observation_date": "2024-01-01"}, None),
                             ({"observation_date": datetime(2024,1,1)}, None),
                             ({"observation_date": None}, ValueError),
                             ({"observation_date": "fake"}, ValueError),
                          ])
def test_spectra(values, error_state):
    """Validating Spectra"""
    # Note: due to how this works, only the columns with values provided get tested 
    schema_tester(Spectra, values, error_state)