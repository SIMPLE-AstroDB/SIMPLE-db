"""
This function is in the process of being moved to astrodb_scripts
"""

import pytest
from astrodb_scripts import (
    AstroDBError,
)
from simple.utils.photometry import (
    fetch_svo,
    assign_ucd,
)


@pytest.mark.parametrize(
    "telescope, instrument, filter_name, wavelength",
    [("HST", "WFC3_IR", "F140W", 13734.66)],
)
def test_fetch_svo(telescope, instrument, filter_name, wavelength):
    filter_id, wave, fwhm = fetch_svo(telescope, instrument, filter_name)
    assert wave.unit == "Angstrom"
    assert wave.value == pytest.approx(wavelength)


def test_fetch_svo_fail():
    with pytest.raises(AstroDBError) as error_message:
        fetch_svo("HST", "WFC3", "F140W")
    assert "not found in SVO" in str(error_message.value)


@pytest.mark.parametrize(
    "wave, ucd",
    [
        (100, None),
        (3001, "em.opt.U"),
        (4500, "em.opt.B"),
        (5500, "em.opt.V"),
        (6500, "em.opt.R"),
        (8020, "em.opt.I"),
        (12000, "em.IR.J"),
        (16000, "em.IR.H"),
        (22000, "em.IR.K"),
        (35000, "em.IR.3-4um"),
        (45000, "em.IR.4-8um"),
        (85000, "em.IR.8-15um"),
        (100000, "em.IR.8-15um"),
        (200000, "em.IR.15-30um"),
        (500000, None),
    ],
)
def test_assign_ucd(wave, ucd):
    assert assign_ucd(wave) == ucd
