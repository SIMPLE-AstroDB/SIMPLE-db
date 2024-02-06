import pytest
from astrodb_scripts.utils import (
    AstroDBError,
)
from scripts.utils.ingest_spectra_utils import spectrum_plottable


@pytest.mark.parametrize(
    "file",
    [
        "https://s3.amazonaws.com/bdnyc/optical_spectra/2MASS1538-1953_tell.fits",
        "https://s3.amazonaws.com/bdnyc/spex_prism_lhs3003_080729.txt",
        "https://bdnyc.s3.amazonaws.com/IRS/2351-2537_IRS_spectrum.dat",
    ],
)
def test_spectrum_plottable_false(file):
    with pytest.raises(AstroDBError) as error_message:
        spectrum_plottable(file)
        assert "unable to load file as Spectrum1D object" in str(error_message.value)

    result = spectrum_plottable(file, raise_error=False)
    assert result is False


@pytest.mark.parametrize(
    "file",
    [
        "https://bdnyc.s3.amazonaws.com/SpeX/Prism/2MASS+J04510093-3402150_2012-09-27.fits",
        "https://bdnyc.s3.amazonaws.com/IRS/2MASS+J23515044-2537367.fits",
        "https://bdnyc.s3.amazonaws.com/optical_spectra/vhs1256b_opt_Osiris.fits",
    ],
)
def test_spectrum_plottable_true(file):
    result = spectrum_plottable(file)
    assert result is True
