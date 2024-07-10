# temp_db and logger is defined in conftest.py
import pytest
from astrodb_utils.utils import (
    AstroDBError,
)

from simple.utils.spectra import (
    ingest_spectrum,
    # ingest_spectrum_from_fits,
    spectrum_plottable,
)


@pytest.mark.filterwarnings(
    "ignore", message=".*Note: astropy.io.fits uses zero-based indexing.*"
)
@pytest.mark.filterwarnings(
    "ignore", message=".*'datfix' made the change 'Set MJD-OBS to.*"
)
@pytest.mark.filterwarnings(
    "ignore",
    message=(
        ".*'erg/cm2/s/A' contains multiple slashes, "
        "which is discouraged by the FITS standard.*",
    ),
)
@pytest.mark.parametrize("test_input, message", [
    ({"source": "apple",
      "telescope": "IRTF", 
      "instrument": "SpeX", 
      "mode": "Prism",
      }, "Value required for regime"),  # missing regime
    ({"source": "apple",
      "regime": "nir", 
      "instrument": "SpeX", 
      "obs_date": "2020-01-01",
      }, "Value required for telescope"),  # missing telescope
    ({"source": "apple",
      "regime": "nir", 
      "telescope": "IRTF", 
      "obs_date": "2020-01-01",
      }, "Value required for instrument"),  # missing instrument
    ({"source": "apple", 
      "telescope": "IRTF", 
      "instrument": "SpeX", 
      "mode": "Prism", 
      "regime": "nir", 
      "obs_date": "2020-01-01",
      }, "NOT NULL constraint failed: Spectra.reference"),  # missing reference
    ({"source": "apple", 
      "telescope": "IRTF", 
      "instrument": "SpeX", 
      "mode": "Prism", 
      "regime": "nir", 
      "obs_date": "2020-01-01",
      "reference": "Ref 5",
      }, "FOREIGN KEY constraint failed"),  # invalid reference
    ({"source": "kiwi", 
      "telescope": "IRTF", 
      "instrument": "SpeX", 
      "mode": "Prism", 
      "regime": "nir", 
      "obs_date": "2020-01-01",
      "reference": "Ref 1",
      }, "No unique source match for kiwi in the database"),  # invalid source
    ({"source": "apple", 
      "telescope": "IRTF", 
      "instrument": "SpeX", 
      "mode": "Prism", 
      "regime": "nir", 
      "reference": "Ref 1",
      }, "Invalid date received: None"),  # missing date
    ({"source": "apple", 
      "telescope": "IRTF", 
      "instrument": "SpeX", 
      "mode": "Prism", 
      "regime": "fake regime", 
      "obs_date": "2020-01-01",
      "reference": "Ref 1",
      }, "FOREIGN KEY constraint failed"),  # invalid regime
])
def test_ingest_spectrum_errors(temp_db, test_input, message):
    # Test for ingest_spectrum that is expected to return errors

    # Prepare parameters to send to ingest_spectrum
    spectrum = "https://bdnyc.s3.amazonaws.com/tests/U10176.fits"
    parameters = {"db": temp_db, "spectrum": spectrum}
    parameters.update(test_input)

    # Check that error was raised
    with pytest.raises(AstroDBError) as error_message:
        _ = ingest_spectrum(**parameters)
    assert message in str(error_message.value)

    # Suppress error but check that it was still captured
    result = ingest_spectrum(**parameters, raise_error=False)
    assert result["added"] is False
    assert message in result["message"]


@pytest.mark.filterwarnings("ignore:Verification")
@pytest.mark.filterwarnings("ignore", message=".*Card 'AIRMASS' is not FITS standard.*")
@pytest.mark.filterwarnings(
    "ignore:Note"
)  # : astropy.io.fits uses zero-based indexing.
@pytest.mark.filterwarnings("ignore:'datfix' made the change 'Set MJD-OBS to")
@pytest.mark.filterwarnings(
    "ignore:'erg/cm2/s/A' contains multiple slashes,"
    " which is discouraged by the FITS standard"
)
@pytest.mark.filterwarnings("ignore")
def test_ingest_spectrum_works(temp_db):
    spectrum = "https://bdnyc.s3.amazonaws.com/tests/U10176.fits"
    result = ingest_spectrum(
        temp_db,
        source="banana",
        regime="nir",
        spectrum=spectrum,
        reference="Ref 1",
        obs_date="2020-01-01",
        telescope="IRTF",
        instrument="SpeX",
        mode="Prism",
    )
    assert result["added"] is True


@pytest.mark.filterwarnings("ignore:Invalid 'BLANK' keyword in header.")
@pytest.mark.filterwarnings("ignore:'datfix' made the change 'Set MJD-OBS to")
@pytest.mark.filterwarnings("ignore:The WCS transformation has more axes")
@pytest.mark.filterwarnings("ignore:'cdfix' made the change 'Success'")
@pytest.mark.filterwarnings("ignore:MJD-OBS =")
@pytest.mark.filterwarnings(
    "ignore",
    message=(
        "'erg/cm2/s/A' contains multiple slashes, "
        "which is discouraged by the FITS standard.*",
    ),
)
@pytest.mark.filterwarnings("ignore")
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
        (
            "https://bdnyc.s3.amazonaws.com/SpeX/Prism/"
            "2MASS+J04510093-3402150_2012-09-27.fits"
        ),
        "https://bdnyc.s3.amazonaws.com/IRS/2MASS+J23515044-2537367.fits",
        "https://bdnyc.s3.amazonaws.com/optical_spectra/vhs1256b_opt_Osiris.fits",
    ],
)
def test_spectrum_plottable_true(file):
    result = spectrum_plottable(file)
    assert result is True
