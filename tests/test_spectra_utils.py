# temp_db and logger is defined in conftest.py
import pytest
from astrodb_utils.utils import (
    AstroDBError,
)

from simple.utils.spectra import (
    ingest_spectrum,
    # ingest_spectrum_from_fits,
)


@pytest.mark.filterwarnings(
    "ignore",
    message=".*SAWarning: Column 'Spectra.reference' is marked as a member of the primary key for table 'Spectra'.*",
)
@pytest.mark.filterwarnings(
    "ignore", message=".*'kiwi': No known catalog could be found.*"
)
@pytest.mark.parametrize(
    "test_input, message",
    [
        (
            {
                "source": "apple",
                "telescope": "IRTF",
                "instrument": "SpeX",
                "mode": "Prism",
            },
            "Value required for regime",
        ),  # missing regime
        (
            {
                "source": "apple",
                "regime": "nir",
                "instrument": "SpeX",
                "obs_date": "2020-01-01",
            },
            "Value required for telescope",
        ),  # missing telescope
        (
            {
                "source": "apple",
                "regime": "nir",
                "telescope": "IRTF",
                "obs_date": "2020-01-01",
            },
            "Value required for instrument",
        ),  # missing instrument
        (
            {
                "source": "apple",
                "telescope": "IRTF",
                "instrument": "SpeX",
                "mode": "Prism",
                "regime": "nir",
                "obs_date": "2020-01-01",
            },
            "NOT NULL constraint failed: Spectra.reference",
        ),  # missing reference
        (
            {
                "source": "apple",
                "telescope": "IRTF",
                "instrument": "SpeX",
                "mode": "Prism",
                "regime": "nir",
                "obs_date": "2020-01-01",
                "reference": "Ref 5",
            },
            "FOREIGN KEY constraint failed",
        ),  # invalid reference
        (
            {
                "source": "kiwi",
                "telescope": "IRTF",
                "instrument": "SpeX",
                "mode": "Prism",
                "regime": "nir",
                "obs_date": "2020-01-01",
                "reference": "Ref 1",
            },
            "No unique source match for kiwi in the database",
        ),  # invalid source
        (
            {
                "source": "apple",
                "telescope": "IRTF",
                "instrument": "SpeX",
                "mode": "Prism",
                "regime": "nir",
                "reference": "Ref 1",
            },
            "Invalid date received: None",
        ),  # missing date
        (
            {
                "source": "apple",
                "telescope": "IRTF",
                "instrument": "SpeX",
                "mode": "Prism",
                "regime": "fake regime",
                "obs_date": "2020-01-01",
                "reference": "Ref 1",
            },
            "FOREIGN KEY constraint failed",
        ),  # invalid regime
    ],
)
def test_ingest_spectrum_errors(temp_db, test_input, message):
    # Test for ingest_spectrum that is expected to return errors

    # Prepare parameters to send to ingest_spectrum
    spectrum = "https://bdnyc.s3.amazonaws.com/IRS/2MASS+J03552337%2B1133437.fits"
    parameters = {"db": temp_db, "spectrum": spectrum}
    parameters.update(test_input)

    # Check that error was raised
    with pytest.raises(AstroDBError) as error_message:
        _ = ingest_spectrum(**parameters)
    # assert message in str(error_message.value)  # Custom error messages from schema.py are no longer returned

    # Suppress error but check that it was still captured
    result = ingest_spectrum(**parameters, raise_error=False)
    assert result["added"] is False
    # assert message in result["message"]  # Custom error messages from schema.py are no longer returned


@pytest.mark.xfail(reason="ingest_spectrum needs to convert obs_date to datetime")
def test_ingest_spectrum_works(temp_db):
    spectrum = "https://bdnyc.s3.amazonaws.com/IRS/2MASS+J03552337%2B1133437.fits"
    result = ingest_spectrum(
        temp_db,
        source="banana",
        regime="nir",
        spectrum=spectrum,
        reference="Ref 1",
        obs_date="2020-01-01",  # needs to be a datetime object
        telescope="IRTF",
        instrument="SpeX",
        mode="Prism",
    )
    assert result["added"] is True
