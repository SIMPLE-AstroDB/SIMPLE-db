# temp_db and logger is defined in conftest.py
import pytest
import sys
from astrodb_scripts.utils import (
    AstroDBError,
)

sys.path.append("./")
from simple.utils.spectra import (
    ingest_spectrum,
    # ingest_spectrum_from_fits,
    spectrum_plottable,
)


@pytest.mark.filterwarnings("ignore::UserWarning")
@pytest.mark.filterwarnings("ignore", message=".*not FITS standard.*")
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
def test_ingest_spectrum_errors(temp_db):
    spectrum = "https://bdnyc.s3.amazonaws.com/tests/U10176.fits"
    with pytest.raises(AstroDBError) as error_message:
        ingest_spectrum(temp_db, source="apple", spectrum=spectrum)
    assert "Regime is required" in str(error_message.value)
    result = ingest_spectrum(
        temp_db, source="apple", spectrum=spectrum, raise_error=False
    )
    assert result["added"] is False
    assert result["skipped"] is True

    with pytest.raises(AstroDBError) as error_message:
        ingest_spectrum(
            temp_db,
            source="apple",
            telescope="IRTF",
            instrument="SpeX",
            mode="Prism",
            regime="nir",
            spectrum=spectrum,
        )
    assert "Reference is required" in str(error_message.value)
    ingest_spectrum(
        temp_db, source="apple", regime="nir", spectrum=spectrum, raise_error=False
    )
    assert result["added"] is False
    assert result["skipped"] is True

    with pytest.raises(AstroDBError) as error_message:
        ingest_spectrum(
            temp_db,
            source="apple",
            regime="nir",
            spectrum=spectrum,
            telescope="IRTF",
            instrument="SpeX",
            mode="Prism",
            reference="Ref 5",
        )
    assert "not in Publications table" in str(error_message.value)
    ingest_spectrum(
        temp_db,
        source="apple",
        regime="nir",
        spectrum=spectrum,
        telescope="IRTF",
        instrument="SpeX",
        mode="Prism",
        reference="Ref 5",
        raise_error=False,
    )
    assert result["added"] is False
    assert result["skipped"] is True

    with pytest.raises(AstroDBError) as error_message:
        ingest_spectrum(
            temp_db,
            source="kiwi",
            regime="nir",
            spectrum=spectrum,
            reference="Ref 1",
            telescope="IRTF",
            instrument="SpeX",
            mode="Prism",
        )
    assert "No unique source match for kiwi in the database" in str(error_message.value)
    result = ingest_spectrum(
        temp_db,
        source="kiwi",
        regime="nir",
        spectrum=spectrum,
        reference="Ref 1",
        raise_error=False,
        telescope="IRTF",
        instrument="SpeX",
        mode="Prism",
    )
    assert result["added"] is False
    assert result["skipped"] is True

    with pytest.raises(AstroDBError) as error_message:
        ingest_spectrum(
            temp_db,
            source="apple",
            regime="nir",
            spectrum=spectrum,
            reference="Ref 1",
            telescope="IRTF",
            instrument="SpeX",
            mode="Prism",
        )
    assert "missing observation date" in str(error_message.value)
    result = ingest_spectrum(
        temp_db,
        source="apple",
        regime="nir",
        spectrum=spectrum,
        reference="Ref 1",
        telescope="IRTF",
        instrument="SpeX",
        mode="Prism",
        raise_error=False,
    )
    assert result["added"] is False
    assert result["skipped"] is False
    assert result["no_obs_date"] is True

    # with pytest.raises(AstroDBError) as error_message:
    #     ingest_spectrum(
    #         db,
    #         source="orange",
    #         regime="nir",
    #         spectrum=spectrum,
    #         reference="Ref 1",
    #         obs_date="Jan20",
    #     )
    # assert "Can't convert obs date to Date Time object" in str(error_message.value)
    # result = ingest_spectrum(
    #     db,
    #     source="orange",
    #     regime="nir",
    #     spectrum=spectrum,
    #     reference="Ref 1",
    #     obs_date="Jan20",
    #     raise_error=False,
    # )
    # assert result["added"] is False
    # assert result["skipped"] is False
    # assert result["no_obs_date"] is True

    with pytest.raises(AstroDBError) as error_message:
        result = ingest_spectrum(
            temp_db,
            source="orange",
            regime="far-uv",
            spectrum=spectrum,
            reference="Ref 1",
            obs_date="1/1/2024",
            telescope="Keck I",
            instrument="LRIS",
            mode="OG570",
        )
    assert "not in Regimes table" in str(error_message.value)
    result = ingest_spectrum(
        temp_db,
        source="orange",
        regime="far-uv",
        spectrum=spectrum,
        reference="Ref 1",
        obs_date="1/1/2024",
        telescope="Keck I",
        instrument="LRIS",
        mode="OG570",
        raise_error=False,
    )
    assert result["added"] is False
    assert result["skipped"] is True


@pytest.mark.filterwarnings("ignore:Verification")
@pytest.mark.filterwarnings("ignore", message=".*Card 'AIRMASS' is not FITS standard.*")
@pytest.mark.filterwarnings(
    "ignore:Note"
)  # : astropy.io.fits uses zero-based indexing.
@pytest.mark.filterwarnings("ignore:'datfix' made the change 'Set MJD-OBS to")
@pytest.mark.filterwarnings(
    "ignore:'erg/cm2/s/A' contains multiple slashes, which is discouraged by the FITS standard"
)
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
        "'erg/cm2/s/A' contains multiple slashes, which is discouraged by the FITS standard.*",
    ),
)
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
