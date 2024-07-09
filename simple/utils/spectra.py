import logging
import sqlite3
from typing import Optional

import astropy.units as u
import matplotlib.pyplot as plt
import numpy as np
import requests
import sqlalchemy.exc
from astrodb_utils import (
    AstroDBError,
    find_publication,
    find_source_in_db,
    internet_connection,
)
from astrodbkit2.astrodb import Database
from astropy.io import fits
from specutils import Spectrum1D

from simple.schema import Spectra

__all__ = [
    "ingest_spectrum",
    "ingest_spectrum_from_fits",
    "spectrum_plottable",
    "find_spectra",
]


logger = logging.getLogger("AstroDB")


def ingest_spectrum(
    db: Database,
    *,
    source: str = None,
    spectrum: str = None,
    regime: str = None,
    telescope: str = None,
    instrument: str = None,
    mode: str = None,
    obs_date: str = None,
    reference: str = None,
    original_spectrum: Optional[str] = None,
    comments: Optional[str] = None,
    other_references: Optional[str] = None,
    local_spectrum: Optional[str] = None,
    raise_error: bool = True,
):
    """
    Parameters
    ----------
    db: astrodbkit2.astrodb.Database
        Database object created by astrodbkit2
    source: str
        source name
    spectrum: str
        URL or path to spectrum file
    regime: str
        Regime of spectrum (optical, infrared, radio, etc.)
        controlled by to-be-made Regimes table
    telescope: str
        Telescope used to obtain spectrum.
        Required to be in Telescopes table.
    instrument: str
        Instrument used to obtain spectrum.
        Instrument-Mode pair needs to be in Instruments table.
    mode: str
        Instrument mode used to obtain spectrum.
        Instrument-Mode pair needs to be in Instruments table.
    obs_date: str
        Observation date of spectrum.

    Returns
    -------
    flags: dict
        Status response with the following keys:
             - "added": True if it's added and False if it's skipped.
             - "content": the data that was attempted to add
             - "message": string which includes information about why skipped

    Raises
    ------
    AstroDBError
    """

    flags = {
        "added": False,
        "content": {},
        "message": ""
    }

    # Get source name as it appears in the database
    db_name = find_source_in_db(db, source)

    if len(db_name) != 1:
        msg = f"No unique source match for {source} in the database"
        if raise_error:
            raise AstroDBError(msg)
        else:
            return flags
    else:
        db_name = db_name[0]

    # Check if spectrum file is accessible
    # First check for internet
    internet = internet_connection()
    if internet:
        request_response = requests.head(spectrum)
        status_code = (
            request_response.status_code
        )  # The website is up if the status code is 200
        if status_code != 200:
            msg = (
                "The spectrum location does not appear to be valid: \n"
                f"spectrum: {spectrum} \n"
                f"status code: {status_code}"
            )
            logger.error(msg)
            if raise_error:
                raise AstroDBError(msg)
        else:
            msg = f"The spectrum location appears up: {spectrum}"
            logger.debug(msg)
        if original_spectrum is not None:
            request_response1 = requests.head(original_spectrum)
            status_code1 = request_response1.status_code
            if status_code1 != 200:
                msg = (
                    "The spectrum location does not appear to be valid: \n"
                    f"spectrum: {original_spectrum} \n"
                    f"status code: {status_code1}"
                )
                logger.error(msg)
                if raise_error:
                    raise AstroDBError(msg)
            else:
                msg = f"The spectrum location appears up: {original_spectrum}"
                logger.debug(msg)
    else:
        msg = "No internet connection. Internet is needed to check spectrum files."
        raise AstroDBError(msg)

    matches = find_spectra(
        db,
        source,
        reference=reference,
        obs_date=obs_date,
        telescope=telescope,
        instrument=instrument,
        mode=mode,
    )
    if len(matches) > 0:
        msg = f"Skipping suspected duplicate measurement: {source}\n"
        msg2 = f"{matches}" f"{instrument, mode, obs_date, reference, spectrum} \n"
        logger.warning(msg)
        logger.debug(msg2)
        flags["message"] = msg
        if raise_error:
            raise AstroDBError(msg)
        else:
            return flags

    # Check if spectrum is plottable
    flags["plottable"] = spectrum_plottable(spectrum, raise_error=raise_error)

    # Compile fields into a dictionary
    row_data = {
        "source": db_name,
        "access_url": spectrum,
        "original_spectrum": original_spectrum,
        "local_spectrum": local_spectrum,
        "regime": regime,
        "telescope": telescope,
        "instrument": instrument,
        "mode": mode,
        "observation_date": obs_date,
        "comments": comments,
        "reference": reference,
        "other_references": other_references,
    }
    logger.debug(row_data)
    flags["content"] = row_data

    try:
        # Attempt to add spectrum to database
        # This will throw errors based on validation in schema.py 
        # and any database checks (as for example IntegrityError)
        obj = Spectra(**row_data)
        with db.session as session:
            session.add(obj)
            session.commit()

        flags["added"] = True
        logger.info(f"Added {source} : \n" f"{row_data}")
    except sqlalchemy.exc.IntegrityError as e:
        msg = f"Integrity Error: {source} \n {e}"
        logger.error(msg + f" \n {row_data}")
        flags["message"] = msg
        if raise_error:
            raise AstroDBError(msg)
        else:
            return flags
    except sqlite3.IntegrityError as e:
        msg = f"Integrity Error: {source} \n {e}"
        logger.error(msg)
        flags["message"] = msg
        if raise_error:
            raise AstroDBError(msg)
        else:
            return flags
    except Exception as e:
        msg = (
            f"Spectrum for {source} could not be added to the database "
            f"for unexpected reason: {e}"
        )
        logger.error(msg)
        flags["message"] = msg
        if raise_error:
            raise AstroDBError(msg)
        else:
            return flags

    return flags


def ingest_spectrum_from_fits(db, source, spectrum_fits_file):
    """
    Ingests spectrum using data found in the header

    Parameters
    ----------
    db
    source
    spectrum_fits_file

    """
    header = fits.getheader(spectrum_fits_file)
    regime = header["SPECBAND"]
    if regime == "opt":
        regime = "optical"
    telescope = header["TELESCOP"]
    instrument = header["INSTRUME"]
    try:
        mode = header["MODE"]
    except KeyError:
        mode = None
    obs_date = header["DATE-OBS"]
    doi = header["REFERENC"]
    data_header = fits.getheader(spectrum_fits_file, 1)
    w_unit = data_header["TUNIT1"]
    flux_unit = data_header["TUNIT2"]

    reference_match = (
        db.query(db.Publications.c.publication)
        .filter(db.Publications.c.doi == doi)
        .table()
    )
    reference = reference_match["publication"][0]

    ingest_spectrum(
        db,
        source,
        spectrum_fits_file,
        regime,
        telescope,
        instrument,
        mode,
        obs_date,
        reference,
        wavelength_units=w_unit,
        flux_units=flux_unit,
    )


def spectrum_plottable(spectrum_path, raise_error=True, show_plot=False):
    """
    Check if spectrum is plottable
    """
    # load the spectrum and make sure it's a Spectrum1D object

    try:
        # spectrum: Spectrum1D = load_spectrum(spectrum_path) #astrodbkit2 method
        spectrum = Spectrum1D.read(spectrum_path)
    except Exception as e:
        msg = (
            str(e) + f"\nSkipping {spectrum_path}: \n"
            "unable to load file as Spectrum1D object"
        )
        if raise_error:
            logger.error(msg)
            raise AstroDBError(msg)
        else:
            logger.warning(msg)
            return False

    # checking spectrum has good units and not only NaNs
    try:
        wave: np.ndarray = spectrum.spectral_axis.to(u.micron).value
        flux: np.ndarray = spectrum.flux.value
    except AttributeError as e:
        msg = str(e) + f"Skipping {spectrum_path}: unable to parse spectral axis"
        if raise_error:
            logger.error(msg)
            raise AstroDBError(msg)
        else:
            logger.warning(msg)
            return False
    except u.UnitConversionError as e:
        msg = (
            f"{e} \n"
            f"Skipping {spectrum_path}: unable to convert spectral axis to microns"
        )
        if raise_error:
            logger.error(msg)
            raise AstroDBError(msg)
        else:
            logger.warning(msg)
            return False
    except ValueError as e:
        msg = f"{e} \nSkipping {spectrum_path}: Value error"
        if raise_error:
            logger.error(msg)
            raise AstroDBError(msg)
        else:
            logger.warning(msg)
            return False

    # check for NaNs
    nan_check: np.ndarray = ~np.isnan(flux) & ~np.isnan(wave)
    wave = wave[nan_check]
    flux = flux[nan_check]
    if not len(wave):
        msg = f"Skipping {spectrum_path}: spectrum is all NaNs"
        if raise_error:
            logger.error(msg)
            raise AstroDBError(msg)
        else:
            logger.warning(msg)
            return False

    if show_plot:
        plt.plot(wave, flux)
        plt.show()

    return True


def find_spectra(
    db: Database,
    source: str,
    *,
    reference: str = None,
    obs_date: str = None,
    telescope: str = None,
    instrument: str = None,
    mode: str = None,
):
    """
    Find what spectra already exists in database for this source
    Finds matches based on parameter provided.
    E.g., if only source is provided, all spectra for that source are returned.
        If Source and telescope are provided,
        only spectra for that source and telescope are provided.

    Parameters
    ----------
    db: astrodbkit2.astrodb.Database
        Database object created by astrodbkit2
    source: str
        source name

    Returns
    -------
    source_spec_data: astropy.table.Table
        Table of spectra for source
    """

    source_spec_data = (
        db.query(db.Spectra).filter(db.Spectra.c.source == source).table()
    )

    n_spectra_matches = len(source_spec_data)

    if n_spectra_matches > 0 and reference is not None:
        source_spec_data = source_spec_data[source_spec_data["reference"] == reference]
        n_spectra_matches = len(source_spec_data)

    if n_spectra_matches > 0 and telescope is not None:
        source_spec_data = source_spec_data[source_spec_data["telescope"] == telescope]
        n_spectra_matches = len(source_spec_data)

    if n_spectra_matches > 0 and obs_date is not None:
        source_spec_data = source_spec_data[
            source_spec_data["observation_date"] == obs_date
        ]
        n_spectra_matches = len(source_spec_data)

    if n_spectra_matches > 0 and instrument is not None:
        source_spec_data = source_spec_data[
            source_spec_data["instrument"] == instrument
        ]
        n_spectra_matches = len(source_spec_data)

    if n_spectra_matches > 0 and mode is not None:
        source_spec_data = source_spec_data[source_spec_data["mode"] == mode]
        n_spectra_matches = len(source_spec_data)

    return source_spec_data
