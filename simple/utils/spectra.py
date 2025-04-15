import logging
import sqlite3
from typing import Optional

import requests
import sqlalchemy.exc
from astrodb_utils import (
    AstroDBError,
    internet_connection,
)
from astrodb_utils.sources import find_source_in_db
from astrodb_utils.spectra import check_spectrum_plottable
from astrodbkit.astrodb import Database
from astropy.io import fits

__all__ = [
    "ingest_spectrum",
    "ingest_spectrum_from_fits",
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
    db: astrodbkit.astrodb.Database
        Database object created by astrodbkit
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
        flags["message"] = msg
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
        msg = f"Skipping suspected duplicate measurement: {source}"
        msg2 = f"{matches} {instrument, mode, obs_date, reference, spectrum}"
        logger.debug(msg2)
        flags["message"] = msg
        if raise_error:
            raise AstroDBError(msg)
        else:
            logger.warning(msg)
            return flags

    # Check if spectrum is plottable
    flags["plottable"] = check_spectrum_plottable(spectrum, raise_error=raise_error)

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
        # Removing old version of ingesting with ORM, which used schema.py
        # Attempt to add spectrum to database
        # This will throw errors based on validation in schema.py
        # and any database checks (as for example IntegrityError)
        # obj = Spectra(**row_data)
        # with db.session as session:
        #     session.add(obj)
        #     session.commit()
        with db.engine.connect() as conn:
            conn.execute(db.Spectra.insert().values(row_data))
            conn.commit()

        flags["added"] = True
        logger.info(f"Added {source} : \n" f"{row_data}")
    except (sqlite3.IntegrityError, sqlalchemy.exc.IntegrityError) as e:
        msg = f"Integrity Error: {source} \n {e}"
        flags["message"] = msg
        if raise_error:
            raise AstroDBError(msg)
        else:
            logger.error(msg)
            return flags
    except Exception as e:
        msg = (
            f"Spectrum for {source} could not be added to the database "
            f"for unexpected reason: {e}"
        )
        flags["message"] = msg
        if raise_error:
            raise AstroDBError(msg)
        else:
            logger.warning(msg)
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
    db: astrodbkit.astrodb.Database
        Database object created by astrodbkit
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
