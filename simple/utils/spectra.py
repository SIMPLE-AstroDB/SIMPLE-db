import logging
import sqlite3
from typing import Optional
from sqlalchemy import and_

import requests
import sqlalchemy.exc
from astrodb_utils import AstroDBError, internet_connection, logger
from astrodb_utils.sources import find_source_in_db
from astrodb_utils.spectra import check_spectrum_plottable
from astrodbkit.astrodb import Database
from astropy.io import fits

from simple.schema import Spectra

__all__ = [
    "ingest_spectrum",
    "ingest_spectrum_from_fits",
    "find_spectra",
]


logger.setLevel(logging.DEBUG)


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
        controlled by Regimes table
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
    reference: str
        Reference for spectrum.
        Required to be in Publications table.
    original_spectrum: str
        Path to original spectrum file if different from spectrum.
    comments: str
        Comments about spectrum.
    other_references: str
        Other references for spectrum.
    local_spectrum: str
        Path to local spectrum file.
    raise_error: bool
        If True, raise an error if the spectrum cannot be added.
        If False, continue without raising an error.

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
    # Compile fields into a dictionary

    flags = {"added": False, "content": {}, "message": ""}

    # Get source name as it appears in the database
    db_name = find_source_in_db(db, source)
    logger.debug(f"Found db_name: {db_name} for source: {source}")

    if len(db_name) == 1:
        db_name = db_name[0]
    else:
        msg = f"No unique source match for {source} in the database. Found {db_name}."
        flags["message"] = msg
        raise AstroDBError(msg)
        # exit_function(msg, raise_error=raise_error) # pass flags

    # Check if spectrum is a duplicate
    matches = find_spectra(
        db,
        db_name,
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
        # exit_function(msg, raise_error=raise_error)
        if raise_error:
            raise AstroDBError(msg)
        else:
            logger.warning(msg)
            return flags

    reference = check_publication_in_db(db, reference)
    regime = check_regime_in_db(db, regime)
    instrument, mode, telescope = check_instrument_in_db(
        db,
        instrument=instrument,
        mode=mode,
        telescope=telescope,
    )

    # Check if spectrum file(s) are accessible
    logger.debug(f"Checking spectrum: {spectrum}")
    check_spectrum_accessible(spectrum)
    if original_spectrum is not None:
        logger.debug(f"Checking original_spectrum: {original_spectrum}")
        check_spectrum_accessible(original_spectrum)

    # Check if spectrum is plottable
    flags["plottable"] = check_spectrum_plottable(spectrum)

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
        "reference": reference,
        "comments": comments,
        "other_references": other_references,
    }
    logger.debug(f"Trying to ingest: {row_data}")
    flags["content"] = row_data

    try:
        # Attempt to add spectrum to database
        obj = Spectra(**row_data)
        with db.session as session:
            session.add(obj)
            session.commit()
        # with db.engine.connect() as conn:
        #     conn.execute(db.Spectra.insert().values(row_data))
        #     conn.commit()

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
            f"for unknown reason: {e}"
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


def check_spectrum_accessible(spectrum: str) -> bool:
    """
    Check if the spectrum is accessible
    Parameters
    ----------
    spectrum: str
        URL or path to spectrum file

    Returns
    -------
    bool
        True if the spectrum is accessible, False otherwise
    """
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
            return False
        else:
            msg = f"The spectrum location appears up: {spectrum}"
            logger.debug(msg)
            return True
    else:
        msg = "No internet connection. Internet is needed to check spectrum files."
        raise AstroDBError(msg)


def check_in_database(db, table, constraints):
    """
    Helper function to check that the result of the query is found in the database
    Parameters
    ----------
    db: astrodbkit.astrodb.Database
        Database object created by astrodbkit
    table: astrodbkit.astrodb.Database.table
        Table to be queried as an astrodbkit.astrodb.Database.table object
    constraints: list
        List of constraints to be used in the query as filter input in astrdbkit.astrodb.Database syntax

    Example
    -------
    >>> check_in_database(
        db,
        db.Instruments,
        [
            db.Instruments.c.telescope == "WISE",
            db.Instruments.c.instrument == "WISE",
            db.Instruments.c.mode == "Imaging",
        ],)
    Returns True

    """

    t = db.query(table).filter(and_(*constraints)).table()
    if len(t) == 0:
        msg = f"Could not find {constraints} in {table.name}"
        logger.error(msg)
        return False
    elif len(t) > 1:
        msg = f"Found multiple entries for {constraints} in {table.name}"
        logger.error(msg)
        return False
    else:
        return True


def check_instrument_in_db(db, instrument=None, mode=None, telescope=None):
    instrument_table = (
        db.query(db.Instruments)
        .filter(
            and_(
                db.Instruments.c.instrument.contains(instrument),
                db.Instruments.c.telescope.contains(telescope),
            )
        )
        .table()
    )

    if len(instrument_table) > 1:
        instrument_table = (
            db.query(db.Instruments)
            .filter(
                and_(
                    db.Instruments.c.instrument.contains(instrument),
                    db.Instruments.c.mode.ilike(mode),
                    db.Instruments.c.telescope.contains(telescope),
                )
            )
            .table()
        )

    if len(instrument_table) == 0:
        msg = f"{telescope},{instrument},{mode}, not found in database. Please add it to the Instruments table."
        raise AstroDBError(msg)
    elif len(instrument_table) == 1:
        return (
            instrument_table["instrument"][0],
            instrument_table["mode"][0],
            instrument_table["telescope"][0],
        )


def check_regime_in_db(db, regime):
    regime_table = (
        db.query(db.Regimes).filter(db.Regimes.c.regime.ilike(regime)).table()
    )

    if len(regime_table) == 0:
        msg = (
            f"Regime {regime} not found in database. "
            f"Please add it to the Regimes table or use an existing regime.\n"
            f"Available regimes:\n {db.query(db.Regimes).table()}"
        )
        raise AstroDBError(msg)
    elif len(regime_table) > 1:
        msg = f"Multiple entries for regime {regime} found in database. Please check the Regimes table. Matches: {regime_table}"
        raise AstroDBError(msg)
    else:
        return regime_table["regime"][0]


def check_publication_in_db(db, reference):
    pubs_table = (
        db.query(db.Publications)
        .filter(db.Publications.c.reference.ilike(reference))
        .table()
    )

    if len(pubs_table) == 0:
        msg = f"Reference {reference} not found in database. Please add it to the Publications table."
        raise AstroDBError(msg)
    elif len(pubs_table) > 1:
        msg = f"Multiple entries for reference {reference} found in database. Please check the Publications table. \n  Matches: \n {pubs_table}"
        raise AstroDBError(msg)
    else:
        return pubs_table["reference"][0]
