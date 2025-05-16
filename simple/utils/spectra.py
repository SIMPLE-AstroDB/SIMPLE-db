import logging
import sqlite3
from sqlalchemy import and_
import datetime
import os
from typing import Optional
import requests
import sqlalchemy.exc
from astrodb_utils import AstroDBError, internet_connection
from astrodb_utils.sources import find_source_in_db
from astrodb_utils.spectra import check_spectrum_plottable
from astrodbkit.astrodb import Database
from astropy.io import fits

__all__ = [
    "ingest_spectrum",
    "find_spectra",
]

logger = logging.getLogger(
    "astrodb_utils.simple.spectra"
)  # becomes a child of "astrodb_utils" logger
# once moved to astrodb_utils, should be
# logger = logging.getLogger(__name__)


def ingest_spectrum(
    db: Database,
    *,
    source: str = None,
    spectrum: str = None,
    regime: str = None,
    telescope: str = None,
    instrument: str = None,
    mode: str = None,
    obs_date: str | datetime.datetime = None,
    reference: str = None,
    original_spectrum: Optional[str] = None,
    comments: Optional[str] = None,
    other_references: Optional[str] = None,
    local_spectrum: Optional[str] = None,
    raise_error: bool = True,
    format: Optional[str] = None,
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
    format: str
        Format of the spectrum file used by specutils to load the file.
        If not provided, the format will be determined by specutils.
        Options: "tabular-fits",

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

    # If a date is provided as a string, convert it to datetime
    logger.debug(f"Parsing obs_date: {obs_date}")
    if obs_date is not None and isinstance(obs_date, str):
        parsed_date = check_obs_date(obs_date, raise_error=raise_error)
    elif isinstance(obs_date, datetime.datetime):
        parsed_date = obs_date

    if obs_date is None or (parsed_date is None and raise_error is False):
        msg = f"Observation date is not valid: {obs_date}"
        flags["message"] = msg
        if raise_error:
            raise AstroDBError(msg)
        else:
            logger.warning(msg)
            return flags

    # Get source name as it appears in the database
    db_name = find_source_in_db(db, source)
    logger.debug(f"Found db_name: {db_name} for source: {source}")

    if len(db_name) == 1:
        db_name = db_name[0]
    else:
        msg = f"No unique source match for {source} in the database. Found {db_name}."
        flags["message"] = msg
        if raise_error:
            raise AstroDBError(msg)
        else:
            logger.warning(msg)
            return flags

    # Check if spectrum is a duplicate
    matches = find_spectra(
        db,
        db_name,
        reference=reference,
        obs_date=parsed_date,
        telescope=telescope,
        instrument=instrument,
        mode=mode,
    )
    if len(matches) > 0:
        msg = f"Skipping suspected duplicate measurement: {source}"
        msg2 = f"{matches} {instrument, mode, parsed_date, reference, spectrum}"
        logger.debug(msg2)
        flags["message"] = msg
        # exit_function(msg, raise_error=raise_error)
        if raise_error:
            raise AstroDBError(msg)
        else:
            logger.warning(msg)
            return flags

    if reference is None:
        msg = "Reference is required."
        flags["message"] = msg
        if raise_error:
            raise AstroDBError(msg)
        else:
            logger.warning(msg)
            return flags
    else:
        # reference = check_publication_in_db(db, reference)
        pubs_table = (
            db.query(db.Publications)
            .filter(db.Publications.c.reference.ilike(reference))
            .table()
        )

        if len(pubs_table) == 1:
            reference = pubs_table["reference"][0]
        else:
            if len(pubs_table) == 0:
                msg = f"Reference not found in database: {reference}. Add it to the Publications table."
            elif len(pubs_table) > 1:
                msg = f"Multiple entries for reference {reference} found in database. Check the Publications table. \n  Matches: \n {pubs_table}"
            else:
                msg = f"Unexpected condition: {reference}."

            if raise_error:
                raise AstroDBError(msg)
            else:
                logger.warning(msg)
                flags["message"] = msg
                return flags

    regime = get_db_regime(db, regime)
    if regime is None:
        msg = f"Regime not found in database: {regime}."
        flags["message"] = msg
        if raise_error:
            raise AstroDBError(msg)
        else:
            logger.warning(msg)
            return flags

    instrument, mode, telescope = check_instrument_in_db(
        db,
        instrument=instrument,
        mode=mode,
        telescope=telescope,
    )

    # Check if spectrum file(s) are accessible
    check_spectrum_accessible(spectrum)
    if original_spectrum is not None:
        check_spectrum_accessible(original_spectrum)

    # Check if spectrum is plottable
    flags["plottable"] = check_spectrum_plottable(spectrum, format=format)

    if os.path.splitext(spectrum)[1] == ".fits":
        with fits.open(spectrum) as hdul:
            hdul.verify("warn")

    row_data = {
        "source": db_name,
        "access_url": spectrum,
        "original_spectrum": original_spectrum,
        "local_spectrum": local_spectrum,
        "regime": regime,
        "telescope": telescope,
        "instrument": instrument,
        "mode": mode,
        "observation_date": parsed_date,
        "reference": reference,
        "comments": comments,
        "other_references": other_references,
    }
    logger.debug(f"Trying to ingest: {row_data}")
    flags["content"] = row_data

    try:
        # Attempt to add spectrum to database
        with db.engine.connect() as conn:
            conn.execute(db.Spectra.insert().values(row_data))
            conn.commit()

        flags["added"] = True
        logger.info(
            f"Added spectrum for {source}: {telescope}-{instrument}-{mode} from {reference} on {parsed_date.strftime('%d %b %Y')}"
        )
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
    logger.debug(f"Checking spectrum: {spectrum}")
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
            msg = "The spectrum location appears up."
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


def get_db_regime(db, regime):
    regime_table = (
        db.query(db.Regimes).filter(db.Regimes.c.regime.ilike(regime)).table()
    )

    if len(regime_table) == 0:
        msg = (
            f"Regime not found in database: {regime}. "
            f"Add it to the Regimes table or use an existing regime.\n"
            f"Available regimes:\n {db.query(db.Regimes).table()}"
        )
        logger.warning(msg)
        return None
    elif len(regime_table) > 1:
        msg = f"Multiple entries for regime {regime} found in database. Please check the Regimes table. Matches: {regime_table}"
        logger.warning(msg)
        return None
    else:
        return regime_table["regime"][0]


# def check_publication_in_db(db, reference):
# if reference is None:
#     msg = "Reference is required."
#     if raise_error:
#         raise AstroDBError(msg)
#     else:
#         logger.warning(msg)
#         return None

# pubs_table = (
#     db.query(db.Publications)
#     .filter(db.Publications.c.reference.ilike(reference))
#     .table()
# )

# if len(pubs_table) == 0:
#     msg = f"Reference not found in database: {reference}. Add it to the Publications table."
#     raise AstroDBError(msg)
# elif len(pubs_table) > 1:
#     msg = f"Multiple entries for reference {reference} found in database. Check the Publications table. \n  Matches: \n {pubs_table}"
#     raise AstroDBError(msg)
# else:
#     return pubs_table["reference"][0]


def check_obs_date(date, raise_error=True):
    """
    Check if the observation date is in the correct format
    Parameters
    ----------
    date: str
        Observation date

    Returns
    -------
    bool
        True if the date is in the correct format, False otherwise
    """
    try:
        parsed_date = datetime.date.fromisoformat(date)
        logger.debug(
            f"Observation date {date} is valid: {parsed_date.strftime('%d %b %Y')}"
        )
        return parsed_date
    except ValueError as e:
        msg = f"Observation date {date} is not valid: {e}"
        result = None
        if raise_error:
            raise AstroDBError(msg)
        else:
            logger.warning(msg)
            return result
