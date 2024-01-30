"""
Utils functions for use in ingests
"""
from astroquery.gaia import Gaia
from astropy.table import Table
import numpy as np
import numpy.ma as ma
import pandas as pd
from sqlalchemy import and_
from astropy.io import fits
import dateutil
import re
import requests
import logging
import sqlalchemy.exc
from astrodb_scripts.utils import (
    AstroDBError,
    find_source_in_db,
    check_internet_connection,
)

__all__ = [
    "ingest_spectral_types",
    "ingest_parallaxes",
    "ingest_proper_motions",
    "ingest_photometry",
    "ingest_spectra",
]

logger = logging.getLogger("AstroDB")


# SPECTRAL TYPES
def ingest_spectral_types(
    db,
    sources,
    spectral_types,
    references,
    regimes,
    spectral_type_error=None,
    comments=None,
):
    """
    Script to ingest spectral types
    Parameters
    ----------
    db: astrodbkit2.astrodb.Database
        Database object created by astrodbkit2
    sources: str or list[str]
        Names of sources
    spectral_types: str or list[strings]
        Spectral Types of sources
    spectral_type_error: str or list[strings], optional
        Spectral Type Errors of sources
    regimes: str or list[str]
        List or string
    comments: list[strings], optional
        Comments
    references: str or list[strings]
        Reference of the Spectral Type
    Returns
    -------

    None

    """

    n_sources = len(sources)

    # Convert single element input value to list
    input_values = [
        sources,
        spectral_types,
        spectral_type_error,
        regimes,
        comments,
        references,
    ]
    for i, input_value in enumerate(input_values):
        if input_value is None:
            input_values[i] = [None] * n_sources
        elif isinstance(input_value, str):
            input_values[i] = [input_value] * n_sources
        # Convert single element input value to list
    (
        sources,
        spectral_types,
        spectral_type_error,
        regimes,
        comments,
        references,
    ) = input_values

    n_added = 0
    n_skipped = 0

    logger.info(f"Trying to add {n_sources} spectral types")

    for i, source in enumerate(sources):
        db_name = find_source_in_db(db, source)
        # Spectral Type data is in the database

        if len(db_name) != 1:
            msg = (
                f"No unique source match for {source} in the database "
                f"(with SpT: {spectral_types[i]} from {references[i]})"
            )
            raise AstroDBError(msg)
        else:
            db_name = db_name[0]

        adopted = None
        source_spt_data = (
            db.query(db.SpectralTypes)
            .filter(db.SpectralTypes.c.source == db_name)
            .table()
        )

        if source_spt_data is None or len(source_spt_data) == 0:
            adopted: True
            logger.debug("No Spectral Type data for this source in the database")
        elif len(source_spt_data) > 0:
            # Spectral Type Data already exists
            dupe_ind = source_spt_data["reference"] == references[i]
            if sum(dupe_ind):
                logger.debug(f"Duplicate measurement\n, {source_spt_data[dupe_ind]}")
            else:
                logger.debug("Another Spectral Type exists,")
                if logger.level == 10:
                    source_spt_data.pprint_all()

            adopted_ind = source_spt_data["adopted"] == 1
            if sum(adopted_ind):
                old_adopted = source_spt_data[adopted_ind]
                if spectral_type_error[i] < min(source_spt_data["spectral_type_error"]):
                    adopted = True

                    if old_adopted:
                        with db.engine.connect() as conn:
                            conn.execute(
                                db.SpectralTypes.update()
                                .where(
                                    and_(
                                        db.SpectralTypes.c.source
                                        == old_adopted["source"][0],
                                        db.SpectralTypes.c.reference
                                        == old_adopted["reference"][0],
                                    )
                                )
                                .values(adopted=False)
                            )
                            conn.commit()
                        # check that adopted flag is successfully changed
                        old_adopted_data = (
                            db.query(db.SpectralTypes)
                            .filter(
                                and_(
                                    db.SpectralTypes.c.source
                                    == old_adopted["source"][0],
                                    db.SpectralTypes.c.reference
                                    == old_adopted["reference"][0],
                                )
                            )
                            .table()
                        )
                        logger.debug("Old adopted measurement unset")
                        if logger.level == 10:
                            old_adopted_data.pprint_all()

                logger.debug(f"The new spectral type's adopted flag is:, {adopted}")
        else:
            msg = "Unexpected state"
            logger.error(msg)
            raise RuntimeError

        # Convert the spectral type string to code
        spectral_type_code = convert_spt_string_to_code(spectral_types[i])[0]
        msg = f"Converted {spectral_types[i]} to {spectral_type_code}"
        logger.debug(msg)

        # Construct the data to be added
        spt_data = [
            {
                "source": db_name,
                "spectral_type_string": spectral_types[i],
                "spectral_type_code": spectral_type_code,
                "spectral_type_error": spectral_type_error[i],
                "regime": regimes[i],
                "adopted": adopted,
                "comments": comments[i],
                "reference": references[i],
            }
        ]

        # Check if the entry already exists; if so: skip adding it
        check = (
            db.query(db.SpectralTypes.c.source)
            .filter(
                and_(
                    db.SpectralTypes.c.source == db_name,
                    db.SpectralTypes.c.regime == regimes[i],
                    db.SpectralTypes.c.reference == references[i],
                )
            )
            .count()
        )
        if check == 1:
            n_skipped += 1
            logger.info(
                f"Spectral type for {db_name} already in the database: skipping insert "
                f"{spt_data}"
            )
            continue

        logger.debug(f"Trying to insert {spt_data} into Spectral Types table ")
        try:
            with db.engine.connect() as conn:
                conn.execute(db.SpectralTypes.insert().values(spt_data))
                conn.commit()
            n_added += 1
            msg = f"Added {str(spt_data)}"
            logger.debug(msg)
        except sqlalchemy.exc.IntegrityError as e:
            if (
                db.query(db.Publications)
                .filter(db.Publications.c.reference == references[i])
                .count()
                == 0
            ):
                msg = f"The publication {references[i]} does not exist in the database"
                "Add it with ingest_publication function."
                logger.debug(msg)
                raise AstroDBError(msg)
            elif "NOT NULL constraint failed: SpectralTypes.regime" in str(e):
                msg = f"The regime was not provided for {source}"
                logger.error(msg)
                raise AstroDBError(msg)
            else:
                msg = "Other error\n"
                logger.error(msg)
                raise AstroDBError(msg)

    msg = f"Spectral types added: {n_added} \n" f"Spectral Types skipped: {n_skipped}"
    logger.info(msg)


def convert_spt_string_to_code(spectral_types):
    """
    normal tests: M0, M5.5, L0, L3.5, T0, T3, T4.5, Y0, Y5, Y9.
    weird TESTS: sdM4, ≥Y4, T5pec, L2:, L0blue, Lpec, >L9, >M10, >L, T, Y
    digits are needed in current implementation.
    :param spectral_types:
    :return:
    """
    if isinstance(spectral_types, str):
        spectral_types = [spectral_types]

    spectral_type_codes = []
    for spt in spectral_types:
        logger.debug(f"Trying to convert: `{spt}`")
        spt_code = np.nan
        if spt == "":
            spectral_type_codes.append(spt_code)
            logger.debug("Appended NAN")
            continue
        if spt == "null":
            spt_code = 0
            spectral_type_codes.append(spt_code)
            logger.debug("Appended Null")
            continue
        # identify main spectral class, loop over any prefix text to identify MLTY
        for i, item in enumerate(spt):
            if item == "M":
                spt_code = 60
                break
            elif item == "L":
                spt_code = 70
                break
            elif item == "T":
                spt_code = 80
                break
            elif item == "Y":
                spt_code = 90
                break
            else:  # only trigger if not MLTY
                i = 0
        # find integer or decimal subclass and add to spt_code
        if re.search("\d*\.?\d+", spt[i + 1 :]) is None:
            spt_code = spt_code
        else:
            spt_code += float(re.findall("\d*\.?\d+", spt[i + 1 :])[0])

        spectral_type_codes.append(spt_code)
    return spectral_type_codes


def convert_spt_code_to_string_to_code(spectral_codes, decimals=1):
    """
    Convert spectral type codes to string values

    Parameters
    ----------
    spectral_codes : list[float]
        List of spectral type codes

    Returns
    -------
    spectral_types : list[str]
        List of spectral types
    """
    if isinstance(spectral_codes, float):
        spectral_codes = [spectral_codes]

    spectral_types = []
    for spt in spectral_codes:
        spt_type = ""

        # Identify major type
        if 60 <= spt < 70:
            spt_type = "M"
        elif 70 <= spt < 80:
            spt_type = "L"
        elif 80 <= spt < 90:
            spt_type = "T"
        elif 90 <= spt < 100:
            spt_type = "Y"

        # Numeric part of type
        format = f".{decimals}f"
        spt_type = f"{spt_type}{spt % 10:{format}}"
        logger.debug(f"Converting: {spt} -> {spt_type}")

        spectral_types.append(spt_type)

    return spectral_types


# PARALLAXES
def ingest_parallaxes(db, sources, plxs, plx_errs, plx_refs, comments=None):
    """

    Parameters
    ----------
    db: astrodbkit2.astrodb.Database
        Database object
    sources: str or list[str]
        list of source names
    plxs: float or list[float]
        list of parallaxes corresponding to the sources
    plx_errs: float or list[float]
        list of parallaxes uncertainties
    plx_refs: str or list[str]
        list of references for the parallax data
    comments: Optional[Union[List[str], str]]

    Examples
    ----------
    > ingest_parallaxes(db, my_sources, my_plx, my_plx_unc, my_plx_refs)

    """

    if isinstance(sources, str):
        n_sources = 1
        sources = [sources]
    else:
        n_sources = len(sources)

    # Convert single element input value to list
    if isinstance(plx_refs, str):
        plx_refs = [plx_refs] * n_sources

    if isinstance(comments, str):
        comments = [comments] * n_sources
    elif comments is None:
        comments = [None] * n_sources

    input_float_values = [plxs, plx_errs]
    for i, input_value in enumerate(input_float_values):
        if isinstance(input_value, float):
            input_value = [input_value] * n_sources
            input_float_values[i] = input_value
    plxs, plx_errs = input_float_values

    n_added = 0

    # loop through sources with parallax data to ingest
    for i, source in enumerate(sources):
        db_name = find_source_in_db(db, source)

        if len(db_name) != 1:
            msg = f"No unique source match for {source} in the database"
            raise AstroDBError(msg)
        else:
            db_name = db_name[0]

        # Search for existing parallax data and determine if this is the best
        # If no previous measurement exists, set the new one to the Adopted measurement
        adopted = None
        source_plx_data: Table = (
            db.query(db.Parallaxes).filter(db.Parallaxes.c.source == db_name).table()
        )

        if source_plx_data is None or len(source_plx_data) == 0:
            # if there's no other measurements in the database,
            # set new data Adopted = True
            adopted = True
            # old_adopted = None  # not used
            logger.debug("No other measurement")
        elif len(source_plx_data) > 0:  # Parallax data already exists
            # check for duplicate measurement
            dupe_ind = source_plx_data["reference"] == plx_refs[i]
            if sum(dupe_ind):
                logger.debug(f"Duplicate measurement\n, {source_plx_data[dupe_ind]}")
                continue
            else:
                logger.debug("!!! Another parallax measurement exists,")
                if logger.level == 10:
                    source_plx_data.pprint_all()

            # check for previous adopted measurement and find new adopted
            adopted_ind = source_plx_data["adopted"] == 1
            if sum(adopted_ind):
                old_adopted = source_plx_data[adopted_ind]
                # if errors of new data are less than other measurements,
                # set Adopted = True.
                if plx_errs[i] < min(source_plx_data["parallax_error"]):
                    adopted = True

                    # unset old adopted
                    if old_adopted:
                        with db.engine.connect() as conn:
                            conn.execute(
                                db.Parallaxes.update()
                                .where(
                                    and_(
                                        db.Parallaxes.c.source
                                        == old_adopted["source"][0],
                                        db.Parallaxes.c.reference
                                        == old_adopted["reference"][0],
                                    )
                                )
                                .values(adopted=False)
                            )
                            conn.commit()
                        # check that adopted flag is successfully changed
                        old_adopted_data = (
                            db.query(db.Parallaxes)
                            .filter(
                                and_(
                                    db.Parallaxes.c.source == old_adopted["source"][0],
                                    db.Parallaxes.c.reference
                                    == old_adopted["reference"][0],
                                )
                            )
                            .table()
                        )
                        logger.debug("Old adopted measurement unset")
                        if logger.level == 10:
                            old_adopted_data.pprint_all()
                else:
                    adopted = False
                logger.debug(f"The new measurement's adopted flag is:, {adopted}")
        else:
            msg = "Unexpected state"
            logger.error(msg)
            raise RuntimeError(msg)

        # Construct data to be added
        parallax_data = [
            {
                "source": db_name,
                "parallax": plxs[i],
                "parallax_error": plx_errs[i],
                "reference": plx_refs[i],
                "adopted": adopted,
                "comments": comments[i],
            }
        ]

        logger.debug(f"{parallax_data}")

        try:
            with db.engine.connect() as conn:
                conn.execute(db.Parallaxes.insert().values(parallax_data))
                conn.commit()
            n_added += 1
            logger.info(f"Parallax added to database: \n " f"{parallax_data}")
        except sqlalchemy.exc.IntegrityError:
            msg = (
                "The source may not exist in Sources table.\n"
                "The parallax reference may not exist in Publications table. "
                "Add it with add_publication function. \n"
                "The parallax measurement may be a duplicate."
            )
            logger.error(msg)
            raise AstroDBError(msg)

    logger.info(f"Total Parallaxes added to database: {n_added} \n")

    return


# PROPER MOTIONS
def ingest_proper_motions(
    db, sources, pm_ras, pm_ra_errs, pm_decs, pm_dec_errs, pm_references
):
    """

    Parameters
    ----------
    db: astrodbkit2.astrodb.Database
        Database object
    sources: list[str]
        list of source names
    pm_ras: list[float]
        list of proper motions in right ascension (RA)
    pm_ra_errs: list[float]
        list of uncertanties in proper motion RA
    pm_decs: list[float]
        list of proper motions in declination (dec)
    pm_dec_errs: list[float]
        list of uncertanties in proper motion dec
    pm_references: str or list[str]
        Reference or list of references for the proper motion measurements

    Examples
    ----------
    > ingest_proper_motions(db, my_sources, my_pm_ra, my_pm_ra_unc,
                            my_pm_dec, my_pm_dec_unc, my_pm_refs,
                            verbose = True)

    """

    n_sources = len(sources)

    # Convert single element input value to list
    if isinstance(pm_references, str):
        pm_references = [pm_references] * len(sources)

    input_float_values = [pm_ras, pm_ra_errs, pm_decs, pm_dec_errs]
    for i, input_value in enumerate(input_float_values):
        if isinstance(input_value, float):
            input_value = [input_value] * n_sources
            input_float_values[i] = input_value
    pm_ras, pm_ra_errs, pm_decs, pm_dec_errs = input_float_values

    n_added = 0

    for i, source in enumerate(sources):
        db_name = find_source_in_db(db, source)

        if len(db_name) != 1:
            msg = f"No unique source match for {source} in the database"
            raise AstroDBError(msg)
        else:
            db_name = db_name[0]

        # Search for existing proper motion data and determine if this is the best
        # If no previous measurement exists, set the new one to the Adopted measurement
        # adopted = None  # not used
        source_pm_data = (
            db.query(db.ProperMotions)
            .filter(db.ProperMotions.c.source == db_name)
            .table()
        )
        if source_pm_data is None or len(source_pm_data) == 0:
            # if there's no other measurements in the database,
            # set new data Adopted = True
            adopted = True
        elif len(source_pm_data) > 0:
            # check to see if other measurement is a duplicate of the new data
            dupe_ind = source_pm_data["reference"] == pm_references[i]
            if sum(dupe_ind):
                logger.debug(f"Duplicate measurement\n, {source_pm_data}")
                continue

            # check for previous adopted measurement
            adopted_ind = source_pm_data["adopted"] == 1
            if sum(adopted_ind):
                old_adopted = source_pm_data[adopted_ind]
            else:
                old_adopted = None

            # if errors of new data are less than other measurements, set Adopted = True.
            if pm_ra_errs[i] < min(source_pm_data["mu_ra_error"]) and pm_dec_errs[
                i
            ] < min(source_pm_data["mu_dec_error"]):
                adopted = True
                # unset old adopted if it exists
                if old_adopted:
                    with db.engine.connect() as conn:
                        conn.execute(
                            db.ProperMotions.update()
                            .where(
                                and_(
                                    db.ProperMotions.c.source
                                    == old_adopted["source"][0],
                                    db.ProperMotions.c.reference
                                    == old_adopted["reference"][0],
                                )
                            )
                            .values(adopted=False)
                        )
                        conn.commit()
            else:
                adopted = False
                # if no previous adopted measurement,
                # set adopted to the measurement with the smallest errors
                if (
                    not adopted
                    and not old_adopted
                    and min(source_pm_data["mu_ra_error"]) < pm_ra_errs[i]
                    and min(source_pm_data["mu_dec_error"]) < pm_dec_errs[i]
                ):
                    adopted_pm = (
                        db.ProperMotions.update()
                        .where(
                            and_(
                                db.ProperMotions.c.source == db_name,
                                db.ProperMotions.c.mu_ra_error
                                == min(source_pm_data["mu_ra_error"]),
                                db.ProperMotions.c.mu_dec_error
                                == min(source_pm_data["mu_dec_error"]),
                            )
                        )
                        .values(adopted=True)
                    )
                    with db.engine.connect() as conn:
                        conn.execute(adopted_pm)
                        conn.commit()
            logger.debug("!!! Another Proper motion exists")
            if logger.level == 10:
                source_pm_data.pprint_all()

        else:
            msg = "Unexpected state"
            logger.error(msg)
            raise RuntimeError(msg)

        # Construct data to be added
        pm_data = [
            {
                "source": db_name,
                "mu_ra": pm_ras[i],
                "mu_ra_error": pm_ra_errs[i],
                "mu_dec": pm_decs[i],
                "mu_dec_error": pm_dec_errs[i],
                "adopted": adopted,
                "reference": pm_references[i],
            }
        ]
        logger.debug(f"Proper motion data to add: {pm_data}")

        try:
            with db.engine.connect() as conn:
                conn.execute(db.ProperMotions.insert().values(pm_data))
                conn.commit()
            n_added += 1
        except sqlalchemy.exc.IntegrityError:
            msg = (
                "The source may not exist in Sources table.\n"
                "The proper motion reference may not exist in Publications table. "
                "Add it with add_publication function. \n"
                "The proper motion measurement may be a duplicate."
            )
            logger.error(msg)
            raise AstroDBError(msg)

        updated_source_pm_data = (
            db.query(db.ProperMotions)
            .filter(db.ProperMotions.c.source == db_name)
            .table()
        )
        logger.info("Updated proper motion data:")
        if logger.level == 20:  # Info = 20, Debug = 10
            updated_source_pm_data.pprint_all()

    return


# PHOTOMETRY
def ingest_photometry(
    db,
    sources,
    bands,
    magnitudes,
    magnitude_errors,
    reference,
    telescope=None,
    epoch=None,
    comments=None,
    raise_error=True,
):
    """
    TODO: Write Docstring

    Parameters
    ----------
    db: astrodbkit2.astrodb.Database
    sources: list[str]
    bands: str or list[str]
    magnitudes: list[float]
    magnitude_errors: list[float]
    reference: str or list[str]
    telescope: str or list[str]
    epoch: list[float], optional
    comments: list[str], optional
    raise_error: bool, optional
        True (default): Raise an error if a source cannot be ingested
        False: Log a warning but skip sources which cannot be ingested

    Returns
    -------

    """

    if isinstance(sources, str):
        n_sources = 1
        sources = [sources]
    else:
        n_sources = len(sources)

    # Convert single element input values into lists
    input_values = [bands, reference, telescope]
    for i, input_value in enumerate(input_values):
        if isinstance(input_value, str):
            input_value = [input_value] * n_sources
        elif input_value is None:
            input_value = [None] * n_sources
        input_values[i] = input_value

    bands, reference, telescope = input_values

    input_float_values = [magnitudes, magnitude_errors]
    for i, input_value in enumerate(input_float_values):
        if isinstance(input_value, float):
            input_value = [input_value] * n_sources
            input_float_values[i] = input_value
    magnitudes, magnitude_errors = input_float_values

    if n_sources != len(magnitudes) or n_sources != len(magnitude_errors):
        msg = (
            f"N Sources: {len(sources)}, "
            f"N Magnitudes: {len(magnitudes)}, N Mag errors: {len(magnitude_errors)},"
            f"\nSources, magnitudes, and magnitude error lists should all be same length"
        )
        logger.error(msg)
        raise RuntimeError(msg)

    if (
        n_sources != len(reference)
        or n_sources != len(telescope)
        or n_sources != len(bands)
    ):
        msg = "All lists should be same length"
        logger.error(msg)
        raise RuntimeError(msg)

    n_added = 0

    for i, source in enumerate(sources):
        db_name = find_source_in_db(db, source)

        if len(db_name) != 1:
            msg = f"No unique source match for {source} in the database"
            raise AstroDBError(msg)
        else:
            db_name = db_name[0]

        # if the uncertainty is masked, don't ingest anything
        if isinstance(magnitude_errors[i], np.ma.core.MaskedConstant):
            mag_error = None
        else:
            mag_error = str(magnitude_errors[i])

        # Construct data to be added
        photometry_data = [
            {
                "source": db_name,
                "band": bands[i],
                "magnitude": str(
                    magnitudes[i]
                ),  # Convert to string to maintain significant digits
                "magnitude_error": mag_error,
                "telescope": telescope[i],
                "epoch": epoch,
                "comments": comments,
                "reference": reference[i],
            }
        ]
        logger.debug(f"Photometry data: {photometry_data}")

        try:
            with db.engine.connect() as conn:
                conn.execute(db.Photometry.insert().values(photometry_data))
                conn.commit()
            n_added += 1
            logger.info(f"Photometry measurement added: \n" f"{photometry_data}")
        except sqlalchemy.exc.IntegrityError as e:
            if "UNIQUE constraint failed:" in str(e):
                msg = "The measurement may be a duplicate."
                if raise_error:
                    logger.error(msg)
                    raise AstroDBError(msg)
                else:
                    logger.warning(msg)
                    continue
            else:
                msg = (
                    "The source may not exist in Sources table.\n"
                    "The reference may not exist in the Publications table. "
                    "Add it with add_publication function."
                )
                logger.error(msg)
                raise AstroDBError(msg)

    logger.info(f"Total photometry measurements added to database: {n_added} \n")

    return


# SPECTRA
def ingest_spectra(
    db,
    sources,
    spectra,
    regimes,
    telescopes,
    instruments,
    modes,
    obs_dates,
    references,
    original_spectra=None,
    wavelength_units=None,
    flux_units=None,
    wavelength_order=None,
    comments=None,
    other_references=None,
    raise_error=True,
):
    """

    Parameters
    ----------
    db: astrodbkit2.astrodb.Database
    sources: list[str]
        List of source names
    spectra: list[str]
        List of filenames corresponding to spectra files
    regimes: str or list[str]
        List or string
    telescopes: str or list[str]
        List or string
    instruments: str or list[str]
        List or string
    modes: str or list[str]
        List or string
    obs_dates: str or datetime
        List of strings or datetime objects
    references: list[str]
        List or string
    original_spectra: list[str]
        List of filenames corresponding to original spectra files
    wavelength_units: str or list[str] or Quantity, optional
        List or string
    flux_units: str or list[str] or Quantity, optional
        List or string
    wavelength_order: list[int], optional
    comments: list[str], optional
        List of strings
    other_references: list[str], optional
        List of strings
    raise_error: bool

    """

    # Convert single value input values to lists
    if isinstance(sources, str):
        sources = [sources]

    if isinstance(spectra, str):
        spectra = [spectra]

    input_values = [
        regimes,
        telescopes,
        instruments,
        modes,
        obs_dates,
        wavelength_order,
        wavelength_units,
        flux_units,
        references,
        comments,
        other_references,
    ]
    for i, input_value in enumerate(input_values):
        if isinstance(input_value, str):
            input_values[i] = [input_value] * len(sources)
        elif isinstance(input_value, type(None)):
            input_values[i] = [None] * len(sources)
    (
        regimes,
        telescopes,
        instruments,
        modes,
        obs_dates,
        wavelength_order,
        wavelength_units,
        flux_units,
        references,
        comments,
        other_references,
    ) = input_values

    n_spectra = len(spectra)
    n_skipped = 0
    n_dupes = 0
    n_missing_instrument = 0
    n_added = 0
    n_blank = 0

    msg = f"Trying to add {n_spectra} spectra"
    logger.info(msg)

    for i, source in enumerate(sources):
        # TODO: check that spectrum can be read by astrodbkit

        # Get source name as it appears in the database
        db_name = find_source_in_db(db, source)

        if len(db_name) != 1:
            msg = f"No unique source match for {source} in the database"
            raise AstroDBError(msg)
        else:
            db_name = db_name[0]

        # Check if spectrum file is accessible
        # First check for internet
        internet = check_internet_connection()
        if internet:
            request_response = requests.head(spectra[i])
            status_code = (
                request_response.status_code
            )  # The website is up if the status code is 200
            if status_code != 200:
                n_skipped += 1
                msg = (
                    "The spectrum location does not appear to be valid: \n"
                    f"spectrum: {spectra[i]} \n"
                    f"status code: {status_code}"
                )
                logger.error(msg)
                if raise_error:
                    raise AstroDBError(msg)
                else:
                    continue
            else:
                msg = f"The spectrum location appears up: {spectra[i]}"
                logger.debug(msg)
            if original_spectra:
                request_response1 = requests.head(original_spectra[i])
                status_code1 = request_response1.status_code
                if status_code1 != 200:
                    n_skipped += 1
                    msg = (
                        "The spectrum location does not appear to be valid: \n"
                        f"spectrum: {original_spectra[i]} \n"
                        f"status code: {status_code1}"
                    )
                    logger.error(msg)
                    if raise_error:
                        raise AstroDBError(msg)
                    else:
                        continue
                else:
                    msg = f"The spectrum location appears up: {original_spectra[i]}"
                    logger.debug(msg)
        else:
            msg = "No internet connection. Internet is needed to check spectrum files."
            raise AstroDBError(msg)

        # Find what spectra already exists in database for this source
        source_spec_data = (
            db.query(db.Spectra).filter(db.Spectra.c.source == db_name).table()
        )

        # SKIP if observation date is blank
        # TODO: try to populate obs date from meta data in spectrum file
        if ma.is_masked(obs_dates[i]) or obs_dates[i] == "":
            obs_date = None
            missing_obs_msg = (
                f"Skipping spectrum with missing observation date: {source} \n"
            )
            missing_row_spe = f"{source, obs_dates[i], references[i]} \n"
            logger.info(missing_obs_msg)
            logger.debug(missing_row_spe)
            n_blank += 1
            continue
        else:
            try:
                obs_date = pd.to_datetime(
                    obs_dates[i]
                )  # TODO: Another method that doesn't require pandas?
            except ValueError:
                n_skipped += 1
                if raise_error:
                    msg = f"{source}: Can't convert obs date to Date Time object: "
                    "{obs_dates[i]}"
                    logger.error(msg)
                    raise AstroDBError
            except dateutil.parser._parser.ParserError:
                n_skipped += 1
                if raise_error:
                    msg = f"{source}: Can't convert obs date to "
                    f"Date Time object: {obs_dates[i]}"
                    logger.error(msg)
                    raise AstroDBError
                else:
                    msg = f"Skipping {source} Can't convert obs date to "
                    f"Date Time object: {obs_dates[i]}"
                    logger.warning(msg)
                continue

        # TODO: make it possible to ingest units and order
        row_data = [
            {
                "source": db_name,
                "spectrum": spectra[i],
                "original_spectrum": None,  # if ma.is_masked(original_spectra[i]) or
                # isinstance(original_spectra,None)
                # else original_spectra[i],
                "local_spectrum": None,  # if ma.is_masked(local_spectra[i])
                # else local_spectra[i],
                "regime": regimes[i],
                "telescope": telescopes[i],
                "instrument": None if ma.is_masked(instruments[i]) else instruments[i],
                "mode": None if ma.is_masked(modes[i]) else modes[i],
                "observation_date": obs_date,
                "wavelength_units": None
                if ma.is_masked(wavelength_units[i])
                else wavelength_units[i],
                "flux_units": None if ma.is_masked(flux_units[i]) else flux_units[i],
                "wavelength_order": None
                if ma.is_masked(wavelength_order[i])
                else wavelength_order[i],
                "comments": None if ma.is_masked(comments[i]) else comments[i],
                "reference": references[i],
                "other_references": None
                if ma.is_masked(other_references[i])
                else other_references[i],
            }
        ]
        logger.debug(row_data)

        try:
            with db.engine.connect() as conn:
                conn.execute(db.Spectra.insert().values(row_data))
                conn.commit()
            n_added += 1
        except sqlalchemy.exc.IntegrityError as e:
            if "CHECK constraint failed: regime" in str(e):
                msg = f"Regime provided is not in schema: {regimes[i]}"
                logger.error(msg)
                if raise_error:
                    raise AstroDBError(msg)
                else:
                    continue
            if (
                db.query(db.Publications)
                .filter(db.Publications.c.publication == references[i])
                .count()
                == 0
            ):
                msg = (
                    f"Spectrum for {source} could not be added to the database "
                    "because the reference {references[i]} is not in "
                    "Publications table. \n"
                    f"(Add it with ingest_publication function.) \n "
                )
                logger.warning(msg)
                if raise_error:
                    raise AstroDBError(msg)
                else:
                    continue
                # check telescope, instrument, mode exists
            telescope = (
                db.query(db.Telescopes)
                .filter(db.Telescopes.c.name == row_data[0]["telescope"])
                .table()
            )
            instrument = (
                db.query(db.Instruments)
                .filter(db.Instruments.c.name == row_data[0]["instrument"])
                .table()
            )
            mode = (
                db.query(db.Modes)
                .filter(db.Modes.c.name == row_data[0]["mode"])
                .table()
            )

            if len(source_spec_data) > 0:  # Spectra data already exists
                # check for duplicate measurement
                ref_dupe_ind = source_spec_data["reference"] == references[i]
                date_dupe_ind = source_spec_data["observation_date"] == obs_date
                instrument_dupe_ind = source_spec_data["instrument"] == instruments[i]
                mode_dupe_ind = source_spec_data["mode"] == modes[i]
                if (
                    sum(ref_dupe_ind)
                    and sum(date_dupe_ind)
                    and sum(instrument_dupe_ind)
                    and sum(mode_dupe_ind)
                ):
                    msg = f"Skipping suspected duplicate measurement\n{source}\n"
                    msg2 = f"{source_spec_data[ref_dupe_ind]['source', 'instrument', 'mode', 'observation_date', 'reference']}"
                    msg3 = f"{instruments[i], modes[i]} "
                    f"{obs_date, references[i], spectra[i]} \n"
                    logger.warning(msg)
                    logger.debug(msg2 + msg3 + str(e))
                    n_dupes += 1
                    if raise_error:
                        raise AstroDBError
                    else:
                        continue  # Skip duplicate measurement
                # else:
                #     msg = f'Spectrum could not be added to the database
                #    "(other data exist): \n ' \
                #           f"{source, instruments[i], modes[i], obs_date, references[i],
                #   "spectra[i]} \n"
                #     msg2 = f"Existing Data: \n "
                #            # f"{source_spec_data[ref_dupe_ind]['source', 'instrument',
                #     'mode', 'observation_date', 'reference', 'spectrum']}"
                #     msg3 = f"Data not able to add: \n {row_data} \n "
                #     logger.warning(msg + msg2)
                #     source_spec_data[ref_dupe_ind][
                #               'source', 'instrument', 'mode', 'observation_date',
                #    'reference', 'spectrum'].pprint_all()
                #     logger.debug(msg3)
                #     n_skipped += 1
                #     continue
            if len(instrument) == 0 or len(mode) == 0 or len(telescope) == 0:
                msg = (
                    f"Spectrum for {source} could not be added to the database. \n"
                    f" Telescope, Instrument, and/or Mode need to be added "
                    "to the appropriate table. \n"
                    f" Trying to find telescope: {row_data[0]['telescope']} "
                    f"instrument: {row_data[0]['instrument']}, "
                    f" mode: {row_data[0]['mode']} \n"
                    f" Telescope: {telescope}, Instrument: {instrument}, Mode: {mode} \n"
                )
                logger.error(msg)
                n_missing_instrument += 1
                if raise_error:
                    raise AstroDBError
                else:
                    continue
            else:
                msg = f"Spectrum for {source} could not be added to the database "
                "for unknown reason: \n {row_data} \n "
                logger.error(msg)
                raise AstroDBError(msg)

    msg = (
        f"SPECTRA ADDED: {n_added} \n"
        f" Spectra with blank obs_date: {n_blank} \n"
        f" Suspected duplicates skipped: {n_dupes}\n"
        f" Missing Telescope/Instrument/Mode: {n_missing_instrument} \n"
        f" Spectra skipped for unknown reason: {n_skipped} \n"
    )
    if n_spectra == 1:
        logger.info(f"Added {source} : \n" f"{row_data}")
    else:
        logger.info(msg)

    if n_added + n_dupes + n_blank + n_skipped + n_missing_instrument != n_spectra:
        msg = "Numbers don't add up: "
        logger.error(msg)
        raise AstroDBError(msg)

    # spec_count = (
    #     db.query(Spectra.regime, func.count(Spectra.regime))
    #     .group_by(Spectra.regime)
    #     .all()
    # )

    # spec_ref_count = (
    #     db.query(Spectra.reference, func.count(Spectra.reference))
    #     .group_by(Spectra.reference)
    #     .order_by(func.count(Spectra.reference).desc())
    #     .limit(20)
    #     .all()
    # )

    # telescope_spec_count = (
    #     db.query(Spectra.telescope, func.count(Spectra.telescope))
    #     .group_by(Spectra.telescope)
    #     .order_by(func.count(Spectra.telescope).desc())
    #     .limit(20)
    #     .all()
    # )

    # logger.info(f'Spectra in the database: \n {spec_count}
    # \n {spec_ref_count} \n {telescope_spec_count}')

    return


def get_gaiadr3(gaia_id, verbose=True):
    """
    Currently setup just to query one source
    TODO: add some debug and info messages

    Parameters
    ----------
    gaia_id: str or int
    verbose

    Returns
    -------
    Table of Gaia data

    """
    gaia_query_string = (
        f"SELECT "
        f"parallax, parallax_error, "
        f"pmra, pmra_error, pmdec, pmdec_error, "
        f"phot_g_mean_flux, phot_g_mean_flux_error, phot_g_mean_mag, "
        f"phot_rp_mean_flux, phot_rp_mean_flux_error, phot_rp_mean_mag "
        f"FROM gaiadr3.gaia_source WHERE "
        f"gaiadr3.gaia_source.source_id = '{gaia_id}'"
    )
    job_gaia_query = Gaia.launch_job(gaia_query_string, verbose=verbose)

    gaia_data = job_gaia_query.get_results()

    return gaia_data


def ingest_gaia_photometry(db, sources, gaia_data, ref):
    # TODO write some tests
    unmasked_gphot = np.logical_not(gaia_data["phot_g_mean_mag"].mask).nonzero()
    gaia_g_phot = gaia_data[unmasked_gphot][
        "phot_g_mean_flux", "phot_g_mean_flux_error", "phot_g_mean_mag"
    ]

    unmased_rpphot = np.logical_not(gaia_data["phot_rp_mean_mag"].mask).nonzero()
    gaia_rp_phot = gaia_data[unmased_rpphot][
        "phot_rp_mean_flux", "phot_rp_mean_flux_error", "phot_rp_mean_mag"
    ]

    # e_Gmag=abs(-2.5/ln(10)*e_FG/FG) from Vizier Note 37 on Gaia DR2 (I/345/gaia2)
    gaia_g_phot["g_unc"] = np.abs(
        -2.5
        / np.log(10)
        * gaia_g_phot["phot_g_mean_flux_error"]
        / gaia_g_phot["phot_g_mean_flux"]
    )
    gaia_rp_phot["rp_unc"] = np.abs(
        -2.5
        / np.log(10)
        * gaia_rp_phot["phot_rp_mean_flux_error"]
        / gaia_rp_phot["phot_rp_mean_flux"]
    )

    if ref == "GaiaDR2":
        g_band_name = "GAIA2.G"
        rp_band_name = "GAIA2.Grp"
    elif ref == "GaiaEDR3" or ref == "GaiaDR3":
        g_band_name = "GAIA3.G"
        rp_band_name = "GAIA3.Grp"
    else:
        raise Exception

    ingest_photometry(
        db,
        sources,
        g_band_name,
        gaia_g_phot["phot_g_mean_mag"],
        gaia_g_phot["g_unc"],
        ref,
        ucds="em.opt",
        telescope="Gaia",
        instrument="Gaia",
    )

    ingest_photometry(
        db,
        sources,
        rp_band_name,
        gaia_rp_phot["phot_rp_mean_mag"],
        gaia_rp_phot["rp_unc"],
        ref,
        ucds="em.opt.R",
        telescope="Gaia",
        instrument="Gaia",
    )

    return


def ingest_gaia_parallaxes(db, sources, gaia_data, ref):
    # TODO write some tests
    unmasked_pi = np.logical_not(gaia_data["parallax"].mask).nonzero()
    gaia_parallaxes = gaia_data[unmasked_pi]["parallax", "parallax_error"]

    ingest_parallaxes(
        db, sources, gaia_parallaxes["parallax"], gaia_parallaxes["parallax_error"], ref
    )


def ingest_gaia_pms(db, sources, gaia_data, ref):
    # TODO write some tests
    unmasked_pms = np.logical_not(gaia_data["pmra"].mask).nonzero()
    pms = gaia_data[unmasked_pms]["pmra", "pmra_error", "pmdec", "pmdec_error"]
    refs = [ref] * len(pms)

    ingest_proper_motions(
        db,
        sources,
        pms["pmra"],
        pms["pmra_error"],
        pms["pmdec"],
        pms["pmdec_error"],
        refs,
    )


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

    ingest_spectra(
        db,
        source,
        spectrum_fits_file,
        regime,
        telescope,
        instrument,
        None,
        obs_date,
        reference,
        wavelength_units=w_unit,
        flux_units=flux_unit,
    )


# COMPANION RELATIONSHIP
def ingest_companion_relationships(
    db,
    source,
    companion_name,
    relationship,
    projected_separation_arcsec=None,
    projected_separation_error=None,
    comment=None,
    ref=None,
    other_companion_names=None,
):
    """
    This function ingests a single row in to the CompanionRelationship table

    Parameters
    ----------
    db: astrodbkit2.astrodb.Database
        Database object created by astrodbkit2
    source: str
        Name of source as it appears in sources table
    relationship: str
        relationship is of the souce to its companion
        should be one of the following: Child, Sibling, Parent, or Unresolved Parent
        see note
    companion_name: str
        SIMBAD resovable name of companion object
    projected_separation_arcsec: float (optional)
        Projected separtaion should be recorded in arc sec
    projected_separation_error: float (optional)
        Projected separtaion should be recorded in arc sec
    references: str (optional)
        Discovery references of sources
    comments: str (optional)
        Comments
    other_companion_names: comma separated names (optional)
        other names used to identify the companion
        ex:  'HD 89744, NLTT 24128, GJ 9326'

    Returns
    -------
    None

    Note: Relationships are constrained to one of the following:
    - *Child*: The source is lower mass/fainter than the companion
    - *Sibling*: The source is similar to the companion
    - *Parent*: The source is higher mass/brighter than the companion
    - *Unresolved Parent*: The source is the unresolved,
        combined light source of an unresolved
        multiple system which includes the companion

    """
    # checking relationship entered
    possible_relationships = ["Child", "Sibling", "Parent", "Unresolved Parent", None]
    # check captialization
    if relationship.title() != relationship:
        logger.info(
            f"Relationship captilization changed from "
            f"{relationship} to {relationship.title()} "
        )
        relationship = relationship.title()
    if relationship not in possible_relationships:
        msg = f"Relationship given for {source}, {companion_name}: {relationship} "
        f"NOT one of the constrained relationships \n {possible_relationships}"
        logger.error(msg)
        raise AstroDBError(msg)

    # source canot be same as companion
    if source == companion_name:
        msg = f"{source}: Source cannot be the same as companion name"
        logger.error(msg)
        raise AstroDBError(msg)

    if source == companion_name:
        msg = f"{source}: Source cannot be the same as companion name"
        logger.error(msg)
        raise AstroDBError(msg)

    if projected_separation_arcsec is not None and projected_separation_arcsec < 0:
        msg = f"Projected separation: {projected_separation_arcsec}, cannot be negative"
        logger.error(msg)
        raise AstroDBError(msg)
    if projected_separation_error is not None and projected_separation_error < 0:
        msg = (
            f"Projected separation error: {projected_separation_error}, "
            "cannot be negative"
        )
        logger.error(msg)
        raise AstroDBError(msg)

    # check other names
    # make sure companion name is included in the list
    if other_companion_names is None:
        other_companion_names = companion_name
    else:
        companion_name_list = other_companion_names.split(", ")
        if companion_name not in companion_name_list:
            companion_name_list.append(companion_name)
        other_companion_names = (",  ").join(companion_name_list)

    try:
        with db.engine.connect() as conn:
            conn.execute(
                db.CompanionRelationships.insert().values(
                    {
                        "source": source,
                        "companion_name": companion_name,
                        "projected_separation_arcsec": projected_separation_arcsec,
                        "projected_separation_error": projected_separation_error,
                        "relationship": relationship,
                        "reference": ref,
                        "comments": comment,
                        "other_companion_names": other_companion_names,
                    }
                )
            )
            conn.commit()
        logger.info(
            "ComapnionRelationship added: ",
            [
                source,
                companion_name,
                relationship,
                projected_separation_arcsec,
                projected_separation_error,
                comment,
                ref,
            ],
        )
    except sqlalchemy.exc.IntegrityError as e:
        if "UNIQUE constraint failed:" in str(e):
            msg = "The companion may be a duplicate."
            logger.error(msg)
            raise AstroDBError(msg)

        else:
            msg = (
                "Make sure all require parameters are provided. \\"
                "Other possible errors: source may not exist in Sources table \\"
                "or the reference may not exist in the Publications table. "
            )
            logger.error(msg)
            raise AstroDBError(msg)
