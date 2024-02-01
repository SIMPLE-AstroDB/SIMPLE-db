import logging
import requests
import numpy.ma as ma
import pandas as pd  # used for to_datetime conversion
import dateutil  # used to convert obs date to datetime object
import sqlalchemy.exc
import numpy as np

from astropy.io import fits
import astropy.units as u

from astrodbkit2.spectra import load_spectrum
from astrodb_scripts import (
    AstroDBError,
    find_source_in_db,
    check_internet_connection,
)

__all__ = ["ingest_spectra", "ingest_spectrum", "ingest_spectrum_from_fits"]


logger = logging.getLogger("AstroDB")


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

        # if ma.is_masked(original_spectra[i]) or isinstance(original_spectra,None)
        # else original_spectra[i],
        # if ma.is_masked(local_spectra[i]) else local_spectra[i],
        # if ma.is_masked(instruments[i]) else instruments[i],
        # if ma.is_masked(modes[i]) else modes[i]
        # if ma.is_masked(comments[i]) else comments[i]
        # if ma.is_masked(other_references[i]) else other_references[i]

        ingest_spectrum(
            db,
            source,
            spectra[i],
            regimes[i],
            telescopes[i],
            instruments[i],
            modes[i],
            obs_dates[i],
            references[i],
            original_spectrum=original_spectra[i],
            raise_error=raise_error,
        )

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


def ingest_spectrum(
    db,
    source,
    spectrum,
    regime,
    telescope,
    instrument,
    mode,
    obs_date,
    reference,
    original_spectrum=None,
    comments=None,
    other_references=None,
    local_spectrum=None,
    raise_error=True,
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
        Regime of spectrum (optical, NIR, radio, etc.)
    telescope: str
        Telescope used to obtain spectrum
    """

    # Get source name as it appears in the database
    db_name = find_source_in_db(db, source)
    skipped = False
    dupe = False
    missing_instrument = False
    no_obs_date = False
    added = False

    if len(db_name) != 1:
        msg = f"No unique source match for {source} in the database"
        raise AstroDBError(msg)
    else:
        db_name = db_name[0]

    # Check if spectrum file is accessible
    # First check for internet
    internet = check_internet_connection()
    if internet:
        request_response = requests.head(spectrum)
        status_code = (
            request_response.status_code
        )  # The website is up if the status code is 200
        if status_code != 200:
            skipped = True
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
                skipped = True
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

    # SKIP if observation date is blank
    if ma.is_masked(obs_date) or obs_date == "":
        obs_date = None
        missing_obs_msg = (
            f"Skipping spectrum with missing observation date: {source} \n"
        )
        missing_row_spe = f"{source, obs_date, reference} \n"
        logger.info(missing_obs_msg)
        logger.debug(missing_row_spe)
        no_obs_date = True
    else:
        try:
            obs_date = pd.to_datetime(
                obs_date
            )  # TODO: Another method that doesn't require pandas?
        except ValueError:
            skipped = True
            if raise_error:
                msg = (
                    f"{source}: Can't convert obs date to Date Time object: {obs_date}"
                )
                logger.error(msg)
                raise AstroDBError
        except dateutil.parser._parser.ParserError:
            skipped = True
            if raise_error:
                msg = (
                    f"{source}: Can't convert obs date to Date Time object: {obs_date}"
                )
                logger.error(msg)
                raise AstroDBError
            else:
                msg = (
                    f"Skipping {source} Can't convert obs date to Date Time object: "
                    f"{obs_date}"
                )
                logger.warning(msg)

    ######################################################################################
    # Check if spectrum is plottable
    ######################################################################################

    # load the spectrum and make sure it's a Spectrum1D object
    # spectrum: Spectrum1D = spec['spectrum']
    spectrum = load_spectrum(spectrum)

    # checking spectrum has good units and not only NaNs
    try:
        wave: np.ndarray = spectrum.spectral_axis.to(u.micron).value
        flux: np.ndarray = spectrum.flux.value
        nan_check: np.ndarray = ~np.isnan(flux) & ~np.isnan(wave)
        wave = wave[nan_check]
        flux = flux[nan_check]
        if not len(wave):
            raise ValueError

    # handle any objects which failed checks
    except (u.UnitConversionError, AttributeError, ValueError):
        skipped = True
        msg = f"Skipping {source}: spectrum is not plottable"
        if raise_error:
            logger.error(msg)
            raise AstroDBError(msg)
        else:
            logger.warning(msg)

    row_data = [
        {
            "source": db_name,
            "spectrum": spectrum,
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
    ]
    logger.debug(row_data)

    try:
        with db.engine.connect() as conn:
            conn.execute(db.Spectra.insert().values(row_data))
            conn.commit()
        added = True
    except sqlalchemy.exc.IntegrityError as e:
        if "CHECK constraint failed: regime" in str(e):
            msg = f"Regime provided is not in schema: {regime}"
            logger.error(msg)
            if raise_error:
                raise AstroDBError(msg)
        if (
            db.query(db.Publications)
            .filter(db.Publications.c.publication == reference)
            .count()
            == 0
        ):
            msg = (
                f"Spectrum for {source} could not be added to the database because the "
                f"reference {reference} is not in Publications table. \n"
                f"(Add it with ingest_publication function.) \n "
            )
            logger.warning(msg)
            if raise_error:
                raise AstroDBError(msg)
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
        mode = db.query(db.Modes).filter(db.Modes.c.name == row_data[0]["mode"]).table()

        #################################################################################
        # Find what spectra already exists in database for this source
        #################################################################################
        source_spec_data = (
            db.query(db.Spectra).filter(db.Spectra.c.source == db_name).table()
        )

        if len(source_spec_data) > 0:  # Spectra data already exists
            # check for duplicate measurement
            ref_dupe_ind = source_spec_data["reference"] == reference
            date_dupe_ind = source_spec_data["observation_date"] == obs_date
            instrument_dupe_ind = source_spec_data["instrument"] == instrument
            mode_dupe_ind = source_spec_data["mode"] == mode
            if (
                sum(ref_dupe_ind)
                and sum(date_dupe_ind)
                and sum(instrument_dupe_ind)
                and sum(mode_dupe_ind)
            ):
                msg = f"Skipping suspected duplicate measurement\n{source}\n"
                msg2 = (
                    f"{source_spec_data[ref_dupe_ind]}"
                    f"{instrument, mode, obs_date, reference, spectrum} \n"
                )
                logger.warning(msg)
                logger.debug(msg2 + str(e))
                dupe = True
                if raise_error:
                    raise AstroDBError
            # else:
            #     msg = (
            # f'Spectrum could not be added to the database
            # (other data exist): \n ' \
            #   f"{source, instruments[i], modes[i], obs_date, references[i],
            # spectra[i]} \n"
            #     msg2 = f"Existing Data: \n "
            #            # f"{source_spec_data[ref_dupe_ind]['source', 'instrument',
            # 'mode', 'observation_date', 'reference', 'spectrum']}"
            #     msg3 = f"Data not able to add: \n {row_data} \n "
            #     logger.warning(msg + msg2)
            #     source_spec_data[ref_dupe_ind][
            #               'source', 'instrument', 'mode', 'observation_date',
            # 'reference', 'spectrum'].pprint_all()
            #     logger.debug(msg3)
            #     n_skipped += 1
            #     continue
        if len(instrument) == 0 or len(mode) == 0 or len(telescope) == 0:
            msg = (
                f"Spectrum for {source} could not be added to the database. \n"
                f" Telescope, Instrument, and/or Mode need to be added to the"
                " appropriate table. \n"
                f" Trying to find telescope: {row_data[0]['telescope']},"
                f" instrument: {row_data[0]['instrument']}, "
                f" mode: {row_data[0]['mode']} \n"
                f" Telescope: {telescope}, Instrument: {instrument}, Mode: {mode} \n"
            )
            logger.error(msg)
            missing_instrument = True
            if raise_error:
                raise AstroDBError
        else:
            msg = f"Spectrum for {source} could not be added to the database"
            "for unknown reason: \n {row_data} \n "
            logger.error(msg)
            raise AstroDBError(msg)

    return [added, skipped, dupe, missing_instrument, no_obs_date]


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
        None,
        obs_date,
        reference,
        wavelength_units=w_unit,
        flux_units=flux_unit,
    )
