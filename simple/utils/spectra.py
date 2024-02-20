import logging
import requests
import numpy.ma as ma
import pandas as pd  # used for to_datetime conversion
import dateutil  # used to convert obs date to datetime object
import sqlalchemy.exc
import numpy as np
from typing import Optional
import matplotlib.pyplot as plt

from astropy.io import fits
import astropy.units as u
from specutils import Spectrum1D

from astrodbkit2.astrodb import Database
from astrodb_scripts import (
    AstroDBError,
    find_source_in_db,
    check_internet_connection,
    find_publication,
)

__all__ = [
    "ingest_spectra",
    "ingest_spectrum",
    "ingest_spectrum_from_fits",
    "spectrum_plottable",
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
        Telescope used to obtain spectrum

    Returns
    -------
    flags: dict

    Raises
    ------
    AstroDBError
    """

    flags = {
        "skipped": False,
        "dupe": False,
        "missing_instrument": False,
        "no_obs_date": False,
        "added": False,
        "plottable": False,
    }

    # Check input values

    if regime is None:
        msg = "Regime is required"
        logger.error(msg)
        flags["skipped"] = True
        if raise_error:
            raise AstroDBError(msg)
        else:
            return flags

    if reference is None:
        msg = "Reference is required"
        logger.error(msg)
        flags["skipped"] = True
        if raise_error:
            raise AstroDBError(msg)
        else:
            return flags
    else:
        good_reference = find_publication(db, reference)
        if good_reference[0] is False:
            msg = (
                f"Spectrum for {source} could not be added to the database because the "
                f"reference {reference} is not in Publications table. \n"
                f"(Add it with ingest_publication function.) \n "
            )
            logger.error(msg)
            flags["skipped"] = True
            if raise_error:
                raise AstroDBError(msg)
            else:
                return flags

    # Get source name as it appears in the database
    db_name = find_source_in_db(db, source)

    if len(db_name) != 1:
        msg = f"No unique source match for {source} in the database"
        flags["skipped"] = True
        if raise_error:
            raise AstroDBError(msg)
        else:
            return flags
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
            flags["skipped"] = True
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
                flags["skipped"] = True
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
    if ma.is_masked(obs_date) or obs_date == "" or obs_date is None:
        obs_date = None
        missing_obs_msg = (
            f"Skipping spectrum with missing observation date: {source} \n"
        )
        missing_row_spe = f"{source, obs_date, reference} \n"
        flags["no_obs_date"] = True
        logger.debug(missing_row_spe)
        if raise_error:
            logger.error(missing_obs_msg)
            raise AstroDBError(missing_obs_msg)
        else:
            logger.warning(missing_obs_msg)
    else:
        try:
            obs_date = pd.to_datetime(
                obs_date
            )  # TODO: Another method that doesn't require pandas?
        except ValueError:
            flags["skipped"] = True
            if raise_error:
                msg = (
                    f"{source}: Can't convert obs date to Date Time object: {obs_date}"
                )
                logger.error(msg)
                raise AstroDBError(msg)
        except dateutil.parser._parser.ParserError:
            flags["skipped"] = True
            if raise_error:
                msg = (
                    f"{source}: Can't convert obs date to Date Time object: {obs_date}"
                )
                logger.error(msg)
                raise AstroDBError(msg)
            else:
                msg = (
                    f"Skipping {source} Can't convert obs date to Date Time object: "
                    f"{obs_date}"
                )
                logger.warning(msg)

    # Check if spectrum is plottable
    flags["plottable"] = spectrum_plottable(spectrum, raise_error=raise_error)

    row_data = {
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

    logger.debug(row_data)

    try:
        with db.engine.connect() as conn:
            conn.execute(db.Spectra.insert().values(row_data))
            conn.commit()
        flags["added"] = True
        logger.info(f"Added {source} : \n" f"{row_data}")
    except sqlalchemy.exc.IntegrityError as e:
        if "CHECK constraint failed: regime" in str(e):
            msg = f"Regime provided is not in schema: {regime}"
            logger.error(msg)
            flags["skipped"] = True
            if raise_error:
                raise AstroDBError(msg)
            # check telescope, instrument, mode exists
        telescope = (
            db.query(db.Telescopes)
            .filter(db.Telescopes.c.telescope == row_data["telescope"])
            .table()
        )
        instrument = (
            db.query(db.Instruments)
            .filter(db.Instruments.c.instrument == row_data["instrument"])
            .table()
        )

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
                flags["dupe"] = True
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
                f" Trying to find telescope: {row_data['telescope']},"
                f" instrument: {row_data['instrument']}, "
                f" mode: {row_data['mode']} \n"
                f" Telescope: {telescope}, Instrument: {instrument}, Mode: {mode} \n"
            )
            logger.error(msg)
            flags["missing_instrument"] = True
            if raise_error:
                raise AstroDBError
        else:
            msg = f"Spectrum for {source} could not be added to the database"
            "for unknown reason: \n {row_data} \n "
            logger.error(msg)
            raise AstroDBError(msg)

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
            e + f"Skipping {spectrum_path}: unable to convert spectral axis to microns"
        )
        if raise_error:
            logger.error(msg)
            raise AstroDBError(msg)
        else:
            logger.warning(msg)
            return False
    except ValueError as e:
        msg = e + f"Skipping {spectrum_path}: Value error"
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
