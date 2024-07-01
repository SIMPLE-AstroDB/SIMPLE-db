import numpy as np
import logging
# from pandas import to_datetime
from astropy.time import Time
from datetime import date
import astropy.io.fits as fits
from astropy.table import Table
import astropy.units as u
import dateparser
from astrodb_utils.photometry import assign_ucd

logger = logging.getLogger("SIMPLE")


def compile_header(wavelength_data, **original_header_dict):
    """Creates a header from a dictionary of values.
    TODO: Check that RA and Dec are in degrees

    wavelength_data: an array of wavelengths

    original_header_dict: a dictionary of values to be included in the header

    Returns: a fits header dictionary

    """

    required_keywords = [
        "VOCLASS",
        "VOPUB",
        "RA",
        "dec",
        "bandpass",
        "aperture",
        "object_name",
        "bibcode",
        "instrument",
        "obs_date",
        "title",
        "author",
        "doi",
        "telescope",
        "history",
        "comment",
        "observatory",
    ]

    # keywords_given = list(original_header_dict.keys())

    # for key in keywords_given:
    #    if key not in required_keywords:
    #        raise Exception(f"Not expecting keyword: {key}. Add manually.")
    #    else:
    #        pass

    history = original_header_dict["history"]

    comment = "To read this file in with specutils use Spectrum1D.read() with format = 'tabular-fits'"

    header = fits.Header()
    header.set("EXTNAME", "PRIMARY", "name of this extension")

    # try:
    #    header.set("VOCLASS", original_header_dict["voclass"], "VO Data Model")
    # except KeyError:
    #    logging.warning("No VO Data Model")

    # REQUIRED Target Data
    try:
        header.set(
            "OBJECT", original_header_dict["object_name"], "Name of observed object"
        )
    except KeyError:
        object_name = input("REQUIRED: Please input a name for the object: ")
        header.set("OBJECT", object_name, "Name of observed object")

    try:
        telescope = original_header_dict["telescope"]
        header.set("TELESCOP", telescope, "name of telescope")
    except KeyError:
        telescope = input("REQURIED: Please input the name of the telescope: ")
        header.set("TELESCOP", telescope, "name of telescope")

    try:
        instrument = original_header_dict["instrument"]
        header.set("INSTRUME", instrument, "name of instrument")
    except KeyError:
        instrument = input("REQUIRED: Please input the name of the instrument: ")
        header.set("INSTRUME", instrument, "name of instrument")

    try:
        ra = original_header_dict["RA"]
        header.set("RA_OBJ", ra, "[deg] Right Ascension of object")
    except KeyError:
        ra = input(
            "REQUIRED: Please input the right ascension of the object in degrees: "
        )
        header.set("RA_OBJ", ra, "[deg] Right Ascension of object")

    try:
        dec = original_header_dict["dec"]
        header.set("DEC_OBJ", dec, "[deg] Declination of object")
    except KeyError:
        dec = input("REQUIRED: Please input the declination of the object in degrees: ")
        header.set("DEC_OBJ", dec, "[deg] Declination of object")

    try:
        obs_date = dateparser.parse(original_header_dict["obs_date"]).strftime(
            "%Y-%m-%d"
        )
        header.set("DATE-OBS", obs_date, "date of the observation")
    except KeyError:
        obs_date = input("REQUIRED: Please input the date of the observation: ")
        obs_date = dateparser.parse(obs_date).strftime("%Y-%m-%d")
        header.set("DATE-OBS", obs_date, "date of the observation")

    # Wavelength info
    w_units = wavelength_data.unit
    w_min = min(wavelength_data).astype(np.single)
    w_max = max(wavelength_data).astype(np.single)
    width = (w_max - w_min).astype(np.single)
    w_mid = ((w_max + w_min) / 2).astype(np.single)

    header.set("SPEC_VAL", w_mid, f"[{w_units}] Characteristic spec coord")
    header.set("SPEC_BW", width, f"[{w_units}] Width of spectrum")
    header.set("TDMIN1", w_min, f"[{w_units}] Starting wavelength")
    header.set("TDMAX1", w_max, f"[{w_units}] Ending wavelength")

    try:
        bandpass = original_header_dict["bandpass"]
        header.set("SPECBAND", bandpass, "SED.bandpass")
    except KeyError:
        bandpass = assign_ucd(w_mid * wavelength_data.unit)
        header.set("SPECBAND", bandpass, "SED.bandpass")

    # OPTIONAL Header keywords
    try:
        exposure_time = original_header_dict["exposure_time"]
        header.set("TELAPSE", exposure_time, "[s] Total elapsed time")
    except KeyError:
        exposure_time = input("Please input the exposure time in seconds: ")
        if exposure_time != "":
            header.set("TELAPSE", exposure_time, "[s] Total elapsed time")

    try:
        time_start = Time(dateparser.parse(original_header_dict["start_time"])).jd
        header.set("TSTART", time_start, "[d] MJD start time")
    except KeyError:
        time_start = None

    try:
        time_stop = Time(dateparser.parse(original_header_dict["stop_time"])).jd
        header.set("TSTOP", time_stop, "[d] MJD stop time")
    except KeyError:
        time_stop = None

    try:
        time = (
            Time(dateparser.parse(original_header_dict["start_time"])).jd
            + Time(dateparser.parse(original_header_dict["stop_time"])).jd
        ) / 2
        header.set("TMID", time, "[d] MJD mid expsoure")
    except KeyError:
        time = None

    try:
        obs_location = original_header_dict["observatory"]
        header.set("OBSERVAT", obs_location, "name of observatory")
    except KeyError:
        obs_location = None

    try:
        aperture = original_header_dict["aperture"]
        header.set("APERTURE", aperture, "[arcsec] slit width")
    except KeyError:
        aperture = input("OPTIONAL: Please input the slitwidth in arcseconds: ")
        if aperture != "":
            header.set("APERTURE", aperture, "[arcsec] slit width")

    # Publication Information
    try:
        title = original_header_dict["title"]  # trim so header wraps nicely
        header.set("TITLE", title, "Data set title")
    except KeyError:
        title = None

    try:
        header.set("AUTHOR", original_header_dict["author"], "Authors of the data")
    except KeyError:
        author = input("OPTIONAL: Please input the original authors of the data: ")
        if author != "":
            header.set("AUTHOR", author, "Authors of the data")

    try:
        header.set("VOREF", original_header_dict["bibcode"], "Bibcode of dataset")
    except KeyError:
        pass

    try:
        header.set("REFERENC", original_header_dict["doi"], "DOI of dataset")
    except KeyError:
        pass

    try:
        header.set("VOPUB", original_header_dict["VOPUB"], "VO Publisher")
    except KeyError:
        pass

    try:
        header.set("COMMENT", comment)
    except KeyError:
        pass

    try:
        header.set("HISTORY", history)
    except KeyError:
        pass

    header.set("DATE", date.today().strftime("%Y-%m-%d"), "Date of file creation")
    header.set("CREATOR", "simple.spectra.convert_to_fits.py", "FITS file creator")

    return header


def convert_to_fits(
    wavelength=None,
    flux=None,
    flux_unc=None,
    header: dict = None,
    out_directory: str = ".",
):
    """Converts a spectrum to a fits file.
    # TODO: add typehits for astropy quantities

    wavelength: an array of wavelengths with units
    flux: an array of fluxes with units
    flux_unc: an array of flux uncertainties with units
    header: a dictionary of header values made with compile_header function

    """

    object_name = header["OBJECT"]

    header["HISTORY"] = "File made with the simple.spectra.convert_to_fits.py function"

    spectrum_data_out = Table(
        {
            "wavelength": wavelength,
            "flux": flux,
            "flux_uncertainty": flux_unc,
        }
    )

    # Make the HDUs
    hdu1 = fits.BinTableHDU(data=spectrum_data_out)
    hdu1.header["EXTNAME"] = "SPECTRUM"
    hdu1.header.set("OBJECT", object_name, "Object Name")
    hdu0 = fits.PrimaryHDU(header=header)

    # Write the MEF with the header and the data
    spectrum_mef = fits.HDUList([hdu0, hdu1])  # hdu0 is header and hdu1 is data

    fits_filename = out_directory + object_name + "_" + header["DATE-OBS"] + ".fits"
    try:
        spectrum_mef.writeto(fits_filename, overwrite=True, output_verify="exception")
        # TODO: think about overwrite
        logger.info(f"Wrote {fits_filename}")
    except:
        raise

    return
