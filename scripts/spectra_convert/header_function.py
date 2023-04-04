from pandas import to_datetime
from astropy.time import Time
from datetime import date
import astropy.io.fits as fits
import numpy as np
import logging

def compile_header(wavelength_data, **spectra_data_info):
    """Creates a header from a dictionary of values. """

    required_keywords = ['VOCLASS', 'VOPUB', 'RA', 'dec', 'bandpass', 'aperture', 'object_name', 'bibcode', 'instrument', 'obs_date', 'title', 'author', 'doi', 'telescope', 'history', 'comment', 'observatory']

    keywords_given = list(spectra_data_info.keys())

    for key in keywords_given:
        if key not in required_keywords:
            raise Exception(f"Not expecting keyword: {key}. Add manually.")
        else :
            pass

    history = spectra_data_info['history']

    comment = 'To read this file in with specutils use Spectrum1D.read() with format = \'tabular-fits\''


    header = fits.Header()
    header.set('EXTNAME', 'PRIMARY', 'name of this extension')

    try:
        header.set('VOCLASS', spectra_data_info['voclass'], 'VO Data Model')
    except KeyError:
        logging.warning('No VO Data Model')

    # Target Data
    try:
        header.set('OBJECT', spectra_data_info['object_name'], 'Name of observed object')
    except KeyError:
        logging.warning('No object name')

    try:
        ra = spectra_data_info['RA']
        header.set('RA', ra, '[deg] Pointing position')
    except KeyError:
        logging.warning('No RA')
        ra = None
    try:
        dec = spectra_data_info['dec']
        header.set('DEC', dec, '[deg] Pointing position')
    except KeyError:
        logging.warning('No DEC')
        dec = None

    try:
        time = (Time(to_datetime(spectra_data_info['start_time'])).jd +
                Time(to_datetime(spectra_data_info['stop_time'])).jd) / 2
        header.set('TMID', time, '[d] MJD mid expsoure')
    except KeyError:
        time = None
        logging.warning('No time')

    try:
        exposure_time = spectra_data_info['exposure_time']
        header.set('TELAPSE', exposure_time, '[s] Total elapsed time')
    except KeyError:
        exposure_time = None
        logging.warning('No exposure time')

    try:
        time_start = Time(to_datetime(spectra_data_info['start_time'])).jd
        header.set('TSTART', time_start, '[d] MJD start time')
    except KeyError:
        time_start = None


    try:
        time_stop = Time(to_datetime(spectra_data_info['stop_time'])).jd
        header.set('TSTOP', time_stop, '[d] MJD stop time')
    except KeyError:
        time_stop = None

    try:
        obs_location = spectra_data_info['observatory']
        header.set('OBSERVAT', obs_location, 'name of observatory')
    except KeyError:
        obs_location = None
        logging.warning('No observatory')

    try:
        telescope = spectra_data_info['telescope']
        header.set('TELESCOP', telescope, 'name of telescope')
    except KeyError:
        telescope = None
        logging.warning('No telescope')

    try:
        instrument = spectra_data_info['instrument']
        header.set('INSTRUME', instrument, 'name of instrument')
    except KeyError:
        instrument = None
        logging.warning('No instrument')

    # Wavelength info
    w_units = wavelength_data.unit
    w_min = min(wavelength_data).astype(np.single)
    w_max = max(wavelength_data).astype(np.single)
    width = (w_max - w_min).astype(np.single)
    w_mid = ((w_max + w_min)/2).astype(np.single)

    try:
        bandpass = spectra_data_info['bandpass']
        header.set('SPECBAND', bandpass, 'SED.bandpass')
    except KeyError:
        bandpass = None
        logging.warning('No bandpass')

    try:
        header.set('SPEC_VAL', w_mid, f"[{w_units}] Characteristic spec coord")
    except KeyError:
        pass

    header.set('SPEC_BW', width, f"[{w_units}] Width of spectrum")
    header.set('TDMIN1', w_min, f"[{w_units}] Starting wavelength")
    header.set('TDMAX1', w_max, f"[{w_units}] Ending wavelength")

    try:
        aperture = spectra_data_info['aperture']
        header.set('APERTURE', aperture, '[arcsec] slit width')
    except KeyError:
        aperture = None
        logging.warning('No aperature')

    try:
        obs_date = to_datetime(spectra_data_info['obs_date']).strftime("%Y-%m-%d")
        header.set('DATE-OBS', obs_date, 'date of the observation')
    except KeyError:
        print(KeyError)
        obs_date = None
        logging.warning('No observation date')

    # Publication Information
    try:
        title = (spectra_data_info['title'])  # trim so header wraps nicely
        header.set('TITLE', title, 'Data set title')
    except KeyError:
        pass
        logging.warning('No title')

    try:
        header.set('AUTHOR', spectra_data_info['author'], 'Authors of the data')
    except KeyError:
        pass
        logging.warning('No author')

    try:
        header.set('VOREF', spectra_data_info['bibcode'], 'Bibcode of dataset')
    except KeyError:
        pass

    try:
        header.set('REFERENC', spectra_data_info['doi'], 'DOI of dataset')
    except KeyError:
        pass

    try:
        header.set('VOPUB', spectra_data_info['VOPUB'], 'VO Publisher')
    except KeyError:
        logging.warning('No VO Publisher')


    try:
        header.set('COMMENT', comment)
    except KeyError:
        comment = None

    try:
        header.set('HISTORY', history)
    except KeyError:
        pass


    header.set('DATE', date.today().strftime("%Y-%m-%d"), 'Date of file creation')

    return header
