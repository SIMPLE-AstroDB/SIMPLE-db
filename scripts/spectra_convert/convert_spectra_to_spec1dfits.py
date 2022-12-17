import os
import sys
import logging
from pandas import to_datetime
from astropy.time import Time
from urllib.parse import unquote
from datetime import date
import astropy.io.fits as fits
from astropy.table import Table
import numpy as np
from matplotlib import pyplot as plt
from specutils import Spectrum1D

logger = logging.getLogger('SIMPLE')

# Logger setup
logger.propagate = False  # prevents duplicated logging messages
LOGFORMAT = logging.Formatter('%(asctime)s %(levelname)s: %(message)s', datefmt='%m/%d/%Y %I:%M:%S%p')
ch = logging.StreamHandler(stream=sys.stdout)
ch.setFormatter(LOGFORMAT)
# To prevent duplicate handlers, only add if they haven't been set previously
if not len(logger.handlers):
    logger.addHandler(ch)
logger.setLevel(logging.INFO)


def convert_to_fits(spectrum_info_all):
    # TODO: add docstring with expected keywords
    # TODO: add error handling for expected keywords
    object_name = unquote(spectrum_info_all['object_name'])
    spectrum_info_all['object_name'] = object_name

    spectrum_path = spectrum_info_all['file_path']
    file = os.path.basename(spectrum_path)
    logger.debug(f'Trying to convert {object_name}: {file}')

    spectrum_info_all['history1'] = ascii(f'Original file: {file}')  # gives original name of file
    spectrum_info_all['history2'] = spectrum_info_all['generated_history']  # shows where file came from

    loader_function = spectrum_info_all['loader_function']
    spectrum_table = loader_function(spectrum_path)

    wavelength = spectrum_table['wavelength']
    flux = spectrum_table['flux']
    flux_unc = spectrum_table['flux_uncertainty']

    header = compile_header(wavelength, **spectrum_info_all)

    spectrum_data_out = Table({'wavelength': wavelength, 'flux': flux, 'flux_uncertainty': flux_unc})
    # logger.debug(spectrum_data_out.info())

    # NOT USING THIS. Writing out HDUs instead
    # Make the Spectrum1D object
    # wavelength_quantity = wavelength.data*wavelength.unit
    # flux_quantity = flux.data*flux.unit
    # flux_unc_quantity = flux_unc.data*flux_unc.unit
    # spectrum = Spectrum1D(spectral_axis=wavelength_quantity, flux=flux_quantity, meta=header)

    # Make the HDUs
    hdu1 = fits.BinTableHDU(data=spectrum_data_out)
    hdu1.header['EXTNAME'] = 'SPECTRUM'
    hdu1.header.set('OBJECT', object_name, 'Object Name')
    hdu0 = fits.PrimaryHDU(header=header)

    # Write the MEF with the header and the data
    spectrum_mef = fits.HDUList([hdu0, hdu1])  # hdu0 is header and hdu1 is data

    fits_filename = spectrum_info_all['fits_data_dir'] + spectrum_info_all['generated_filename'] + '.fits'
    try:
        spectrum_mef.writeto(fits_filename, overwrite=False, output_verify="exception")
        # TODO: think about overwrite
        logger.info(f'Wrote {fits_filename}')
    except:
        raise

    # NOT USING. Writing out FITS file instead.
    # fits1d_filename = f"{spectrum_info_all['fits_data_dir']}{file_root}_1d.fits"
    # try:
    #     spectrum.write(fits1d_filename, format='tabular-fits', overwrite=True, update_header=True)
    # except:
    #    raise

    # Read spectrum back in and plot
    # TODO: Make plotting optional
    spec1d = Spectrum1D.read(fits_filename, format='tabular-fits')
    header = fits.getheader(fits_filename)
    name = header['OBJECT']
    logger.info(f'Plotting spectrum of {name} stored in {fits_filename}')

    ax = plt.subplots()[1]
    # ax.plot(spec1d.spectral_axis, spec1d.flux)
    ax.errorbar(spec1d.spectral_axis.value, spec1d.flux.value, yerr=spec1d.uncertainty.array, fmt='-')
    ax.set_xlabel(f"Dispersion ({spec1d.spectral_axis.unit})")
    ax.set_ylabel(f"Flux ({spec1d.flux.unit})")
    plt.title(f"{name} {header['TELESCOP']} {header['INSTRUME']}")
    plt.savefig(spectrum_info_all['fits_data_dir'] + spectrum_info_all['generated_filename'] + '.png')

    return


def compile_header(wavelength_data, **spectra_data_info):
    """Creates a header from a dictionary of values. """
    #  TODO: Generate errors when REQUIRED keywords are missing
    #  TODO: Generate logger warnings when recommended keywords are missing

    header = fits.Header()
    header.set('EXTNAME', 'PRIMARY', 'name of this extension')

    try:
        header.set('VOCLASS', spectra_data_info['voclass'], 'VO Data Model')
    except KeyError:
        pass

    # Target Data
    try:
        header.set('OBJECT', spectra_data_info['object_name'], 'Name of observed object')
    except KeyError:
        pass

    try:
        ra = spectra_data_info['RA']
        header.set('RA', ra, '[deg] Pointing position')
    except KeyError:
        ra = None

    try:
        dec = spectra_data_info['dec']
        header.set('DEC', dec, '[deg] Pointing position')
    except KeyError:
        dec = None

    try:
        time = (Time(to_datetime(spectra_data_info['start_time'])).jd +
                Time(to_datetime(spectra_data_info['stop_time'])).jd) / 2
        header.set('TMID', time, '[d] MJD mid expsoure')
    except KeyError:
        time = None

    try:
        exposure_time = spectra_data_info['exposure_time']
        header.set('TELAPSE', exposure_time, '[s] Total elapsed time')
    except KeyError:
        exposure_time = None

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

    try:
        telescope = spectra_data_info['telescope']
        header.set('TELESCOP', telescope, 'name of telescope')
    except KeyError:
        telescope = None

    try:
        instrument = spectra_data_info['instrument']
        header.set('INSTRUME', instrument, 'name of instrument')
    except KeyError:
        instrument = None

    # Wavelength info
    w_units = wavelength_data.unit
    w_min = min(wavelength_data).astype(np.single)
    w_max = max(wavelength_data).astype(np.single)
    width = (w_max - w_min).astype(np.single)
    w_mid = ((w_max + w_min)/2).astype(np.single)

    try:
        header.set('SPECBAND', spectra_data_info['bandpass'], 'SED.bandpass')
    except KeyError:
        pass

    try:
        header.set('SPEC_VAL', w_mid, f"[{w_units}] Characteristic spec coord")
    except KeyError:
        pass

    header.set('SPEC_BW', width, f"[{w_units}] Width of spectrum")
    header.set('TDMIN1', w_min, f"[{w_units}] Starting wavelength")
    header.set('TDMAX1', w_max, f"[{w_units}] Ending wavelength")

    try:
        header.set('APERTURE', spectra_data_info['aperture'], '[arcsec] slit width')
    except KeyError:
        pass

    try:
        obs_date = to_datetime(spectra_data_info['observation_date']).strftime("%Y-%m-%d")
        header.set('DATE-OBS', obs_date, 'date of the observation')
    except KeyError:
        obs_date = None

    # Publication Information
    try:
        title = (spectra_data_info['title'])[0:20]  # trim so header wraps nicely
        header.set('TITLE', title, 'Data set title')
    except KeyError:
        pass

    try:
        header.set('AUTHOR', spectra_data_info['author'], 'Authors of the data')
    except KeyError:
        pass

    try:
        header.set('VOREF', spectra_data_info['bibcode'], 'Bibcode of dataset')
    except KeyError:
        pass

    try:
        header.set('REFERENC', spectra_data_info['doi'], 'DOI of dataset')
    except KeyError:
        pass

    try:
        header.set('VOPUB', spectra_data_info['vopub'], 'VO Publisher')
    except KeyError:
        pass

    try:
        comment = spectra_data_info['spectrum_comments']
        header.set('COMMENT', comment)
    except KeyError:
        comment = None

    try:
        header.set('HISTORY', spectra_data_info['history1'])
    except KeyError:
        pass

    try:
        header.set('HISTORY', spectra_data_info['history2'])
    except KeyError:
        pass

    header.set('HISTORY', 'This file generated by convert_spectra_to_spec1dfits.py')

    header.set('DATE', date.today().strftime("%Y-%m-%d"), 'Date of file creation')

    return header
