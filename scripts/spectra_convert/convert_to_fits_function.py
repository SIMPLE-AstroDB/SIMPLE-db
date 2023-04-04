import os
import sys
import logging
from urllib.parse import unquote
import astropy.io.fits as fits
from astropy.table import Table
from matplotlib import pyplot as plt
from specutils import Spectrum1D
from header_function import *
logger = logging.getLogger('SIMPLE')
import astropy.units as u


# Logger setup
logger.propagate = False  # prevents duplicated logging messages
LOGFORMAT = logging.Formatter('%(asctime)s %(levelname)s: %(message)s', datefmt='%m/%d/%Y %I:%M:%S%p')
ch = logging.StreamHandler(stream=sys.stdout)
ch.setFormatter(LOGFORMAT)
# To prevent duplicate handlers, only add if they haven't been set previously
if not len(logger.handlers):
    logger.addHandler(ch)
logger.setLevel(logging.INFO)


def convert_to_fits(spectrum_info_all, spectrum_table, header):
    # TODO: add docstring with expected keywords
    # TODO: add error handling for expected keywords

    object_name =header['OBJECT']

    header['HISTORY'] = "File made with the SIMPLE convert_to_fits.py function"

    wavelength = spectrum_table['wavelength']
    flux = spectrum_table['flux']
    flux_unc = spectrum_table['flux_uncertainty']

    #header = compile_header(wavelength, **spectrum_info_all)

    spectrum_data_out = Table({'wavelength': wavelength*u.um, 'flux': flux * u.Jy, 'flux_uncertainty': flux_unc*u.Jy})


    # Make the HDUs
    hdu1 = fits.BinTableHDU(data=spectrum_data_out)
    hdu1.header['EXTNAME'] = 'SPECTRUM'
    hdu1.header.set('OBJECT', object_name, 'Object Name')
    hdu0 = fits.PrimaryHDU(header=header)

    # Write the MEF with the header and the data
    spectrum_mef = fits.HDUList([hdu0, hdu1])  # hdu0 is header and hdu1 is data

    fits_filename = spectrum_info_all['fits_data_dir'] + object_name + '_' + header['DATE-OBS'] + '.fits'
    try:
        spectrum_mef.writeto(fits_filename, overwrite=True, output_verify="exception")
        # TODO: think about overwrite
        logger.info(f'Wrote {fits_filename}')
    except:
        raise

    return
