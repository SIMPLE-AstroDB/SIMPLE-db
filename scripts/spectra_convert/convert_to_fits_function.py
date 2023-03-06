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


    # Make the HDUs
    hdu1 = fits.BinTableHDU(data=spectrum_data_out)
    hdu1.header['EXTNAME'] = 'SPECTRUM'
    hdu1.header.set('OBJECT', object_name, 'Object Name')
    hdu0 = fits.PrimaryHDU(header=header)

    # Write the MEF with the header and the data
    spectrum_mef = fits.HDUList([hdu0, hdu1])  # hdu0 is header and hdu1 is data

    file_root = os.path.splitext(file)[0]
    fits_filename = spectrum_info_all['fits_data_dir'] + file_root + '.fits'
    try:
        spectrum_mef.writeto(fits_filename, overwrite=True, output_verify="exception")
        # TODO: think about overwrite
        logger.info(f'Wrote {fits_filename}')
    except:
        raise

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
    plt.show()

    return
