import os
from pandas import to_datetime
from astropy.time import Time
from astropy.nddata import StdDevUncertainty
from urllib.parse import unquote
from datetime import date
import astropy.io.fits as fits
from astropy.table import Table
import warnings

warnings.filterwarnings('ignore')

def convert_to_fits(spectrum_info_all):
    object_name = unquote(spectrum_info_all['object_name'])
    spectrum_info_all['object_name'] = object_name
    print(object_name)

    spectrum_path = spectrum_info_all['file_path']
    file = os.path.basename(spectrum_path)

    history1 = ascii(f'Original file: {file}')  # gives original name of file
    history2 = spectrum_info_all['generated_history']  # shows where file came from
    spectrum_info_all['history'] = (history1 + ', ' + history2)

    loader_function = spectrum_info_all['loader_function']
    spectrum_table = loader_function(spectrum_path)

    # add units to spectral data - SHOULD BE DONE BY loader_function.
    # wavelength_data = spectrum_table['wavelength'] * spectrum_info_all['wavelength_unit']
    wavelength_data = spectrum_table['wavelength']
    # flux_data = spectrum_table['flux'] * spectrum_info_all['flux_unit']
    flux_data = spectrum_table['flux']
    # flux_unc = StdDevUncertainty(spectrum_table['flux_uncertainty'] * spectrum_info_all['flux_unit'])
    flux_unc = spectrum_table['flux_uncertainty']

    spectrum_data_out = Table({'wavelength': wavelength_data, 'flux': flux_data, 'flux_uncertainty': flux_unc})

    hdu1 = fits.BinTableHDU(data=spectrum_data_out)

    # Write the headers
    hdu1.header['EXTNAME'] = 'SPECTRUM'
    hdu1.header.set('Spectrum', object_name, 'Object Name')

    header = compile_header(wavelength_data, **spectrum_info_all)
    hdu0 = fits.PrimaryHDU(header=header)

    # Write the MEF with the header and the data
    spectrum_mef = fits.HDUList([hdu0, hdu1])  # hdu0 is header and hdu1 is data

    file_root = os.path.splitext(file)[0]  # split the path name into a pair root and ext so the root is just the ext [0] is the name of the file wihtout the .csv
    fits_filename = spectrum_info_all['fits_data_dir'] + file_root + '.fits'  # turns into fits files by putting it in new folder that we defined at begining and adding name of file then .fits
    try:
        spectrum_mef.writeto(fits_filename, overwrite=True)
        # SHOULD BE: spectrum.write(fits_filename, format='tabular-fits', overwrite=True, update_header=True)
        #logger.info(f'Wrote {fits_filename}')
    except:
        raise ValueError

    return


def compile_header(wavelength_data, **spectra_data_info):
    """Creates a header from a dictionary of values. """

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

    try:
        header.set('SPECBAND', spectra_data_info['bandpass'], 'SED.bandpass')
    except KeyError:
        pass

    try:
        header.set('SPEC_VAL', , '[angstrom] Characteristic spec coord')
    except KeyError:
        pass

    try:
        f"[{wavelength_data.unit:FITS}]"
        header.set('SPEC_BW', header_dict['width'], f"[{header_dict['wavelength_units']}] Width of spectrum")

    try:
        header.set('APERTURE', spectra_data_info['aperture'], '[arcsec]slit width')
    except KeyError:
        pass

    try:
        obs_date = to_datetime(spectra_data_info['observation_date']).strftime("%Y-%m-%d")
        header.set('DATE-OBS', obs_date, 'date of the observation')
    except KeyError:
        obs_date = None

    # Publication Information
    try:
        header.set('TITLE', spectra_data_info['title'], 'Data set title')
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
        comment = spectra_data_info['spectra_comments']
        header.set('COMMENT', comment)
    except KeyError:
        comment = None

    try:
        header.set('HISTORY', spectra_data_info['history'])
    except KeyError:
        pass

    # Put header information into a dictionary
    header_dict = {

            'wavelength_units': ,
            'width': (max(wavelength_data) - min(wavelength_data)),
            'min_wave': min(wavelength_data),
            'max_wave': max(wavelength_data),
            'comment': comment,
            'obs_location': obs_location
        }


    if header_dict['width'] != None:

    if header_dict['min_wave'] != None:
        header.set('TDMIN1', header_dict['min_wave'], f"[{header_dict['wavelength_units']}] starting wavelength")
    if header_dict['min_wave'] != None:
        header.set('TDMAX1', header_dict['max_wave'], f"[{header_dict['wavelength_units']}] ending wavelength")
    if header_dict['aperture'] != None:


    header.set('DATE', date.today().strftime("%Y-%m-%d"), 'date of file creation')

    return header