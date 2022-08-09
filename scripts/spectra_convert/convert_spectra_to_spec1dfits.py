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

def convert_to_fits(spectra_data_info):
    for row in spectra_data_info['data']:
        print(row[spectra_data_info['object_name_column']])
        object_name = unquote(row[spectra_data_info['object_name_column']])
        spectra_data_info['object_name'] = object_name
        print(object_name)

        spectrum_path = row[spectra_data_info['spectrum_path_column']]
        file = os.path.basename(spectrum_path)

        history1 = ascii(f'Original file: {file}')  # gives original name of file
        history2 = spectra_data_info['generated_history']  # shows where file came from
        spectra_data_info['history'] = (history1 + ', ' + history2)

        loader_function = spectra_data_info['loader_function']
        spectrum_table = loader_function(spectrum_path)

        # add units to spectral data
        wavelength_data = spectrum_table['wavelength'] * spectra_data_info['wavelength_unit']
        flux_data = spectrum_table['flux'] * spectra_data_info['flux_unit']
        flux_unc = StdDevUncertainty(spectrum_table['flux_uncertainty'] * spectra_data_info['flux_unit'])

        spectrum_data_out = Table({'wavelength': wavelength_data, 'flux': flux_data, 'flux_uncertainty': flux_unc})

        hdu1 = fits.BinTableHDU(data=spectrum_data_out)

        # Write the headers
        hdu1.header['EXTNAME'] = 'SPECTRUM'
        hdu1.header.set('Spectrum', object_name, 'Object Name')

        header = compile_header(row, wavelength_data, **spectra_data_info)
        hdu0 = fits.PrimaryHDU(header=header)

        # Write the MEF with the header and the data
        spectrum_mef = fits.HDUList([hdu0, hdu1])  # hdu0 is header and hdu1 is data

        file_root = os.path.splitext(file)[0]  # split the path name into a pair root and ext so the root is just the ext [0] is the name of the file wihtout the .csv
        fits_filename = spectra_data_info['fits_data_dir'] + file_root + '.fits'  # turns into fits files by putting it in new folder that we defined at begining and adding name of file then .fits
        try:
            spectrum_mef.writeto(fits_filename, overwrite=True)
            # SHOULD BE: spectrum.write(fits_filename, format='tabular-fits', overwrite=True, update_header=True)
            #logger.info(f'Wrote {fits_filename}')
        except:
            raise ValueError

    return


def compile_header(row, wavelength_data, **spectra_data_info):
    """Creates a header from a dictionary of values. """

    # Setup the header information
    try:
        ra = row[spectra_data_info['RA_column_name']]
    except KeyError:
        ra = None

    try:
        dec = row[spectra_data_info['dec_column_name']]
    except KeyError:
        dec = None

    try:
        time = (Time(to_datetime(row[spectra_data_info['start_time_column_name']])).jd +
                Time(to_datetime(row[spectra_data_info['stop_time_column_name']])).jd) / 2
    except KeyError:
        time = None

    try:
        exposure_time = row[spectra_data_info['exposure_time_column_name']]
    except KeyError:
        exposure_time = None

    try:
        time_start = Time(to_datetime(row[spectra_data_info['start_time_column_name']])).jd
    except KeyError:
        time_start = None

    try:
        time_stop = Time(to_datetime(row[spectra_data_info['stop_time_column_name']])).jd
    except KeyError:
        time_stop = None

    try:
        instrument = row[spectra_data_info['instrument_column_name']]
    except KeyError:
        instrument = None

    try:
        obs_date = to_datetime(row[spectra_data_info['observation_date_column_name']])
    except KeyError:
        obs_date = None

    try:
        telescope = row[spectra_data_info['telescope_column_name']]
    except KeyError:
        telescope = None

        # Put header information into a dictionary
        header_dict = {
            'RA': ra,
            'dec': dec,
            'time': time,
            'exposure_time': exposure_time,
            'bandpass': spectra_data_info['bandpass'],
            'aperture': spectra_data_info['aperture'],
            'object_name': spectra_data_info['object_name'],

            # OTHER KEYWORDS
            'time_start': time_start,
            'time_stop': time_stop,
            'bibcode': spectra_data_info['bibcode'],
            'instrument': instrument,
            'obs_date': obs_date,
            'author': spectra_data_info['author'],
            'reference_doi': spectra_data_info['doi'],
            'telescope': telescope,
            'history': spectra_data_info['history'],
            'wavelength_units': f"[{wavelength_data.unit:FITS}]",
            'width': (max(wavelength_data).value - min(wavelength_data.value)),
            'min_wave': min(wavelength_data).value,
            'max_wave': max(wavelength_data).value,
            'comment': spectra_data_info['spectra_comments'],
            'obs_location': spectra_data_info['obs_location']
        }

    header = fits.Header()
    header.set('EXTNAME', 'PRIMARY', 'name of this extension')
    # IVOA SpectrumDM keywords REQUIRED

    if header_dict['VOCLASS'] != None:
        header.set('VOCLASS', spectra_data_info['voclass'])
    if header_dict['VOPUB'] != None:
        header.set('VOPUB', spectra_data_info['vopub'],'VO Publisher ID URI')  # uniform research identifier
    if header_dict['title'] != None:
        header.set('TITLE', spectra_data_info['title'], 'data set title')
    if header_dict['object_name'] != None:
        header.set('OBJECT', header_dict['object_name'], 'name of observed object')
    if header_dict['RA'] != None:
        header.set('RA', header_dict['RA'], '[deg] Pointing position')
    if header_dict['dec'] != None:
        header.set('DEC', header_dict['dec'], '[deg] Pointing position')
    if header_dict['time'] != None:
        header.set('TMID', header_dict['time'], '[d] MJD mid expsoure')
    if header_dict['time_start'] != None:
        header.set('TSTART', header_dict['time_start'], '[d] MJD start time')
    if header_dict['time_stop'] != None:
        header.set('TSTOP', header_dict['time_stop'], '[d] MJD stop time')
    if header_dict['exposure_time'] != None:
        header.set('TELAPSE', header_dict['exposure_time'], '[s] Total elapsed time')
    if header_dict['bandpass'] != None:
        header.set('SPEC_VAL', header_dict['bandpass'], '[angstrom] Characteristic spec coord')
    if header_dict['width'] != None:
        header.set('SPEC_BW', header_dict['width'], f"[{header_dict['wavelength_units']}] Width of spectrum")
    if header_dict['min_wave'] != None:
        header.set('TDMIN1', header_dict['min_wave'], f"[{header_dict['wavelength_units']}] starting wavelength")
    if header_dict['min_wave'] != None:
        header.set('TDMAX1', header_dict['max_wave'], f"[{header_dict['wavelength_units']}] ending wavelength")
    if header_dict['aperture'] != None:
        header.set('APERTURE', header_dict['aperture'],'[arcsec]slit width')
    if header_dict['author'] != None:
        header.set('AUTHOR', header_dict['author'], 'author of the data')
    # Other IVOA SpectrumDM keywords
    if header_dict['bibcode'] != None:
        header.set('VOREF', header_dict['bibcode'], 'bibcode')

    header.set('DATE', date.today().strftime("%Y-%m-%d"), 'date of file creation')

    if header_dict['instrument'] != None:
        header.set('INSTRUME', header_dict['instrument'], 'name of instrument')
    if header_dict['obs_date'] != None:
        header.set('DATE-OBS', header_dict['obs_date'].strftime("%Y-%m-%d"), 'date of the observation')
    if header_dict['reference_doi'] != None:
        header.set('REFERENC', header_dict['reference_doi'], 'bibliographic reference')
    if header_dict['telescope'] != None:
        header.set('TELESCOP', header_dict['telescope'], 'name of telescope')
    if header_dict['history'] != None:
        header.set('HISTORY', header_dict['history'])
    if header_dict['comment'] != None:
        header.set('COMMENT', header_dict['comment'])
    if header_dict['obs_location'] != None:
        header.set('OBSERVAT', header_dict['obs_location'], 'name of observatory')

    return header