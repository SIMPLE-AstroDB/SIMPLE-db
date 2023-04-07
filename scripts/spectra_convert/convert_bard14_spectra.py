import pandas as pd
import warnings
warnings.filterwarnings('ignore')
from convert_to_fits_function import *
from w3lib.url import safe_url_string

# url : https://docs.google.com/spreadsheets/d/11o5NRGA7jSbHKaTNK7SJnu_DTECjsyZ6rY3rcznYsJk/edit#gid=0
SHEET_ID = '11o5NRGA7jSbHKaTNK7SJnu_DTECjsyZ6rY3rcznYsJk'
SHEET_NAME = 'AAPL'
full = 'all'

url = f'https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={full}'
data = pd.read_csv(url)

for index, row in data.iterrows():
    object_name = row['Source'] #row is defined so it loops through each row to
    spectrum_url = row['Spectrum'] #goes to specific column entry finds the url

    url_data = safe_url_string(spectrum_url, encoding="utf-8")
    spectrum_table = Table.read(url_data,format = 'ascii', names=['wavelength', 'flux', 'flux_uncertainty'])



    spectrum_info = {
        'fits_data_dir': '/Users/jolie/gitlocation/SIMPLE-db/scripts/spectra_convert/bard14/'
        }

    header_dictionary = {
        'VOCLASS' : 'Spectrum-1.0',
        'VOPUB' : 'SIMPLE Archive' ,
        'RA' : row['ra'] ,
        'dec' : row['dec'] ,
        'bandpass' : row['regime'] ,
        'aperture' : row['aperture'] ,
        'object_name' : object_name,
        'bibcode' : '2014ApJ...794..143B' ,
        'instrument' : row['instrument'] ,
        'obs_date' : row['observation date'] ,
        'title' : 'SpeX Spectroscopy of Unresolved Very Low Mass Binaries. II',
        'author' : 'Bardalez Gagliuffi, et. al.',
        'doi' : '10.1088/0004-637X/794/2/143' ,
        'telescope' : row['telescope'] ,
        'history' :  f'Original file: {os.path.basename(spectrum_url)}',
        'comment': row['spectrum comments'],
        'observatory' : row['telescope']
    }

    header = compile_header(spectrum_table['wavelength'],**header_dictionary)

    convert_to_fits(spectrum_info, spectrum_table, header)



