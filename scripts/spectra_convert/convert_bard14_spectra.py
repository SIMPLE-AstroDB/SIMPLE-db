import pandas as pd
import os
import warnings
warnings.filterwarnings('ignore')
from astropy.table import Table
from astropy import units as u
from astropy.time import Time
from header_function import *
from convert_to_fits_function import *
from w3lib.url import safe_url_string

fits_data_dir = '/Users/jolie/gitlocation/SIMPLE-db/scripts/spectra_convert/bard14' #where we want the new fits files to go

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
        'title' : None ,
        'author' : 'Bardalez Gagliuffi, Daniella ; Burgasser, Adam ; Gelino, Christopher ; Looper, Dagny ; Nicholls, Christine ; Schmidt, Sarah ; Cruz, Kelle ; West, Andrew ; Gizis, John ; Metchev, Stanimir ',
        'doi' : '10.1088/0004-637X/794/2/143' ,
        'telescope' : row['telescope'] ,
        'history' :  f'Original file: {os.path.basename(spectrum_url)}',
        'comment': row['spectrum comments'],
        'observatory' : row['telescope']

    }
    print(header_dictionary['obs_date'])
    header = compile_header(spectrum_table['wavelength'],**header_dictionary)

    convert_to_fits(spectrum_info, spectrum_table, header)
