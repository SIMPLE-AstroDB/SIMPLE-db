import pandas as pd
from pandas import to_datetime
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
    file = os.path.basename(spectrum_url)

    history = f'Original file: {file}' #gives orginal name of file

    #turn these into a dictionary
    spectrum_info_all = {
        'VOCLASS' : 'Spectrum-1.0',
        'VOPUB' : 'SIMPLE Archive' ,
        'RA' : row['ra'] ,
        'dec' : row['dec'] ,
        'bandpass' : None ,
        'aperture' : None ,
        'object_name' : object_name,

        #OTHER KEYWORDS
        'bibcode' : None ,
        'instrument' : row['instrument'] ,
        'obs_date' : to_datetime(row['observation_date']) ,
        'author' : None,
        'doi' : None ,
        'telescope' : row['telescope'] ,
        'history' : history ,
        'comment': row['spectrum comments'],
        'fits_data_dir': '/Users/jolie/gitlocation/SIMPLE-db/scripts/spectra_convert/bard14', #seperate dic
        'generated_history': None,
        }

    #compile_header()
    #convert_to_fits(spectrum_info_all, spectrum_table)
