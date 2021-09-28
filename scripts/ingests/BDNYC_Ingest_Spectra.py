from scripts.ingests.utils import *
import pandas as pd


SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = False  # recreates the .db file from the data files
VERBOSE = False


db = load_simpledb('SIMPLE.db', RECREATE_DB=RECREATE_DB)

#Read in CSV file with Pandas
df = pd.read_csv('scripts/ingests/BDNYC_spectra.csv', usecols=['id', 'source_id', 'spectrum', 'wavelength_units', 'flux_units', 'wavelength_order', 'regime', 'publication_shortname', 'obs_date', 'filename', 'comments', 'best', 'local_spectrum', 'name', 'name', 'mode']) .dropna()
df.reset_index(inplace=True, drop=True)
print(len(df))
