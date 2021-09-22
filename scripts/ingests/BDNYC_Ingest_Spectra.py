from scripts.ingests.utils import *
import pandas as pd


SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = False  # recreates the .db file from the data files
VERBOSE = False


db = load_db('SIMPLE.db')

#Read in CSV file with Pandas
df = pd.read_csv('scripts/ingests/BDNYC_spectra.csv')

print(len(df))
