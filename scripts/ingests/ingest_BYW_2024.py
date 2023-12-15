from scripts.ingests.ingest_utils import *
from scripts.ingests.utils import *
from astropy.io import ascii
import astropy.units as u
from astropy.coordinates import Angle

SAVE_DB = True  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files
# LOAD THE DATABASE
db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)


# live google sheet
link = "https://docs.google.com/spreadsheets/d/1JFa8F4Ngzp3qAW8NOBurkz4bMKo9zXYeF6N1vMtqDZs/edit#gid=0"

columns = ['source', 'ra', 'dec', 'epoch', 'equinox', 'shortname', 'reference', 'other_ref', 'comments']
byw_table = ascii.read(link, format='ascii', data_start=2, data_end=90, header_start=1, names=columns, guess=False,
                           fast_reader=False, delimiter=',')

data_columns = ['Source', 'RA', 'Dec', 'Epoch', 'Equinox', 'Shortname', 'Reference', 'Other_ref', 'Comments']  # columns with wanted data values

# replacing empty values ('cdots') with None
for column in data_columns:
    byw_table[column][np.where(byw_table[column] == 'cdots')] = None

def ingest_source(db):

    ingest_source(db, source = ["CWISE J000021.45-481314.9"], 
                  reference = "Rothermich", 
                  ra = [0.0893808], 
                  dec = [-48.2208077], 
                  epoch = [2015.4041], 
                  equinox = "ICRS")
                  

# WRITE THE JSON FILES
if SAVE_DB:
    db.save_database(directory='data/') 