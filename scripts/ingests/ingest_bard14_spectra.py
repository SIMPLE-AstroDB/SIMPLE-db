from scripts.ingests.ingest_utils import *
from scripts.ingests.utils import *
from astropy.table import Table
import astropy.units as u
from astropy.coordinates import Angle
   

SAVE_DB = False  # True: save the data files(json) in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files
# LOAD THE DATABASE
db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)

#Find how to read data from folder
bard14 = ascii.read()

# Read in file as Astropy table (Not sure how to ingest a folder, dont use csv)
# file = 'bard14.csv'
file = 'bard14.csv'
data = Table.read('scripts/ingests/' + file)

# print result astropy table
print(bard14.info)

#Ingest spectral types 
#Loop through data 
def ingest_all_spectral_types(db):
    for row in file[1:90]:  

        # Print spectral type information
        print("Spectral Type Information:")
        
        for col_name in row.colnames:
            print(f"{col_name}: {row[col_name]}")


        print("-" * 20)

#Call spectral types function
ingest_all_spectral_types(db)


# Ingest SPECTRAL TYPES, loop through data
ingest_spectral_types(db, 
                          sources = " ", 
                          spectral_types= " ", 
                          references = " ", 
                          regimes = " ", 
                          spectral_type_error=None,
                          comments=None)

#Idea to open a fits file
#from astropy.io import fits
#fits_image_filename = fits.util.get_testdata_filepath('test0.fits')
#hdul = fits.open(fits_image_filename)

# WRITE THE JSON FILES
if SAVE_DB:
    db.save_database(directory="data/")