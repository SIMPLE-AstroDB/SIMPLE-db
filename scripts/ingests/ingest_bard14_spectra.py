from scripts.ingests.ingest_utils import *
from scripts.ingests.utils import *
from astropy.io import ascii
import requests
   

SAVE_DB = True  # True: save the data files(json) in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files
# LOAD THE DATABASE
db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)

#bard14 url : https://docs.google.com/spreadsheets/d/11o5NRGA7jSbHKaTNK7SJnu_DTECjsyZ6rY3rcznYsJk/edit#gid=0

SHEET_ID = '11o5NRGA7jSbHKaTNK7SJnu_DTECjsyZ6rY3rcznYsJk'
SHEET_NAME = 'bard14'
full = 'all'

url = f'https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={full}'

bard14_table = ascii.read(
    url,
    format="csv",
    data_start=1,
    header_start=0,
    guess=False,
    fast_reader=False, 
    delimiter=",",
)

#print result table
print(bard14_table.info)

          
#function to update all spectra in the database
def update_all_spectra(db):
    
    for row in bard14_table:
        source_value = row['Source']
        spectrum_value = row['Spectrum']
        original_spectrum_value = row['Original Spectrum']          


        print(source_value)
        print(spectrum_value)
        print(original_spectrum_value)

        # Add a separator between rows for better readability
        print("-" * 50)


        # The website is up if the status code is 200 (checking validity of links)
        request_response = requests.head(spectrum_value)
        status_code = (request_response.status_code)

        if status_code != 200: 
            msg = f"Link invalid:{spectrum_value}"
            raise SimpleError(msg)  

        if status_code != 200: 
            msg = f"Link invalid:{original_spectrum_value}"
            raise SimpleError(msg) 
            

        print(status_code) 


        #update data in loop
        with db.engine.begin() as conn:
            conn.execute(db.Spectra.update()
                    .where(db.Spectra.c.source == source_value)
                    .values(spectrum = spectrum_value))
    
            conn.execute(db.Spectra.update()
                    .where(db.Spectra.c.source == source_value)
                    .values(original_spectrum = original_spectrum_value))
        
        
#Call spectra function
update_all_spectra(db)


# WRITE THE JSON FILES
if SAVE_DB:
    db.save_database(directory="data/")