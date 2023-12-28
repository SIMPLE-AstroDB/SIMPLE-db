from scripts.ingests.ingest_utils import *
from scripts.ingests.utils import *
from astropy.table import Table
from astropy.io import ascii
import astropy.units as u
from astropy.coordinates import Angle
   

SAVE_DB = False  # True: save the data files(json) in addition to modifying the .db file
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


#Loop through data and update spectra
def updating_spectra(db):
    for row in bard14_table[0:19]:  

        # Print spectra information
        print("Spectra Information:")
        
        for col_name in row.colnames:
            print(f"{col_name}: {row[col_name]}")

#Call spectra function
updating_spectra(db)

#Ingest spectra loop 2
            
def update_all_spectra(db):
    updating_spectra(db)

    with db.engine.begin() as conn:
        for entry in update_all_spectra:
            source_value = entry['Source']
            spectrum_value = entry['Spectrum']
            original_spectrum_value = entry['Original Spectrum']
            regime_value = entry["regime"]
            telescope_value = entry["telescope"]
            instrument_value = entry["instrument"]
            mode_value  = entry["mode"]
            observation_date_value = entry["observation date"]
            spectrum_comments_value = entry["spectrum comments"]
            spectrum_reference_value = entry["spectrum reference"]
            ra_value = entry["ra"]
            dec_value = entry["dec"]
            aperture_value = entry["aperture"]


#update data in loop
        conn.execute(db.Spectra_table.update()\
                .where(db.Spectra_table.c.source == source_value)\
                .values(source = source_value))
        
        conn.execute(db.Spectra_table.update()\
                .where(db.Spectra_table.spectrum == spectrum_value)\
                .values(spectrum = spectrum_value))
        
        conn.execute(db.Spectra_table.update()\
                .where(db.Spectra_table.c.original_spectrum == original_spectrum_value)\
                .values(original_spectrum = original_spectrum_value))
        
        conn.execute(db.Spectra_table.update()\
                .where(db.Spectra_table.c.regime == regime_value)\
                .values(regime = regime_value))
        
        conn.execute(db.Spectra_table.update()\
                .where(db.Spectra_table.c.telescope == telescope_value)\
                .values(telescope = telescope_value))
        
        conn.execute(db.Spectra_table.update()\
                .where(db.Spectra_table.c.instrument == instrument_value)\
                .values(instrument = instrument_value))
        
        conn.execute(db.Spectra_table.update()\
                .where(db.Spectra_table.c.mode == mode_value)\
                .values(mode = mode_value))
        
        conn.execute(db.Spectra_table.update()\
                .where(db.Spectra_table.c.observation_date == observation_date_value)\
                .values(observation_date = observation_date_value))
        
        conn.execute(db.Spectra_table.update()\
                .where(db.Spectra_table.c.comments == spectrum_comments_value)\
                .values(comments = spectrum_comments_value))
        
        conn.execute(db.Spectra_table.update()\
                .where(db.Spectra_table.c.reference == spectrum_reference_value)\
                .values(reference = spectrum_reference_value))
        
        conn.execute(db.Spectra_table.update()\
                .where(db.Spectra_table.c.ra == ra_value)\
                .values(ra = ra_value))
        
        conn.execute(db.Spectra_table.update()\
                .where(db.Spectra_table.c.dec == dec_value)\
                .values(dec = dec_value))
        
        conn.execute(db.Spectra_table.update()\
                .where(db.Spectra_table.c.aperture == aperture_value)\
                .values(aperture = aperture_value))
        

        
#Call spectra function
update_all_spectra(db)

# Ingest SPECTRA, loop through data
    #ingest_spectra(db, 
    #        sources = "Source", 
    #        spectrum= "Spectrum",
    #        original_spectrum= "Original Spectrum",
    #        regimes = "regime",
    #        telescope= "telescope",
    #        instrument= "instrument",
    #        mode= "mode",
    #        observation_date= "observation date",
    #        spectrum_comments= "spectrum comments",
    #        spectrum_reference= "spectrum reference",
    #        ra= "ra",
    #        dec= "dec",
    #        aperture= "aperture",
    #        raise_error=True,
    #        search_db= True, 
    #)
#DB Table has local spectrum, comments(spectrum comments on sheet), reference(spectrum reference on sheet)
#reference and other_references as categories not listed on sheet
#Sheet has ra, sec and aperture not in db




# Add a separator between rows for better readability
#    print("-" * 50)

#Call spectral types function
#update_all_spectra(db)



# WRITE THE JSON FILES
if SAVE_DB:
    db.save_database(directory="data/")