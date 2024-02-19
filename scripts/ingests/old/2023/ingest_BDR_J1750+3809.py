#script ingest first data into companion tables

from scripts.ingests.ingest_utils import *
from scripts.ingests.utils import *
from astropy.table import Table
import astropy.units as u
from astropy.coordinates import Angle

SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files

# LOAD THE DATABASE
db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)


# Ingest 2MASS J01415823-4633574 and its reference
#doi- 10.3847/2041-8213/abc256
#bibcode of coordinates reference- 2020ApJ...903L..33V
ingest_publication(db, doi = '10.3847/2041-8213/abc256', 
                   description = "Direct radio discovery of a cold brown dwarf.", 
                   ignore_ads = False)


ra_1750= Angle("17 50 01.13", u.hour).degree 
dec_3809=Angle("+38 09 19.5", u.degree).degree

ingest_sources(db, ["[VCS2020] BDR J1750+3809"], references="Veda20", 
            ras= [ra_1750], 
            decs=[dec_3809], 
            search_db=False)

#  Ingest other name for BDR J1750+3809 (one used in SIMBAD)
#  code from deprecated utils does not work
ingest_names(db, '[VCS2020] BDR J1750+3809', 'BDR J1750+3809')


# SPECTRAL TYPES
# The spectral type of T6.5 is based on data in the nir regime
ingest_spectral_types(db, sources = ["[VCS2020] BDR J1750+3809"],
                          spectral_types = "T6.5",
                          references = "Veda20",
                          regimes = "nir", 
                          spectral_type_error="1",
                          comments=None)

# PROPER MOTIONS
ingest_proper_motions(db, sources = ["[VCS2020] BDR J1750+3809"], 
                          pm_ras = [-120], 
                          pm_ra_errs = [30],
                          pm_decs = [200], 
                          pm_dec_errs = [30], 
                          pm_references = "Veda20")

# WRITE THE JSON FILES
if SAVE_DB:
    db.save_database(directory='data/')
    
