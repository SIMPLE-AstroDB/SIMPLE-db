# script ingest first data into companion tables

from scripts.ingests.ingest_utils import *
from scripts.ingests.utils import *
from astropy.table import Table
import astropy.units as u
from astropy.coordinates import Angle


SAVE_DB = True  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files
# LOAD THE DATABASE
db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)


# Ingest wise_1810-1010 and its reference
#doi- 10.3847/1538-4357/ab9a40  and  10.1051/0004-6361/202243516
#bibcode of coordinates reference- 2020ApJ...898...77S  and  2022A&A...663A..84L
ingest_publication(db, doi = "10.3847/1538-4357/ab9a40")

ingest_publication(db, doi = "10.1051/0004-6361/202243516")

ra_1810= Angle("18 10 06.18", u.hour).degree  
dec_1010=Angle("-10 10 00.5", u.degree).degree

ingest_sources(db, ["CWISEP J181006.00-101001.1"], references=["Schn20"], 
               ras= [ra_1810], 
               decs=[dec_1010], 
               search_db=False)

#  Ingest other name for Wise 1810-1010 (one used in SIMBAD)
#  code from deprecated utils does not work
ingest_names(db, 'CWISEP J181006.00-101001.1', 'Wise 1810-1010')

# PARALLAXES
ingest_parallaxes(db, 
                  sources = ["CWISEP J181006.00-101001.1"], 
                  plxs = [112.5], 
                  plx_errs = [8.1], 
                  plx_refs = "Lodi22", 
                  comments=None)

# PROPER MOTIONS
ingest_proper_motions(db, sources = ["CWISEP J181006.00-101001.1"], 
                          pm_ras = [-1027], 
                          pm_ra_errs = [3.5],
                          pm_decs = [-246.4], 
                          pm_dec_errs = [3.6], 
                          pm_references = "Schn20")

# WRITE THE JSON FILES
if SAVE_DB:
    db.save_database(directory='data/')