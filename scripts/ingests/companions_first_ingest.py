# script ingest first data into companion tables

from scripts.ingests.ingest_utils import *
from scripts.ingests.utils import *
from astropy.table import Table
import astropy.units as u
from astropy.coordinates import Angle


SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files
VERBOSE = False
# LOAD THE DATABASE
db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)


# Ingest CWISE J210640.16+250729.0 and its reference
ingest_publication(db, doi = None, bibcode = None, publication = "Roth", 
                   description = "Rothermich in prep.", ignore_ads = True)

RA =   "21 06 40.1664"
DEC = "+25 07 28.99"
ingest_sources(db, ["CWISE J210640.16+250729.0"], references="Roth", 
            ras= [Angle(RA, u.degree).degree], 
            decs=[Angle(DEC, u.degree).degree], 
            search_db=False)

