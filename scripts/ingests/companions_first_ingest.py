# script ingest first data into companion tables

from scripts.ingests.ingest_utils import *
from scripts.ingests.utils import *
from astropy.table import Table
import astropy.units as u
from astropy.coordinates import Angle


SAVE_DB = True  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files
VERBOSE = False
# LOAD THE DATABASE
db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)


# Ingest CWISE J210640.16+250729.0 and its reference
ingest_publication(db, doi = None, bibcode = None, publication = "Roth", 
                   description = "Rothermich in prep.", ignore_ads = True)


ra_2106= Angle("21 06 40.1664", u.hour).degree 
dec_2507=Angle("+25 07 28.99", u.degree).degree

ingest_sources(db, ["CWISE J210640.16+250729.0"], references="Roth", 
            ras= [ra_2106], 
            decs=[dec_2507], 
            search_db=False)

#  Ingest other name for NLTT 1011B (one used in SIMBAD)
#  code from deprecated utils does not work
ingest_names(db, 'NLTT 1011B', '2MASS J00193275+4018576')


#  start of ingest 
#  link to live google sheet
link = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vQEOZ56agEsAAd6SHVrwXc4hIrTtlCnCNWMefGuKkAXBjius1LgKFbj8fgbL7e8bmLTJbbBIZgwPQrz/pub?gid=0&single=true&output=csv'
#  
columns = ['source', 'companion_name', 'projected_separation', 'projected_separation_error', 'relationship', 'comment', 'ref', 'other_companion_names']
companions = Table.read(link, format='ascii', data_start=2, data_end=14, names=columns, guess=False,
                           fast_reader=False, delimiter=',')


for row in companions:
    #  collecting variables 
    source, companion_name, projected_separation_arcsec, relationship = row['source', 'companion_name', 'projected_separation','relationship']
    
    #  getting reference if there is one 
    ref = None
    if row['ref'] != '':
        ref = row['ref']
    # getting other name if there is one 
    other_companion_names = None
    if row['other_companion_names'] != '':
        other_companion_names = row['other_companion_names']

    #  adding row
    ingest_companion_relationships(db, source, companion_name, projected_separation_arcsec =projected_separation_arcsec, 
                                 relationship = relationship, ref = ref, other_companion_names= other_companion_names)




# WRITE THE JSON FILES
if SAVE_DB:
    db.save_database(directory='data/')