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

#  Ingest other name for NLTT 1011B (one used in SIMBAD)
#  code from deprecated utils does not work
#  add_names(db, sources=['NLTT 1011B'], other_names=['2MASS J00193275+4018576'])

#  start of ingest 

#ingest a single row
def add_to_CompanionRelationship(source, companion_name, projected_separation = None, 
                                 projected_separation_error = None, relationship = None, 
                                 comment = None, ref = None):
    """
    This function ingests a single row in to the CompanionRelationship table
    """
    with db.engine.connect() as conn:
        conn.execute(db.CompanionRelationships.insert().values(
            {'source': source, 
             'companion_name': companion_name, 
             'projected_separation_arcsec':projected_separation,
             'projected_separation_error':projected_separation_error, 
             'relationship':relationship,
             'reference': ref, 
             'comment': comment}))
        
        conn.commit()  # sqlalchemy 2.0 does not autocommit  can go again

#  link to live google sheet
link = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vQEOZ56agEsAAd6SHVrwXc4hIrTtlCnCNWMefGuKkAXBjius1LgKFbj8fgbL7e8bmLTJbbBIZgwPQrz/pub?gid=0&single=true&output=csv'
#  
columns = ['source', 'companion_name', 'projected_separation', 'projected_separation_error', 'relationship', 'comment', 'ref', 'in_Simple']
companions = Table.read(link, format='ascii', data_start=2, data_end=14, names=columns, guess=False,
                           fast_reader=False, delimiter=',')
   
for row in companions:
    #  collecting variables 
    source, companion_name, projected_separation, relationship = row['source', 'companion_name', 'projected_separation','relationship']
    
    #  getting reference if there is one 
    ref = None
    if row['ref'] != '':
        ref = row['ref']
    
    #  adding row
    add_to_CompanionRelationship(source, companion_name, projected_separation =projected_separation, 
                                 relationship = relationship, ref = ref)
