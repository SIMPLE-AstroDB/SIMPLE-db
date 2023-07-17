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
ras= Angle(RA, u.hour).degree 
decs=Angle(DEC, u.degree).degree

ingest_sources(db, ["CWISE J210640.16+250729.0"], references="Roth", 
            ras= [Angle(RA, u.degree).degree], 
            decs=[Angle(DEC, u.degree).degree], 
            search_db=False)

#  Ingest other name for NLTT 1011B (one used in SIMBAD)
#  code from deprecated utils does not work
ingest_names(db, 'NLTT 1011B', '2MASS J00193275+4018576')

# testing if duplicate is blocked
try:
    ingest_names(db, 'NLTT 1011B', '2MASS J00193275+4018576')
except SimpleError as e:
    print('DUPLICATE BLOCKED! (as expected)')
    pass


#  start of ingest 
#  link to live google sheet
link = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vQEOZ56agEsAAd6SHVrwXc4hIrTtlCnCNWMefGuKkAXBjius1LgKFbj8fgbL7e8bmLTJbbBIZgwPQrz/pub?gid=0&single=true&output=csv'
#  
columns = ['source', 'companion_name', 'projected_separation', 'projected_separation_error', 'relationship', 'comment', 'ref', 'in_Simple']
companions = Table.read(link, format='ascii', data_start=2, data_end=14, names=columns, guess=False,
                           fast_reader=False, delimiter=',')
   
for row in companions:
    #  collecting variables 
    source, companion_name, projected_separation_arcsec, relationship = row['source', 'companion_name', 'projected_separation','relationship']
    
    #  getting reference if there is one 
    ref = None
    if row['ref'] != '':
        ref = row['ref']
    
    #  adding row
    ingest_CompanionRelationship(db, source, companion_name, projected_separation_arcsec =projected_separation_arcsec, 
                                 relationship = relationship, ref = ref)




# testing companion ingest
## trying no companion 
try:
    ingest_CompanionRelationship(db, source, None )
except SimpleError as e:
    print('Companionless source in CompanionRelationship BLOCKED (as expected)')
    pass

## trying companion == source
try:
    ingest_CompanionRelationship(db, source, source)
except AssertionError as e:
    print('source == compnion_name in CompanionRelationship BLOCKED (as expected)')
    pass


## trying negative separation
try:
    ingest_CompanionRelationship(db, source, 'Bad Companion', projected_separation_arcsec = -5)
except AssertionError as e:
    print('negative separation in CompanionRelationship BLOCKED (as expected)')
    pass

## trying negative separation error
try:
    ingest_CompanionRelationship(db, source, 'Bad Companion', projected_separation_error = -5)
except AssertionError as e:
    print('negative separation in CompanionRelationship BLOCKED (as expected)')
    pass