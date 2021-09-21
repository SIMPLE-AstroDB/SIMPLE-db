from scripts.ingests.utils import *
from scripts.ingests.ingest_utils import *
from astropy.table import Table, setdiff
from astropy import table
import numpy as np
from sqlalchemy import func
import numpy as np


SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files
VERBOSE = True
DATE_SUFFIX = 'Sep2021'

db = load_simpledb(RECREATE_DB=RECREATE_DB)

#get list of all sources
sources = db.query(db.Sources.c.source).astropy()

# Use SIMBAD to 2MASS designations for all sources
# tmass_designations = find_in_simbad(sources, '2MASS', verbose=VERBOSE)
tmass_desig_file_string = 'scripts/ingests/2MASS/2MASS_designations_'+DATE_SUFFIX+'.xml'
# tmass_designations.write(tmass_desig_file_string, format='votable', overwrite=True)
tmass_designations = Table.read(tmass_desig_file_string, format='votable')


# tmass_phot = query_tmass(tmass_designations['db_names'])
tmass_phot_file_string = 'scripts/ingests/2MASS/2MASS_data_'+DATE_SUFFIX+'.xml'
# tmass_phot.write(tmass_phot_file_string, format='votable')
# read results from saved table
tmass_phot = Table.read(tmass_phot_file_string, format='votable')


# add 2MASS designations to Names table as needed
# Find difference between all 2MASS and Names Table
# add_names(db, sources=gaia_dr2_data['db_names'], other_names=gaia_dr2_data['gaia_designation'], verbose=VERBOSE)


add_tmass_photometry(db, tmass_phot)



# query the database for number to add to the data tests
phot_count = db.query(Photometry.band, func.count(Photometry.band)).group_by(Photometry.band).all()
print('Photometry: ', phot_count)

# Write the JSON files
if SAVE_DB:
    db.save_database(directory='data/')
