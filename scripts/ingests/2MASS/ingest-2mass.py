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
sources = db.query(db.Sources.c.source)

# Use SIMBAD to 2MASS designations for all sources
tmass_designations = find_in_simbad(sources, '2MASS', verbose=VERBOSE)
tmass_desig_file_string = 'scripts/ingests/2MASS/2MASS_designations_'+DATE_SUFFIX+'.xml'
tmass_designations.write(tmass_desig_file_string, format='votable', overwrite=True)
# tmass_designations = Table.read(tmass_desig_file_string, format='votable')


tmass_data = query_tmass(tmass_designations)
tmass_data_file_string = 'scripts/ingests/2MASS/2MASS_data_'+DATE_SUFFIX+'.xml'
tmass_data.write(tmass_data_file_string, format='votable')
# read results from saved table
# tmass_data = Table.read(tmass_data_file_string, format='votable')




# add 2MASS telescope, instrument, and filters
def update_ref_tables():
    add_publication(db, doi='10.1051/0004-6361/201629272', name='Gaia')
    db.Publications.delete().where(db.Publications.c.name == 'GaiaDR2').execute()
    db.Publications.update().where(db.Publications.c.name == 'Gaia18').values(name='GaiaDR2').execute()
    add_publication(db, doi='10.1051/0004-6361/202039657', name='GaiaEDR3')

    gaia_instrument = [{'name': 'Gaia', 'reference': 'Gaia'}]
    gaia_telescope = gaia_instrument
    db.Instruments.insert().execute(gaia_instrument)
    db.Telescopes.insert().execute(gaia_telescope)

    gaia_filters = [{'band': 'GAIA2.G',
                     'instrument': 'Gaia',
                     'telescope': 'Gaia',
                     'effective_wavelength': 6230.,
                     'width': 4183.},
                    {'band': 'GAIA2.Grp',
                     'instrument': 'Gaia',
                     'telescope': 'Gaia',
                     'effective_wavelength': 7730.,
                     'width': 2757.},
                    {'band': 'GAIA3.G',
                     'instrument': 'Gaia',
                     'telescope': 'Gaia',
                     'effective_wavelength': 5822.,
                     'width': 4053.},
                    {'band': 'GAIA3.Grp',
                     'instrument': 'Gaia',
                     'telescope': 'Gaia',
                     'effective_wavelength': 7620.,
                     'width': 2924.}
                    ]

    gaia_dr3filters = [{'band': 'GAIA3.G',
                     'instrument': 'Gaia',
                     'telescope': 'Gaia',
                     'effective_wavelength': 5822.,
                     'width': 4053.},
                    {'band': 'GAIA3.Grp',
                     'instrument': 'Gaia',
                     'telescope': 'Gaia',
                     'effective_wavelength': 7620.,
                     'width': 2924.}
                    ]

    db.PhotometryFilters.insert().execute(gaia_filters)
    db.PhotometryFilters.insert().execute(gaia_dr3filters)
    db.save_reference_table('Publications', 'data')
    db.save_reference_table('Instruments', 'data')
    db.save_reference_table('Telescopes', 'data')
    db.save_reference_table('PhotometryFilters', 'data')


# update_ref_tables()

# add 2MASS designations to Names table as needed
# Find difference between all 2MASS and Names Table
# add_names(db, sources=gaia_dr2_data['db_names'], other_names=gaia_dr2_data['gaia_designation'], verbose=VERBOSE)





add_tmass_photometry(db, tmass_data)



# query the database for number to add to the data tests
phot_count = db.query(Photometry.band, func.count(Photometry.band)).group_by(Photometry.band).all()
print('Photometry: ', phot_count)

# Write the JSON files
if SAVE_DB:
    db.save_database(directory='data/')
