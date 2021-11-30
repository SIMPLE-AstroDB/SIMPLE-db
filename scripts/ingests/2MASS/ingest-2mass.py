from scripts.ingests.utils import *
from astropy.table import Table, unique, setdiff
from sqlalchemy import func


SAVE_DB = True  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files
VERBOSE = True
DATE_SUFFIX = 'Sep2021'

db = load_simpledb(db_file='SIMPLE.db', RECREATE_DB=RECREATE_DB)

## FUNCTIONS

# QUERY SIMBAD TO GET 2MASS PHOTOMETRY
def query_tmass(source_names):
    Simbad.reset_votable_fields()
    Simbad.add_votable_fields('typed_id')  # keep search term in result table
    Simbad.add_votable_fields('flux(J)')
    Simbad.add_votable_fields('flux_error(J)')
    Simbad.add_votable_fields('flux_bibcode(J)')
    Simbad.add_votable_fields('flux(H)')
    Simbad.add_votable_fields('flux_error(H)')
    Simbad.add_votable_fields('flux_bibcode(H)')
    Simbad.add_votable_fields('flux(K)')
    Simbad.add_votable_fields('flux_error(K)')
    Simbad.add_votable_fields('flux_bibcode(K)')

    print('2MASS query started')
    result_table = Simbad.query_objects(source_names)
    print('2MASS query complete')

    # find indexes which contain 2MASS results
    ind = result_table['FLUX_BIBCODE_J'] == '2003yCat.2246....0C'

    tmass_phot = result_table['TYPED_ID', 'FLUX_J', 'FLUX_ERROR_J', 'FLUX_H','FLUX_ERROR_H', 'FLUX_K', 'FLUX_ERROR_K'][ind]
    tmass_phot_unique = unique(tmass_phot, keys='TYPED_ID',keep='first')
    print(len(tmass_phot), len(tmass_phot_unique))
    return tmass_phot_unique

# remove sources with blank other_name
# blank_names = db.query(db.Names).filter(db.Names.c.other_name == '').astropy()
# db.Names.delete().where(db.Names.c.other_name == '').execute()
# db.save_database(directory='data/')


#get list of all sources
sources = db.query(db.Sources.c.source).astropy()

# Use SIMBAD to 2MASS designations for all sources
# tmass_designations = find_in_simbad(sources['source'], '2MASS J')
tmass_desig_file_string = 'scripts/ingests/2MASS/2MASS_designations_'+DATE_SUFFIX+'.xml'
# tmass_designations.write(tmass_desig_file_string, format='votable', overwrite=True)
tmass_designations = Table.read(tmass_desig_file_string, format='votable')

# Use SIMBAD to get 2MASS photometry for all 2MASS sources
# tmass_phot = query_tmass(tmass_designations['db_names'])
tmass_phot_file_string = 'scripts/ingests/2MASS/2MASS_data_'+DATE_SUFFIX+'.xml'
# tmass_phot.write(tmass_phot_file_string, format='votable')
# read results from saved table
tmass_phot = Table.read(tmass_phot_file_string, format='votable')
tmass_phot_unique = unique(tmass_phot, keys='TYPED_ID',keep='first')


# add 2MASS designations to Names table as needed

# Find difference between all 2MASS and Names Table
tmass_desig_in_db = db.query(db.Names).filter(db.Names.c.other_name.in_(tmass_designations['designation'])).table()

# find sources in tmass_designations not in tmass
tmass_desig_in_db['designation']=tmass_desig_in_db['other_name']
tmass_not_in_db = setdiff(tmass_designations, tmass_desig_in_db, keys=['designation'])

add_names(db, sources=tmass_not_in_db['db_names'], other_names=tmass_not_in_db['designation'])
# There was a problem with 2MASS J12475047-0152142. \t in source name. Modified JSON directly.


# ADD J band photometry
unmasked_J_phot = np.logical_not(tmass_phot_unique['FLUX_J'].mask).nonzero()
tmass_J_phot = tmass_phot_unique[unmasked_J_phot]['TYPED_ID', 'FLUX_J', 'FLUX_ERROR_J']
ingest_photometry(db, tmass_J_phot['TYPED_ID'], '2MASS.J', tmass_J_phot['FLUX_J'], tmass_J_phot['FLUX_ERROR_J'],
                  'Cutr03', ucds='em.IR.J', telescope='2MASS', instrument='2MASS')

# ADD H band photometry
unmasked_H_phot = np.logical_not(tmass_phot_unique['FLUX_H'].mask).nonzero()
tmass_H_phot = tmass_phot_unique[unmasked_H_phot]['TYPED_ID', 'FLUX_H', 'FLUX_ERROR_H']
tmass_H_phot.sort('FLUX_ERROR_H', reverse=True)
ingest_photometry(db, tmass_H_phot['TYPED_ID'], '2MASS.H', tmass_H_phot['FLUX_H'], tmass_H_phot['FLUX_ERROR_H'],
                  'Cutr03', ucds='em.IR.H', telescope='2MASS', instrument='2MASS')

# ADD K band photometry
unmased_K_phot = np.logical_not(tmass_phot_unique['FLUX_K'].mask).nonzero()
tmass_K_phot = tmass_phot_unique[unmased_K_phot]['TYPED_ID', 'FLUX_K', 'FLUX_ERROR_K']
ingest_photometry(db, tmass_K_phot['TYPED_ID'], '2MASS.Ks', tmass_K_phot['FLUX_K'], tmass_K_phot['FLUX_ERROR_K'],
                  'Cutr03', ucds='em.IR.K', telescope='2MASS', instrument='2MASS')


# ADded
# J Photometry measurements added to database:  1789
# H Photometry measurements added to database:  1778
# K Photometry measurements added to database:  1751

# query the database for number to add to the data tests
phot_count = db.query(Photometry.band, func.count(Photometry.band)).group_by(Photometry.band).all()
print('Photometry: ', phot_count)

# Write the JSON files
if SAVE_DB:
    db.save_database(directory='data/')
