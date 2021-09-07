import os
from astrodbkit2.astrodb import create_database
from astrodbkit2.astrodb import Database
from scripts.ingests.utils import *
from simple.schema import *
from pathlib import Path
from astroquery.gaia import Gaia
from sqlalchemy import and_, or_
from astropy.table import Table, Column, unique
from astropy.io.votable import from_table, writeto
from astroquery.simbad import Simbad


SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = False  # recreates the .db file from the data files
VERBOSE = True

verboseprint = print if VERBOSE else lambda *a, **k: None


def load_db():
    # Utility function to load the database

    db_file = 'SIMPLE.db'
    db_file_path = Path(db_file)
    db_connection_string = 'sqlite:///SIMPLE.db'
      # SQLite browser

    if RECREATE_DB and db_file_path.exists():
        os.remove(db_file)  # removes the current .db file if one already exists

    if not db_file_path.exists():
        create_database(db_connection_string)  # creates empty database based on the simple schema
        db = Database(db_connection_string)  # connects to the empty database
        db.load_database('data/')  # loads the data from the data files into the database
    else:
        db = Database(db_connection_string)  # if database already exists, connects to .db file

    return db


db = load_db()

# get all sources that have Gaia DR2 designations
# 2nd query which is all sources that are not in that list
gaia_sources = db.query(db.Names.c.source).filter(db.Names.c.other_name.ilike("Gaia DR2%")).table()
if gaia_sources:
    gaia_sources_list = gaia_sources['source'].tolist()
    no_gaia_source_id = db.query(db.Sources.c.source).filter(db.Sources.c.source.notin_(gaia_sources_list)).table()['source'].tolist()
else:
    no_gaia_source_id = db.query(db.Sources.c.source).table()['source'].tolist()


# Use SIMBAD to find Gaia designations for sources without a Gaia designation
def find_gaia_in_simbad(no_gaia_source_id, verbose = False):
    verboseprint = print if VERBOSE else lambda *a, **k: None

    sources=no_gaia_source_id
    n_sources = len(sources)

    Simbad.reset_votable_fields()
    Simbad.add_votable_fields('typed_id') # keep search term in result table
    Simbad.add_votable_fields('ids') # add all SIMBAD identifiers as an output column
    print("simbad query started")
    result_table = Simbad.query_objects(sources)
    print("simbad query ended")

    ind = result_table['SCRIPT_NUMBER_ID'] > 0 # find indexes which contain results

    simbad_ids = result_table['TYPED_ID','IDS'][ind] #.topandas()

    db_names = []
    simbad_gaia_designation = []

    for row in simbad_ids:
        db_name = row['TYPED_ID']
        ids = row['IDS'].split('|')
        gaia_designation = [i for i in ids if "Gaia DR2" in i]

        if gaia_designation:
            verboseprint(db_name,gaia_designation[0])
            db_names.append(db_name)
            simbad_gaia_designation.append(gaia_designation[0])

    n_matches = len(db_names)
    print('Found', n_matches, 'Gaia sources for', n_sources, ' sources')

    # TODO: make table of db_names and simbad_gaia_designation and return it. ZIP?
    # print(simbad_gaia_designation)
    print(n_matches, len(simbad_gaia_designation))

    table = Table([db_names, simbad_gaia_designation], names=('db_names', 'gaia_designation'))

    table.write('scripts/ingests/Gaia/gaia_designations.xml', format='votable', overwrite=True)

    return(table)


# gaia_designations = find_gaia_in_simbad(no_gaia_source_id)
# Don't need to re-run since designations are in scripts/ingests/Gaia/gaia_designations.xml

def query_gaia():
    gaia_query_string = "SELECT *,upload_table.db_names FROM gaiadr2.gaia_source " \
                             "INNER JOIN tap_upload.upload_table ON gaiadr2.gaia_source.designation = tap_upload.upload_table.gaia_designation  "
    job_gaia_query = Gaia.launch_job(gaia_query_string, upload_resource='scripts/ingests/Gaia/gaia_designations.xml',
                                     upload_table_name="upload_table", verbose=True)

    gaia_data = job_gaia_query.get_results()

    return gaia_data

# Re-run Gaia query
# gaia_data = query_gaia()

# read results from saved table
gaia_data = Table.read('scripts/ingests/Gaia/gaia_data.xml',format='votable')

# TODO: add Gaia designations to Names table
#add_names(db, sources=tmass_matches_unique['SIMPLE_source'], other_names=tmass_matches_unique['designation'], verbose=True)

# TODO: ingest Gaia proper motions
# TODO: ingest Gaia parallaxes
# TODO: ingest Gaia photometry

#add Gaia telescope, instrument, and Gaia filters
add_publication(db,doi='10.1051/0004-6361/201629272', name='Gaia')
db.Publications.delete().where(db.Publications.c.name == 'GaiaDR2').execute()
db.Publications.update().where(db.Publications.c.name == 'Gaia18').values(name='GaiaDR2').execute()

gaia_instrument = [{'name': 'Gaia','reference': 'Gaia'}]
gaia_telescope = gaia_instrument
db.Instruments.insert().execute(gaia_instrument)
db.Telescopes.insert().execute(gaia_telescope)

gaia_filters =[{'band': 'GAIA2.Gbp',
                'instrument': 'Gaia',
                'telescope': 'Gaia',
                'effective_wavelength': 5050.,
                'width': 2347.},
               {'band': 'GAIA2.G',
                'instrument': 'Gaia',
                'telescope': 'Gaia',
                'effective_wavelength': 6230.,
                'width': 4183.},
               {'band': 'GAIA2.Grp',
                'instrument': 'Gaia',
                'telescope': 'Gaia',
                'effective_wavelength': 7730.,
                'width': 2757.}
               ]

db.PhotometryFilters.insert().execute(gaia_filters)
db.save_reference_table('Publications', 'data')
db.save_reference_table('Instruments', 'data')
db.save_reference_table('Telescopes', 'data')
db.save_reference_table('PhotometryFilters', 'data')


# MAY NOT USE THIS
def find_tmass_gaia_matches():
    # Find all sources in database with 2MASS designations
    tmass_sources = db.query(db.Names).filter(db.Names.c.other_name.like('2MASS J%')).all()
    n_tmass = len(tmass_sources)

    # only do a small subset
    tmass_sources = tmass_sources[0:100]

    # should only do unique 2MASS names
    # tmass_sources = unique(tmass_sources,keys='source')

    # make table of sources in database with 2MASS designations
    db_names = []
    tmass_source_ids = []
    for tmass_source in tmass_sources:
        db_names.append(tmass_source[0])
        # remove "2MASS J" to match with Gaia catalog id
        tmass_source_ids.append(tmass_source[1][7:23])

    table = Table(tmass_source,db_names,colnames=('',''))
    table.write('table.xml', overwrite=True)

    # Query Gaia catalog to get info on each 2MASS source
    i=0.
    for db_name, tmass_source in zip(db_names,tmass_source_ids):
        # query 2MASS and DR2 at the same time
        tmass_query_string = "SELECT *,upload_table.db_name FROM gaiadr2.gaia_source " \
                         "INNER JOIN gaiadr2.tmass_best_neighbour ON gaiadr2.gaia_source.source_id = gaiadr2.tmass_best_neighbour.source_id " \
                         "WHERE gaiadr2.tmass_best_neighbour.original_ext_source_id = '"+ tmass_source + "'"  # ('03552337+1133437','03550477-1032415')" #tap_upload.upload_table"
        #"WHERE gaiadr2.tmass_best_neighbour.original_ext_source_id IN (SELECT ?name? from upload_table)"
        # INNER JOIN tap_upload.upload_table ON  gaiadr2.tmass_best_neighbour.original_ext_source_id = tap_upload.upload_table.name
        job_tmass = Gaia.launch_job(tmass_query_string) # upload_resource='upload.xml', upload_table_name="upload_table", verbose=True)

        result = job_tmass.get_results()

        if len(result) > 0:
            # add database source name to results table
            result.add_column(db_name, name='SIMPLE_source', index=0)

            if i == 0:
                all_results = result
                i = 1
            else:
                all_results.add_row(result[0])

        out_file ='scripts/ingests/Gaia/tmass_gaia_results_test.xml'
        all_results.write(out_file, format='votable', overwrite=True)
        n_matches = len(all_results)
        print('Found', n_matches, 'Gaia sources for', n_tmass, ' 2MASS sources')
        print('Wrote table to ',out_file)
        return all_results


# Of those without Gaia designation, find 2MASS and WISE designations.
# tmass_sources = db.query(db.Names.c.other_name).filter(and_(db.Names.c.other_name.ilike("2MASS J%"),
#                                                        db.Names.c.source.in_(no_gaia_source_id))).table()


# read results from saved table
# tmass_matches = Table.read('scripts/ingests/Gaia/tmass_gaia_results.xml',format='votable')
# print("tmass matches ", len(tmass_matches))
#print(tmass_matches['SIMPLE_source','designation','pmra','parallax','phot_rp_mean_mag'])



