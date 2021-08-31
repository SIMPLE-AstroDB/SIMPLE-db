import os
from astrodbkit2.astrodb import create_database
from astrodbkit2.astrodb import Database
from scripts.ingests.utils import *
from simple.schema import *
from pathlib import Path
from astroquery.gaia import Gaia
from astropy.table import Table, Column, unique
from astropy.io.votable import from_table, writeto
#from astropy.utils.console import ProgressBar
from astroquery.simbad import Simbad
import time
from tqdm import tqdm


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


# TODO: Query database for Sources with no Gaia designation
# get all sources that have Gaia DR2
# 2nd query which is all sources that are not in that list
gaia_sources = db.query(db.Names.c.source).filter(db.Names.c.other_name.ilike("Gaia DR2%")).table()
if gaia_sources:
    gaia_sources_list = gaia_sources['source'].tolist()
no_gaia_source_id = db.query(db.Sources.c.source).filter(db.Sources.c.source.notin_(gaia_sources_list)).all()


# Of those without Gaia designation, find 2MASS and WISE designations.

# Should we be using Simbad to check all sources for Gaia designation?
# and then those that don't have 2MASS, WISE, PanStarrs designations.
# Use Simbad to find Gaia or 2MASS designation

# TODO: Query SIMBAD for Gaia designations
# should submit a list of names and do one query
def find_gaia_in_simbad():
    sources = db.query(db.Sources.c.source).table()
    sources=sources[0:10]
    n_sources = len(sources)

    # TODO: learn more about how to add my input column to the output of query_objectids
    Simbad.add_votable_fields('typed_id')
    result_table = Simbad.query_objectids(sources)

    db_names = []
    simbad_gaia_designation = []

    for source in tqdm(sources, total=len(sources),desc='Gaia IDs'):
        time.sleep(0.16)
        if result_table:
            gaia_designation = result_table['ID'][[name.startswith('Gaia DR2') for name in result_table['ID']]]
        else:
            gaia_designation = None

        if gaia_designation:
            db_names.append(source)
            simbad_gaia_designation.append(gaia_designation[0])

    n_matches = len(db_names)
    print('Found', n_matches, 'Gaia sources for', n_sources, ' sources')

    print(simbad_gaia_designation)

find_gaia_in_simbad()

#

# TODO: change script to focus on getting Gaia designations from multiple sources.

# TODO: use all gaia desginations to get Gaia data

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

# TODO: if no 2MASS id, query around RA/dec
# coord = SkyCoord(ra=280, dec=-60, unit=(u.degree, u.degree), frame='icrs')
# radius = u.Quantity(1.0, u.deg)
# j = Gaia.cone_search_async(coord, radius)
# r = j.get_results()

# re-run the query. Takes a little while
# all_results = find_tmass_gaia_matches()

# read results from saved table
tmass_matches = Table.read('scripts/ingests/Gaia/tmass_gaia_results.xml',format='votable')
print("tmass matches ", len(tmass_matches))
#print(tmass_matches['SIMPLE_source','designation','pmra','parallax','phot_rp_mean_mag'])

# only ingest unique sources
# TODO: Figure out what the three sources which aren't unique. Probably secondaries with AB suffixes.
#tmass_matches_unique = unique(tmass_matches,keys=['SIMPLE_source','designation'],keep='none')

#add_names(db, sources=tmass_matches_unique['SIMPLE_source'], other_names=tmass_matches_unique['designation'], verbose=True)




