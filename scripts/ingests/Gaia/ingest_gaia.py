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


SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files
VERBOSE = True

verboseprint = print if VERBOSE else lambda *a, **k: None


def load_db():
    # Utility function to load the database

    db_file = 'SIMPLE.db'
    db_file_path = Path(db_file)
    db_connection_string = 'sqlite:///SIMPLE.db'  # SQLite browser

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

    # Query Gaia catalog to get info on each 2MASS source
    i=0.
    for db_name, tmass_source in zip(db_names,tmass_source_ids):
        # query 2MASS and DR2 at the same time
        tmass_query_string = "SELECT * FROM gaiadr2.gaia_source " \
                         "INNER JOIN gaiadr2.tmass_best_neighbour ON gaiadr2.gaia_source.source_id = gaiadr2.tmass_best_neighbour.source_id " \
                         "WHERE gaiadr2.tmass_best_neighbour.original_ext_source_id = '"+ tmass_source + "'"  # ('03552337+1133437','03550477-1032415')" #tap_upload.upload_table"

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

# re-run the query. Takes a little while
# all_results = find_tmass_gaia_matches()

# read results from saved table
tmass_matches = Table.read('scripts/ingests/Gaia/tmass_gaia_results.xml',format='votable')
print(len(tmass_matches))

#only ingest unique sources
tmass_matches_unique = unique(tmass_matches[501:600],keys=['SIMPLE_source','designation'],keep='none')

#print(tmass_matches['SIMPLE_source','designation','pmra','parallax','phot_rp_mean_mag'])

add_names(db, sources=tmass_matches_unique['SIMPLE_source'], other_names=tmass_matches_unique['designation'], verbose=True)



# TODO: if no gaia id, query around RA/dec
# coord = SkyCoord(ra=280, dec=-60, unit=(u.degree, u.degree), frame='icrs')
# radius = u.Quantity(1.0, u.deg)
# j = Gaia.cone_search_async(coord, radius)
# r = j.get_results()
