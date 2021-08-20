from astrodbkit2.astrodb import create_database
from astrodbkit2.astrodb import Database
from scripts.ingests.utils import *
from simple.schema import *
from pathlib import Path
from astroquery.gaia import Gaia


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

# Select which Gaia catalog to query
# tables = Gaia.load_tables(only_names=True)
# for table in (tables):
#   print (table.get_qualified_name())
# Gaia.MAIN_GAIA_TABLE = "gaiadr2.gaia_source"  # Select Data Release 2, default
# Gaia.MAIN_GAIA_TABLE = "gaiaedr3.gaia_source" # Select early Data Release 3
# Gaia.MAIN_GAIA_TABLE = "gaiadr2.tmass_best_neighbour"
# gaiaedr3.tmass_psc_xsc_best_neighbour

#load table and inspect columns
# table = Gaia.load_table(Gaia.MAIN_GAIA_TABLE)
# for column in (table.columns):
#    print(column.name)

# find sources without Gaia IDs
gaia_sources = db.query(db.Names).filter(db.Names.c.other_name.like('Gaia%')).all()

# make table of sources in database with 2MASS designations
#find 2MASS id and remove "2MASS J"
tmass_sources = db.query(db.Names).filter(db.Names.c.other_name.like('2MASS J%')).all()
tmass_source_ids = []
for row in tmass_sources:
    tmass_source_ids.append(row[1][7:23])

source = '12195156+3128497'

# TODO: upload table and find all gaia IDs given 2MASS IDs
# QUESTION: votable vs votable_plain
tmass_query_string = "SELECT source_id,original_ext_source_id " \
                     "FROM gaiadr2.tmass_best_neighbour " \
                     "WHERE original_ext_source_id= '" + source +"'"
job_tmass = Gaia.launch_job(tmass_query_string)

results_tmass = job_tmass.get_results()
gaiadr2_source_id = results_tmass[0]['source_id']  #3303349202364648320

#add Gaia IDs to names table

#if gaia id, query using it.
dr2_query_string = "SELECT ra, dec," \
                   "parallax, parallax_error," \
                   "pmra, pmra_error, pmdec, pmdec_error," \
                   "phot_g_mean_mag, phot_bp_mean_mag, phot_rp_mean_mag," \
                    "radial_velocity, radial_velocity_error " \
                    "FROM gaiadr2.gaia_source " \
                    "WHERE source_id = '" + gaiadr2_source_id + "'"
job_dr2 = Gaia.launch_job(dr2_query_string)

results_dr2 = job_dr2.get_results()



# TODO: if no gaia id, query around RA/dec
# coord = SkyCoord(ra=280, dec=-60, unit=(u.degree, u.degree), frame='icrs')
# radius = u.Quantity(1.0, u.deg)
# j = Gaia.cone_search_async(coord, radius)
# r = j.get_results()
