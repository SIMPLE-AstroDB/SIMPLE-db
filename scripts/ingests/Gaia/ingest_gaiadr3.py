from scripts.ingests.ingest_utils import ingest_radial_velocities
from scripts.ingests.utils import *
from astroquery.gaia import Gaia
from astropy.table import Table, setdiff
from astropy import table
from sqlalchemy import func
import numpy as np

# GLOBAL VARIABLES
SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files
VERBOSE = False
DATE_SUFFIX = 'Sep2021'

# LOAD THE DATABASE
db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)

logger.setLevel(logging.DEBUG)

# input_table = 'radial_velocity'

# Functions
# USE DR2 DESIGNATIONS TO GET DR3 DESIGNATIONS
# gaiadr3_names = query_gaia_dr3(dr2_desig_file_string)
dr3_desig_file_string = 'scripts/ingests/Gaia/gaia_dr3_designations_' + DATE_SUFFIX + '.xml'
# gaiadr3_names.write(dr3_desig_file_string, format='votable', overwrite=True)
gaiadr3_names = Table.read(dr3_desig_file_string, format='votable')


def query_gaia_dr3(input_table):
    print('Gaia DR3 query started')
    gaia_query_string = "SELECT *,upload_table.db_names FROM gaiadr3.gaia_source" \
                        "INNER JOIN tap_upload.upload_table ON " \
                        "gaiadr3.gaia_source.designation = tap_upload.upload_table.designation"
    job_gaia_query = Gaia.launch_job(gaia_query_string, upload_resource=input_table,
                                     upload_table_name="upload_table", verbose=VERBOSE)

    gaia_data = job_gaia_query.get_results()

    print('Gaia DR3 query complete')

    return gaia_data


def update_ref_tables():
    ingest_publication(db, doi='10.1051/0004-6361/202243940', publication='GaiaDR3',
                       description='Gaia Data Release 3.Summary of the content and survey properties', ignore_ads=True)


update_ref_tables()

#
# def add_gaia_rvs(gaia_data, ref):
#     unmasked_rvs = np.logical_not(gaia_data['radial_velocity'].mask).nonzero
#     rvs = gaia_data[unmasked_rvs]['gaiadr3_names', 'radial_velocity', 'radial_velocity_error']
#     refs = [ref] * len(rvs)
#     ingest_radial_velocities(db, rvs['db_names'], rvs['radial_velocity'], rvs['radial_velocity_error'], refs,
#                              verbose=VERBOSE)
#     return


# add_gaia_rvs(gaiadr3_names, 'GaiaDR3')
