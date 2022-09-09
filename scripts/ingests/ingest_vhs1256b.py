from scripts.ingests.ingest_utils import *
from scripts.ingests.utils import *
from astroquery.gaia import Gaia

def get_gaiadr3(db, gaia_id):
    """ To be moved to ingest_utils """
    gaia_query_string = f"SELECT * FROM gaiadr3.gaia_source WHERE " \
                        "gaiadr3.gaia_source.designation = {gaia_id}"
    job_gaia_query = Gaia.launch_job(gaia_query_string, verbose=True)

    gaia_data = job_gaia_query.get_results()

    return gaia_data

SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files

logger.setLevel(logging.DEBUG)

db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)

source = '2MASS J12560183-1257276'

# Add other name
other_name_data = [{'source': source, 'other_name': 'VHS 1256-1257b'}]
db.Names.insert().execute(other_name_data)

# TODO: add Gaia and WISE designations as other names.

# Ingesting missing publications
ingest_publication(db, bibcode='2018ApJ...869...18M')

# Ingest spectral type
ingest_spectral_types(db,
                      source=source,
                      spectral_types=87, # should be L7, not sure what the code is
                      spectral_type_error=1.5,
                      references='Gauz15')

# ingest optical type of L8 pm 2
# other spectral types from Miles 2018?

# Ingest spectra
#ingest_spectra(db, source, data['spectrum'], 'mir', 'Spitzer', 'IRS', 'SL',
#               data['observation_date'],
#               'Suar22', original_spectra=data['original_spectrum'], wavelength_units='um', flux_units='Jy', comments=data['spectrum comments'])

# ingest Gauz15 spectrum
# ingest Miles18 spectrum
# ingest Miles18 version of Gauz15 data

# TODO: ingest SDSS photometry
from astroquery.sdss import SDSS
SDSS_id = 'blah'
sdss_query = f"SELECT psfMag_u, psfMag_g, psfMag_r, psfMag_i, psfMag_z " \
             "FROM  PhotoTag " \
             "WHERE objid = {SDSS_id}"
sdss_data = SDSS.query_sql(sdss_query)

# TODO: ingest WISE photometry: WISEA J125601.66-125728.7
# TODO: ingest VISTA photometry
# TODO: ingest 2MASS photometry 2MASS J12560183-1257276 - or not?

# TODO: ingest Gaia parallax from primary 2MASS J12560215-1257217, Gaia DR3 3526198184723289472
gaia_data = get_gaiadr3(db, '3526198184723289472')
ingest_gaia_parallaxes(db, gaia_data, 'GaiaDR3')

# WRITE THE JSON FILES
if SAVE_DB:
    db.save_database(directory='data/')