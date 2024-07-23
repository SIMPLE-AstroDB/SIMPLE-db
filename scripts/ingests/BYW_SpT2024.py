from astrodb_utils import load_astrodb
from astrodb_utils import ingest_publication
from simple.schema import *
from simple.schema import REFERENCE_TABLES
from astropy.io import ascii
from simple.utils.spectral_types import ingest_spectral_type
import logging

logger = logging.getLogger("SIMPLE")
logger.setLevel(logging.INFO)
SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files
# LOAD THE DATABASE
db = load_astrodb("SIMPLE.sqlite", recreatedb=RECREATE_DB, reference_tables=REFERENCE_TABLES)
ingest_publication(db, doi='10.3847/1538-3881/ad324e' )
# Load Google sheet

link = 'scripts/ingests/Austin-BYW-Benchmark-SpT.csv'

# read the csv data into an astropy table
# ascii.read attempts to read data from local files rather from URLs so using a library like requests helps get data and create object that can be passed to ascii.read
byw_table = ascii.read(
    link, #change this to the path for the csv file
    format="csv",
    data_start=1,
    header_start=0,
    guess=False,
    fast_reader=False,
    delimiter=",",
)

for row in byw_table:  # skip the header row - [1:10]runs only first 10 rows
    if row["photometric"] == 'True': 
        photometric = True
    else:
        photometric = False

    preexisting = ['CWISE J183207.94-540943.3',
                   'SDSS J134403.83+083950.9']
    if row["Source"] in preexisting:
        continue

    ingest_spectral_type(
            db,
            source=row["Source"],
            spectral_type_string=row["spectral_type_string"],
            spectral_type_error=row["spectral_type_error"],
            regime=row["regime"],
            comments=row["comments"],
            photometric=photometric,
            reference=row["Reference"],
            raise_error=False,
        ) 
    
    # WRITE THE JSON FILES
if SAVE_DB:
    db.save_database(directory="data/")