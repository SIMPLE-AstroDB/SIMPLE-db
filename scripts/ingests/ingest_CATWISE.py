from astrodb_utils import load_astrodb
import sys
sys.path.append(".")
from simple import *
from simple import REFERENCE_TABLES
from astropy.io import ascii
from astrodb_utils.publications import (
    logger
)
from sqlalchemy import select
from astroquery.ipac.irsa import Irsa #we will use astroquery 
from astropy.coordinates import SkyCoord
import astropy.units as u


SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files
SCHEMA_PATH = "simple/schema.yaml" 
# LOAD THE DATABASE
db = load_astrodb("SIMPLE.sqlite", recreatedb=RECREATE_DB, reference_tables=REFERENCE_TABLES, felis_schema=SCHEMA_PATH)

# generates all the sources in the db
sources_table = select(db.Sources)
with db.engine.connect() as conn:
    sources = conn.execute(sources_table).mappings().all()

# should be 3598
logger.info(f"Found {len(sources)} sources to process.")



for row in sources:
    #create skycoord object because one of the parameters for query region for position
    coord = SkyCoord(ra = row["ra"], dec = row["dec"], unit = "deg", frame = "icrs")

    # generates a list of objects from the catwise2020 catalogs that are within this radius of a certain position/coordinate
    results = Irsa.query_region(coordinates=coord, spatial='Cone', catalog='catwise_2020', radius=2 * u.arcmin)


    # generates a list of filtered results
    filtered_results = results[(results["ab_flags"] == "00") & (results["cc_flags"] == "0000")]
    print(filtered_results)
    #use closest/best match
    match = filtered_results[0]
    print(match)

    break

logger.info("done")


