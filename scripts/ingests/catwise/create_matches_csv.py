from astrodb_utils import load_astrodb
import sys
sys.path.append(".")
from simple import *
from simple import REFERENCE_TABLES
from astropy.io import ascii
from astrodb_utils.publications import (
    logger,
    find_publication,
    ingest_publication
)
from astrodb_utils.sources import (
    find_source_in_db,
    ingest_source,
    AstroDBError
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

matches=[]

for row in sources:
    #create skycoord object because one of the parameters for query region for position
    coord = SkyCoord(ra = row["ra"], dec = row["dec"], unit = "deg", frame = "icrs")

    # generates a list of objects from the catwise2020 catalogs that are within this radius of a certain position/coordinate
    results = Irsa.query_region(coordinates=coord, spatial='Cone', catalog='catwise_2020', radius=1 * u.arcmin, columns="source_name,PMRA,sigPMRA,PMDec,sigPMDec,ab_flags,cc_flags,w1mpro,w1sigmpro,w2mpro,w2sigmpro,ra,dec")

    # generates a list of filtered results
    filtered_results = []
    for source in results:
        if (source["ab_flags"] == '00') & (source["cc_flags"] == '0000'):
            filtered_results.append(source["source_name"])

    if(len(filtered_results)==0):
        continue
    matches.append(results[results["source_name"]==filtered_results[0]])
           
