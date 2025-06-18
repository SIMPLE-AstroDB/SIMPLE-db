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
    results = Irsa.query_region(coordinates=coord, spatial='Cone', catalog='catwise_2020', radius=2 * u.arcmin, columns="source_name,PMRA,sigPMRA,PMDec,sigPMDec,ab_flags,cc_flags,w1mpro,w1sigmpro,w2mpro,w2sigmpro")
    print(results)
    # generates a list of filtered results
    filtered_results = []
    for source in results:
       if (source["ab_flags"] == '00') & (source["cc_flags"] == '0000'):
        filtered_results.append(source["source_name"])
    
    print(filtered_results)
    #use closest/best match
    match = filtered_results[0]
    logger.info("source match found")


    source_row = results[results["source_name"] == match]
    print(source_row)
    print("\n")
    print(source_row[0])

    with db.engine.connect() as conn:
        #ingest proper motions
        conn.execute(
                db.ProperMotions.insert().values(
                    {
                        "source": source_row["source_name"],
                        "mu_ra": source_row["pmra"],
                        "mu_ra_error": source_row["sigpmra"],
                        "mu_dec": source_row["pmdec"],
                        "mu_dec_error": source_row["sigpmdec"]
                    }
                )
        )
        #ingest WISE W1 photometry
        conn.execute(
                db.Photometry.insert().values(
                    {
                        "source": source_row["source_name"],
                        "band": 1,
                        "magnitude": source_row["w1mpro"],
                        "magnitude_error": source_row["w1sigmpro"],
                    }
                )
        )
        #ingest WISE W2 photometry
        conn.execute(
                db.Photometry.insert().values(
                    {
                        "source": source_row["source_name"],
                        "band": 2,
                        "magnitude": source_row["w2mpro"],
                        "magnitude_error": source_row["w2sigmpro"],
                    }
                )
        )
        conn.commit()

logger.info("done")


