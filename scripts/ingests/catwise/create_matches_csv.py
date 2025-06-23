from astrodb_utils import load_astrodb
import sys
sys.path.append(".")
from simple import *
from simple import REFERENCE_TABLES
from astrodb_utils.publications import (
    logger
)
from sqlalchemy import select
from astroquery.ipac.irsa import Irsa #we will use astroquery 
from astropy.coordinates import SkyCoord
import astropy.units as u
import os
import csv


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



csv_file = "catwise_matches.csv"
write_header = not os.path.exists(csv_file)  # Only write header if the file doesn't exist

# Open CSV file in append mode
with open(csv_file, mode="a", newline="") as f:
    writer = csv.writer(f)

    # Write the header once if file doesn't exist
    if write_header:
        writer.writerow(["source_name", "ra", "dec", "PMRA", "sigPMRA", "PMDec", "sigPMDec", "w1mpro", "w1sigmpro", "w2mpro", "w2sigmpro"])

    source_num = 0
    for source in sources:
        #create skycoord object because one of the parameters for query region for position
        coord = SkyCoord(ra = source["ra"], dec = source["dec"], unit = "deg", frame = "icrs")

        # generates a list of objects from the catwise2020 catalogs that are within this radius of a certain position/coordinate
        results = Irsa.query_region(coordinates=coord, spatial='Cone', catalog='catwise_2020', radius=0.5 * u.arcmin, columns="source_name,PMRA,sigPMRA,PMDec,sigPMDec,ab_flags,cc_flags,w1mpro,w1sigmpro,w2mpro,w2sigmpro,ra,dec")

        try:
            filtered_results = results[(results["ab_flags"] == '00') & (results["cc_flags"] == '0000')][0]
            writer.writerow([
                filtered_results[0],
                filtered_results[1],
                filtered_results[2],
                filtered_results[3],
                filtered_results[4],
                filtered_results[7],
                filtered_results[8],
                filtered_results[9],
                filtered_results[10],
                filtered_results[11],
                filtered_results[12]
            ])
            logger.info("source match found and added to csv file")
            source_num +=1
            print(source_num)
        except IndexError:
            source_num+=1
            print(source_num)
            logger.warning("no source match found")
    logger.info("done")
    


