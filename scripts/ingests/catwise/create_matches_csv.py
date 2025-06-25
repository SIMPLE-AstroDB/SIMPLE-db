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
import numpy as np
import pandas as pd


SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = False  # recreates the .db file from the data files
SCHEMA_PATH = "simple/schema.yaml" 
# LOAD THE DATABASE
db = load_astrodb("SIMPLE.sqlite", recreatedb=RECREATE_DB, reference_tables=REFERENCE_TABLES, felis_schema=SCHEMA_PATH)

# generates all the sources in the db
sources_table = select(db.Sources)
with db.engine.connect() as conn:
    sources = conn.execute(sources_table).mappings().all()

sources_df = pd.DataFrame(sources)


coord_vector = SkyCoord(ra = sources_df['ra'], dec = sources_df['dec'], unit = "deg", frame = "icrs")
print(coord_vector)
results = Irsa.query_region(coordinates=coord_vector, spatial='Cone', catalog='catwise_2020', radius=0.5 * u.arcmin, 
columns="source_name,PMRA,sigPMRA,PMDec,sigPMDec,ab_flags,cc_flags,w1mpro,w1sigmpro,w2mpro,w2sigmpro,ra,dec")

# should be 3598
logger.info(f"Found {len(sources)} sources to process.")



one_match_csv = "catwise_matches.csv"
multiple_matches_csv = "catwise_multiple_matches.csv"
no_matches_csv = "catwise_no_matches.csv"
write_header1 = not os.path.exists(one_match_csv)  # Only write header if the file doesn't exist
write_header2 = not os.path.exists(multiple_matches_csv)
write_header3 = not os.path.exists(no_matches_csv)

# Open CSV file in append mode
with open(one_match_csv, mode="a", newline="") as f1, open(multiple_matches_csv, mode="a", newline="") as f2, open(no_matches_csv, mode="a", newline="") as f3:
    writer1 = csv.writer(f1)
    writer2 = csv.writer(f2)
    writer3 = csv.writer(f3)

    # Write the header once if file doesn't exist
    if write_header1:
        writer1.writerow(["source_name", "ra", "dec", "PMRA", "sigPMRA", "PMDec", "sigPMDec", "w1mpro", "w1sigmpro", "w2mpro", "w2sigmpro"])
    if write_header2:
        writer2.writerow(["source_name", "ra", "dec", "PMRA", "sigPMRA", "PMDec", "sigPMDec", "w1mpro", "w1sigmpro", "w2mpro", "w2sigmpro"])
    if write_header3:
        writer3.writerow(["source_name", "ra", "dec", "PMRA", "sigPMRA", "PMDec", "sigPMDec", "w1mpro", "w1sigmpro", "w2mpro", "w2sigmpro"])

    source_num = 0
    multiple_sources = 0
    no_sources = 0
    one_source = 0
    for result in results:
        
        try:
            filtered_results = results[(results["ab_flags"] == '00') & (results["cc_flags"] == '0000')]
            if(len(filtered_results)>1):
                multiple_sources += 1
                for filtered in filtered_results:
                    writer2.writerow([
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
            else:        
                one_source += 1
                writer1.writerow([
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
            no_sources += 1
            writer3.writerow([
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
            print(source_num)
            logger.warning("no source match found")
    logger.info("done")
    


