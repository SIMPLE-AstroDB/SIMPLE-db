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

# Define the indices you want to include
indices = [0, 1, 2, 3, 4, 7, 8, 9, 10, 11, 12]




SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files
SCHEMA_PATH = "simple/schema.yaml" 
# LOAD THE DATABASE
db = load_astrodb("SIMPLE.sqlite", recreatedb=RECREATE_DB, reference_tables=REFERENCE_TABLES, felis_schema=SCHEMA_PATH)

# generates all the sources in the db
sources_table = db.query(db.Sources).table()
print(sources_table)
coord_vector = SkyCoord(ra = sources_table["ra"], dec = sources_table["dec"], unit = "deg", frame = "icrs")
results = Irsa.query_region(coordinates=coord_vector, spatial='Cone', catalog='catwise_2020', radius=0.5 * u.arcmin, 
columns="source_name,PMRA,sigPMRA,PMDec,sigPMDec,ab_flags,cc_flags,w1mpro,w1sigmpro,w2mpro,w2sigmpro,ra,dec", verbose=True)

# should be 3598
logger.info(f"Found {len(sources_table["source"])} sources to process.")


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
                # Convert selected values to strings and join with commas
                row_string = ", ".join(str(filtered_results[i]) for i in indices if i < len(filtered_results)) 
                writer2.writerow([row_string]) 
                logger.info("source match found and added to csv file")
            else:        
                one_source += 1
                row_string = ", ".join(str(filtered_results[i]) for i in indices if i < len(filtered_results)) 
                writer1.writerow([row_string]) 
                logger.info("source match found and added to csv file")
            source_num +=1
            print(source_num)
        except IndexError:
            source_num+=1
            no_sources += 1
            row_string = ", ".join(str(filtered_results[i]) for i in indices if i < len(filtered_results)) 
            writer3.writerow([row_string]) 
            print(source_num)
            logger.warning("no source match found")
    logger.info("done")
    


