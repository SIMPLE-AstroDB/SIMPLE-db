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

source_num_counter = 0
multiple_sources_counter = 0
no_sources_counter = 0
one_source = 0

one_source_names, multiple_source_names, no_source_names = [], [], []
one_source_PMRA, multiple_source_PMRA, no_source_PMRA = [], [], []
one_source_sigPMRA, multiple_source_sigPMRA, no_source_sigPMRA = [], [], []
one_source_PMDec, multiple_source_PMDec, no_source_PMDec = [], [], []
one_source_sigPMDec, multiple_source_sigPMDec, no_source_sigPMDec = [], [], []
one_source_w1mpro, multiple_source_w1mpro, no_source_w1mpro = [], [], []
one_source_w1sigmpro, multiple_source_w1sigmpro, no_source_w1sigmpro = [], [], []
one_source_w2mpro, multiple_w2mpro, no_source_w2mpro = [], [], []
one_source_w2sigmpro, multiple_source_w2sigmpro, no_source_w2sigmpro = [], [], []
one_source_ra, multiple_source_ra, no_source_ra = [], [], []
one_source_dec, multiple_source_dec, no_source_dec = [], [], []


for result in results:
    try:
        filtered_results = results[(results["ab_flags"] == '00') & (results["cc_flags"] == '0000')]
        if(len(filtered_results)>1):
            multiple_sources_counter += 1
            # Convert selected values to strings and join with commas
            row_string = ", ".join(str(filtered_results[i]) for i in indices if i < len(filtered_results)) 
            logger.info("source match found and added to csv file")
        else:        
            one_source += 1
            row_string = ", ".join(str(filtered_results[i]) for i in indices if i < len(filtered_results)) 
            logger.info("source match found and added to csv file")
        source_num_counter +=1
        print(source_num_counter)
    except IndexError:
        source_num_counter+=1
        no_sources_counter += 1
        row_string = ", ".join(str(filtered_results[i]) for i in indices if i < len(filtered_results)) 
        print(source_num_counter)
        logger.warning("no source match found")

logger.info("done")
    


