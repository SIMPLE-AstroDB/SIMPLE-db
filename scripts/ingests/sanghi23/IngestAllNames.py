# SIMPLE & Astrodb Packages
from astrodb_utils import load_astrodb, logger
from astrodb_utils.sources import ingest_source, find_source_in_db, ingest_name
from astrodb_utils.publications import ingest_publication
from simple.schema import *
from simple.schema import REFERENCE_TABLES
import pandas as pd
import os
# logger.setLevel("DEBUG")

# Load Database
recreate_db = False
save_db = True
db = load_astrodb("SIMPLE.sqlite", recreatedb=recreate_db, reference_tables=REFERENCE_TABLES)
path = "scripts/ingests/sanghi23/"


allsources = pd.read_csv(os.path.join(path, "Ultracool_Fundamental_Properties_Table.csv")) # Read entire table
for i, source in allsources.iterrows():
    db_source = find_source_in_db(
        db, 
        source['name'], 
        ra=source['ra_j2000_formula'], 
        dec=source['dec_j2000_formula'], 
        ra_col_name="ra", 
        dec_col_name="dec"
    ) or find_source_in_db(
        db, 
        source['name_simbadable'],  
        ra=source['ra_j2000_formula'], 
        dec=source['dec_j2000_formula'], 
        ra_col_name="ra", 
        dec_col_name="dec"
    )
    
    if db_source:
        alt_names = [source["name_simbadable"], source["name"]]
        for name in alt_names:
            if name != db_source[0] and name != "Null":
                ingest_name(db, db_source[0], name.strip())


# Save to Database, Writes the JSON Files
if save_db: 
    db.save_database(directory="data/")
