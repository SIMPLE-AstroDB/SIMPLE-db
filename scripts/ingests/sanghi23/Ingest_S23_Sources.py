# SIMPLE & Astrodb Packages
from astrodb_utils import load_astrodb, logger
from astrodb_utils.sources import ingest_source, find_source_in_db, ingest_names
from astrodb_utils.publications import ingest_publication
from simple.schema import *
from simple.schema import REFERENCE_TABLES
import pandas as pd
import os
# logger.setLevel("DEBUG")

# Load Database
recreate_db = False
save_db = False
db = load_astrodb("SIMPLE.sqlite", recreatedb=recreate_db, reference_tables=REFERENCE_TABLES)
path = "scripts/ingests/sanghi23/"


# Workflow: Ingest pub, ingest sources, ingest alt names functions ----

# Ingest Publications ---
ingest_publication(
    db,
    doi = "10.3847/1538-4357/ad0b12"
)


# Ingest New Sources ---
# Expecting to ingest 43 new sources
n_added = 0
n_skipped = 0 

newsources = pd.read_csv(os.path.join(path, "NewSources-23.csv")) # Read in new sources
for _, source in newsources.iterrows():
    try:
        ingest_source(
            db,
            source=source['name'],
            ra=source['ra_j2000_formula'],
            dec=source['dec_j2000_formula'],
            reference=source['ref_discovery'],
            ra_col_name = "ra",
            dec_col_name = "dec"
        )
        print(f"Source {source['name']} ingested.")
        n_added += 1
    except Exception as e: 
        print(f"Error: {e}")
        n_skipped += 1
        continue

print(f"Total sources add: {n_added}/43")
print(f"Total sources skipped: {n_skipped}/43")


# # Ingest Alternative Sources ---
# ultracool = pd.read_csv(path + "Ultracool_Fundamental_Properties_Table.csv")
# for _, source in ultracool.iterrows():
#     existing_source = find_source_in_db(db, source['name'], 
#                                         ra=source['ra_j2000_formula'],
#                                         dec=source['dec_j2000_formula'],
#                                         ra_col_name="ra",
#                                         dec_col_name="dec")
#     if existing_source:
#         try:
#             ingest_names(db, existing_source[0], source['name_simbadable'])
#             print(f"Alternative name {source['name_simbadable']} ingested for existing source {source['name']}.")
#         except Exception as e:
#             print(f"Failed to ingest alternative name for {source['name']}: {e}")


# Save to Database, Writes the JSON Files
if save_db: 
    db.save_database(directory="data/")
