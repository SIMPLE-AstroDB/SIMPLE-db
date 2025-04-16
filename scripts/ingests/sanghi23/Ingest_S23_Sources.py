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


# Workflow: Ingest pub, ingest sources, ingest parameters, ingest alt names functions ? ----

# Ingest Publications ---
ingest_publication(
    db,
    doi = "10.3847/1538-4357/accf9d"

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
            epoch = None,
            ra_col_name = "ra",
            dec_col_name = "dec",
            epoch_col_name="epoch"
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
print(f"Ingesting alternative names for {n_added} sources.")
for _, source in newsources.iterrows():
    if source['name_simbadable'] != "Null":
        try:
            ingest_name(
                db,
                source=source['name'],
                other_name=source['name_simbadable'],
            )
            print(f"Alternative name {source['name_simbadable']} ingested for existing source {source['name']}.")
        except Exception as e:
            print(f"Failed to ingest alternative name for {source['name']}: {e}")

    else:
        print(f"Source {source['name']} does not have an alternative name.")

# Save to Database, Writes the JSON Files
if save_db: 
    db.save_database(directory="data/")
