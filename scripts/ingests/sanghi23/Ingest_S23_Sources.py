# SIMPLE & Astrodb Packages
from astrodb_utils import load_astrodb, logger
from astrodb_utils.sources import ingest_source, find_source_in_db, ingest_names
from simple.schema import *
from simple.schema import REFERENCE_TABLES
import pandas as pd
# logger.setLevel("DEBUG")

db = load_astrodb("SIMPLE.sqlite", recreatedb=True, reference_tables=REFERENCE_TABLES)
path = "scripts/ingests/sanghi23/"

# Ingest New Sources
newsources = pd.read_csv(path + "NewSources-23.csv")  # Read in new sources
for _, source in newsources.iterrows():
    try:
        ingest_source(
            db,
            source=source['name'],
            ra=source['ra_j2000_formula'],
            dec=source['dec_j2000_formula'],
            reference=source['reference'],
        )
        print(f"Source {source['name']} ingested.")
    except Exception as e:  # noqa: E722
        print(f"Source {source['name']} already in database. Moving on. Error: {e}")
        continue

# Ingest Alternative Sources
ultracool = pd.read_csv(path + "Ultracool_Fundamental_Properties_Table.csv")
for _, source in ultracool.iterrows():
    existing_source = find_source_in_db(db, source['name'], 
                                        ra=source['ra_j2000_formula'],
                                        dec=source['dec_j2000_formula'],
                                        ra_col_name="ra",
                                        dec_col_name="dec")
    if existing_source:
        try:
            ingest_names(db, existing_source[0], source['name_simbadable'])
            print(f"Alternative name {source['name_simbadable']} ingested for existing source {source['name']}.")
        except Exception as e:
            print(f"Failed to ingest alternative name for {source['name']}: {e}")


# Save to Database
db.save_database(directory="data/")
