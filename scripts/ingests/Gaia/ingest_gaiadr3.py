from astroquery.gaia import Gaia
from astropy.table import Table, setdiff
from astropy import table
from sqlalchemy import func
import numpy as np
import pandas as pd
from astrodb_utils import load_astrodb
from astrodb_utils.sources import (
    find_source_in_db,
    AstroDBError,
    find_publication,
    ingest_source,
)

from astrodb_utils.publications import (
    ingest_publication,
    logger,
)
from simple import REFERENCE_TABLES
SCHEMA_PATH = "simple/schema.yaml"

# GLOBAL VARIABLES

SAVE_DB = True  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files
VERBOSE = False
#Changed from Jun2022 to Sep2021
DATE_SUFFIX = "Sep2021"
# LOAD THE DATABASE
db = load_astrodb("SIMPLE.sqlite", recreatedb=RECREATE_DB, reference_tables=REFERENCE_TABLES, felis_schema=SCHEMA_PATH)

#logger.setLevel(logging.DEBUG)


# Functions
# Querying GaiaDR3
def query_gaia_dr3(input_table):
    print("Gaia DR3 query started")
    gaia_query_string = (
        "SELECT *,upload_table.db_names FROM gaiadr3.gaia_source "
        "INNER JOIN tap_upload.upload_table ON "
        "gaiadr3.gaia_source.source_id = tap_upload.upload_table.dr3_source_id "
    )
    job_gaia_query = Gaia.launch_job(
        gaia_query_string,
        upload_resource=input_table,
        upload_table_name="upload_table",
        verbose=VERBOSE,
    )

    gaia_data = job_gaia_query.get_results()

    print("Gaia DR3 query complete")

    return gaia_data

GAIA_BIBCODE = "2021A&A...649A...1G"

# Ingesting the GAIADR3 publication
def update_ref_tables():
    ingest_publication(
        db,
        doi="10.1051/0004-6361/202243940",
        publication="GaiaDR3",
        description="Gaia Data Release 3.Summary of the content and survey properties",
        ignore_ads=True,
    )


# update_ref_tables()

ingested = 0
total = 0    

def add_gaia_coordinates_and_epochs(data, ref):
    
    global ingested
    global total
    
    for row in data:
        # source in the database
        source = find_source_in_db(db, row["db_names"])
        if source is None:
            logger.warning(f"Source {row['db_names']} not found in database")
            continue
        
        # coordinates and epoch from Gaia DR3 data
        ra = row["ra"]
        dec = row["dec"] 
        ref_epoch = row["ref_epoch"]
        
        if ra is np.ma.masked or dec is np.ma.masked:
            logger.warning(f"Coordinates are masked for {row['db_names']}")
            continue
        
        epoch_value = None if ref_epoch is np.ma.masked else ref_epoch
        
        try:
            with db.engine.connect() as conn:
                conn.execute(
                    db.Sources.update().where(db.Sources.c.source == source[0]).values(
                        {
                            "source": source[0],
                            "ra": ra,
                            "dec": dec,
                            "epoch": ref_epoch,
                            "equinox": None, 
                            "reference": ref,
                            "other_references": None,
                            "shortname": None,
                        }
                    )
                )
                conn.commit()
                
            ingested += 1
            total += 1
            print(f"Ingested coordinates for {row['db_names']}: RA={ra}, Dec={dec}, epoch={epoch_value}")
            
        except Exception as e:
            total += 1
            logger.warning(f"Could not ingest coordinates for {row['db_names']}: {e}")
    
    return

'''
dr3_desig_file_string = (
    "scripts/ingests/Gaia/gaia_dr3_designations_" + "Sep2021" + ".xml"
)
gaia_dr3_names = Table.read(dr3_desig_file_string, format="votable")
pd_gaia_dr3_names = gaia_dr3_names.to_pandas
'''

# Querying the GAIA DR3 Data
# gaia_dr3_data = query_gaia_dr3(gaia_dr3_names)

# making the data file and then converting the string into an astropy table

dr3_data_file_string = "scripts/ingests/Gaia/gaia_edr3_data_" + DATE_SUFFIX + ".xml"
gaia_dr3_data = Table.read(dr3_data_file_string, format="votable")

#ingest_sources(db, gaia_dr3_data['designation'], 'GaiaDR3')

add_gaia_coordinates_and_epochs(gaia_dr3_data, "GaiaDR3")
print("ingested: " + str(ingested))
print("total: " + str(total))

# WRITE THE JSON FILES
if SAVE_DB:
    db.save_database(directory="data/")
