import logging
from astrodb_utils import load_astrodb, AstroDBError
from astrodb_utils.sources import find_source_in_db
from astrodb_utils.photometry import ingest_photometry, ingest_photometry_filter
from simple import REFERENCE_TABLES
import pandas as pd
import csv
import sqlalchemy

# Set the logging level of the astrodb_utils logger
astrodb_utils_logger = logging.getLogger("astrodb_utils")
astrodb_utils_logger.setLevel(logging.INFO)

# Set up the logging for pan_starrs.
logger = logging.getLogger("astrodb_utils.pan_starrs")
logger.setLevel(logging.INFO) 

# load database
recreate_db = True
save_db = False

SCHEMA_PATH = "simple/schema.yaml"
db = load_astrodb(
    "SIMPLE.sqlite",
    recreatedb = recreate_db,
    reference_tables = REFERENCE_TABLES,
    felis_schema = SCHEMA_PATH
)

excel_path = "scripts/ingests/Pan-STARRS/Pan-STARRS Photometry.xlsx"
data = pd.read_excel(excel_path)

csv_output1 = "scripts/ingests/Pan-STARRS/valid_photometry.csv"
csv_output2 = "scripts/ingests/Pan-STARRS/invalid_photometry.csv"

# ingest Photometry Filters: PAN-STARRS/PS1.g, PAN-STARRS/PS1.r, PAN-STARRS/PS1.i, PAN-STARRS/PS1.z, PAN-STARRS/PS1.y
def ingest_PanSTARRS_photometry_filters():
    ugriz = ['g', 'r', 'i', 'z', 'y']
    for band in ugriz:
        try:
            ingest_photometry_filter(
                db,
                telescope="Pan-STARRS",
                instrument="PS1",
                filter_name=f"{band}",
                wavelength_col_name="effective_wavelength",
                width_col_name="width"
            )
            logger.info(f"Photometry filter PS1.{band} added successfully.")
        except AstroDBError as e:
            if "already exists" in str(e):
                logger.warning(f"Photometry filter PS1.{band} already exists.")
            else:
                logger.error(f"Error adding photometry filter PS1.{band}: {e}")


# Ingest Photometry: Add different bands depends on the ugriz magnitude available in the datasets
# process data in chunks as dataset is large
def ingest_PanSTARRS_photometry(data, start_idx=0, chunk_size=0):
    source_added = 0
    skipped = 0
    inaccessible = 0
    raise_error = False
    successful_source = []
    failed_source = []
    band_counts = {band: 0 for band in ['g', 'r', 'i', 'z', 'y']}

    end_idx = min(start_idx + chunk_size, len(data))
    data_chunk = data.iloc[start_idx:end_idx]

    for _, row in data_chunk.iterrows():
        source = row["source"]
        try:
            db_name = find_source_in_db(
                db, 
                source
            )
            
            if len(db_name) != 1:
                msg = f"No unique source match for {source} in the database"
                skipped += 1
                failed_source.append({"source":source, "reason": "No unique match"})
                logger.warning(msg)
                continue
            else:
                db_name = db_name[0]

            for band in ['g', 'r', 'i', 'z', 'y']:
                mag_col = f"{band}mag"
                err_col = f"e_{band}mag"
                if not pd.isna(row[f"{band}mag"]):
                    photometry_data = {
                        "source": db_name,
                        "band": f"PAN-STARRS/PS1.{band}",
                        "magnitude": row[mag_col],
                        "magnitude_error": row[err_col],
                        "telescope": "Pan-STARRS",
                        "epoch": None,
                        "comments": None,
                        "reference": "Best18",
                    }
                    successful_source.append(photometry_data)
                    band_counts[band] += 1
                    
            source_added += 1        
            logger.info(f"collecting photometry for {source}")

        except Exception as e:
            msg = f"Error adding {source} photometry: {e}"
            if "None of [Index(['ra_deg', 'dec_deg']" in str(e):
                inaccessible += 1
                failed_source.append({"source": source, "reason": str(e)})
                logger.warning(msg)
            else:
                inaccessible += 1
                failed_source.append({"source": source, "reason": str(e)})
                logger.error(msg)

    # Store valid source into csv
    with open(csv_output1, "w", newline='') as f:
        fieldnames = [
            "source",
            "band",
            "magnitude",
            "magnitude_error",
            "telescope",
            "epoch",
            "comments",
            "reference"
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(successful_source)

    # Store invalid ingestion sources
    with open(csv_output2, "w", newline='') as f:
        writer = csv.DictWriter(f, fieldnames=["source", "reason"])
        writer.writeheader()
        writer.writerows(failed_source)

    logger.info(f"Total source added: {source_added}")
    logger.info(f"Total source skipped: {skipped}")
    logger.info(f"Total inaccessible data: {inaccessible}")
    for band, count in band_counts.items():
        logger.info(f"Total entries for PS1.{band} band: {count}")

    """
    log output:
        INFO     - astrodb_utils.pan_starrs - Total source added: 2084
        INFO     - astrodb_utils.pan_starrs - Total source skipped: 497
        INFO     - astrodb_utils.pan_starrs - Total inaccessible data: 7307
        INFO     - astrodb_utils.pan_starrs - Total entries for PS1.g band: 364
        INFO     - astrodb_utils.pan_starrs - Total entries for PS1.r band: 894
        INFO     - astrodb_utils.pan_starrs - Total entries for PS1.i band: 1556
        INFO     - astrodb_utils.pan_starrs - Total entries for PS1.z band: 1938
        INFO     - astrodb_utils.pan_starrs - Total entries for PS1.y band: 2050
    """


# make ingestion from valid data csv file after review
def ingest_from_csv(csv_path):
    with open(csv_path, 'r') as f:
        rows = csv.DictReader(f)
        for row in rows:
            try:
                with db.engine.begin() as conn:
                    conn.execute(db.Photometry.insert().values(row))
                logger.info(f"Added photometry for {row['source']}")
            except sqlalchemy.exc.IntegrityError as e:
                logger.error(f"Error adding photometry for {row['source']}: {e}")


# Call ingestion function
#ingest_PanSTARRS_photometry_filters()
                
ingest_PanSTARRS_photometry(data,0,10000)

#ingest_from_csv(csv_path="scripts/ingests/Pan-STARRS/successful_sources_PS1.csv")



# save to database
if save_db:
    db.save_database(directory="data/")
    logger.info("Pan-STARRS Photometry Database saved as SIMPLE.sqlite")