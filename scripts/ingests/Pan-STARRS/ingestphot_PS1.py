import logging
from astrodb_utils import load_astrodb, AstroDBError
from astrodb_utils.sources import find_source_in_db, ingest_source
from astrodb_utils.photometry import ingest_photometry, ingest_photometry_filter
from simple import REFERENCE_TABLES
import pandas as pd
import csv
import sqlalchemy

# Set the logging level of the astrodb_utils logger
astrodb_utils_logger = logging.getLogger("astrodb_utils")
astrodb_utils_logger.setLevel(logging.INFO)

# Set up the logging for pan_starrs.
panlogger = logging.getLogger("astrodb_utils.pan_starrs")
panlogger.setLevel(logging.INFO) 

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
            panlogger.info(f"Photometry filter PS1.{band} added successfully.")
        except AstroDBError as e:
            if "already exists" in str(e):
                panlogger.warning(f"Photometry filter PS1.{band} already exists.")
            else:
                panlogger.error(f"Error adding photometry filter PS1.{band}: {e}")


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
            # add RA deg to excel to find source, add column to find_source_in_db
            # RAJ2000, DEJ2000
            # Create new csv that store the matching SIMPLE source name and Pan-STARRS source name
            db_name = find_source_in_db(
                db, 
                source,
                ra_col_name="ra",
                dec_col_name="dec",
            )
            
            if (len(db_name) != 1) or (len(db_name) == 0) or (len(db_name) >= 1):
                msg = f"No unique source match for {source} in the database"
                skipped += 1
                failed_source.append({"source":source, "reason": "No unique match"})
                panlogger.warning(msg)
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
            panlogger.info(f"collecting photometry for {source}")

        except Exception as e:
            msg = f"Error adding {source} photometry: {e}"
            if "None of [Index(['ra_deg', 'dec_deg']" in str(e):
                inaccessible += 1
                failed_source.append({"source": source, "reason": str(e)})
                panlogger.warning(msg)
            else:
                inaccessible += 1
                failed_source.append({"source": source, "reason": str(e)})
                panlogger.error(msg)

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

    # Store invalid ingestion sources into csv
    with open(csv_output2, "w", newline='') as f:
        writer = csv.DictWriter(f, fieldnames=["source", "reason"])
        writer.writeheader()
        writer.writerows(failed_source)

    panlogger.info(f"Total source added: {source_added}")
    panlogger.info(f"Total source skipped: {skipped}")
    panlogger.info(f"Total inaccessible data: {inaccessible}")
    for band, count in band_counts.items():
        panlogger.info(f"Total entries for PS1.{band} band: {count}")

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
                panlogger.info(f"Added photometry for {row['source']}")
            except sqlalchemy.exc.IntegrityError as e:
                panlogger.error(f"Error adding photometry for {row['source']}: {e}")


# Call ingestion function
                
#ingest_PanSTARRS_photometry_filters()

ingest_PanSTARRS_photometry(data,0,10000)

#ingest_from_csv(csv_path="scripts/ingests/Pan-STARRS/successful_sources_PS1.csv")



# save to database
if save_db:
    db.save_database(directory="data/")
    panlogger.info("Pan-STARRS Photometry Database saved as SIMPLE.sqlite")