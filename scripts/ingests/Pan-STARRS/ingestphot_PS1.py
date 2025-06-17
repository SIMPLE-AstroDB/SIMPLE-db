import logging
from astrodb_utils import load_astrodb, AstroDBError
from astrodb_utils.sources import find_source_in_db
from astrodb_utils.photometry import ingest_photometry, ingest_photometry_filter
from simple import REFERENCE_TABLES
import pandas as pd
from sqlalchemy.exc import IntegrityError

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

# Ingest Photometry Filters: PAN-STARRS/PS1.g, PAN-STARRS/PS1.r, PAN-STARRS/PS1.i, PAN-STARRS/PS1.z, PAN-STARRS/PS1.y
# Error: Unconsumed column names: effective_wavelength_angstroms, width_angstroms
# collide with columns in PhotometryFilters table [ "band", "ucd", "effective_wavelength", "width"]
def ingest_PanSTARRS_photometry_filters():
    ugriz = ['g', 'r', 'i', 'z', 'y']
    for band in ugriz:
        try:
            ingest_photometry_filter(
                db,
                telescope="Pan-STARRS",
                instrument="PS1",
                filter_name=f"{band}"
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
    photometry_added = 0
    skipped = 0
    inaccessible = 0
    band_counts = {band: 0 for band in ['g', 'r', 'i', 'z', 'y']}
    raise_error = False

    end_idx = min(start_idx + chunk_size, len(data))
    data_chunk = data.iloc[start_idx:end_idx]
    print(f"\nProcessing source {start_idx} to {end_idx - 1}\n")

    for _, row in data_chunk.iterrows():
        source = row["source"]
        try:
            db_name = find_source_in_db(db, source)
            
            if len(db_name) != 1:
                msg = f"No unique source match for {source} in the database"
                skipped += 1
                if raise_error:
                    logger.error(msg)
                    raise AstroDBError(msg)
                else:
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
                        "magnitude": str(row[mag_col]),
                        "magnitude_error": row[err_col],
                        "telescope": "Pan-STARRS",
                        "epoch": None,
                        "comments": None,
                        "reference": "Best18",
                    }
                    band_counts[band] += 1
                    #print(f"Inserted data: {photometry_data}")

            with db.engine.begin() as conn:
                conn.execute(db.Photometry.insert().values(photometry_data))
            photometry_added += 1
            logger.info(f"Added photometry for {source}")

        except (IntegrityError, AstroDBError, KeyError) as e:
            if "UNIQUE constraint failed:" in str(e):
                skipped += 1
                msg = f"Duplicate photometry for {source}."
                if raise_error:
                    logger.error(msg)
                    raise AstroDBError(msg)
                else:
                    logger.warning(msg)
            else:
                inaccessible += 1
                if raise_error:
                    logger.error(str(e))
                    raise AstroDBError( str(e))
                else:
                    logger.warning(str(e))

    logger.info(f"Total photometry added: {photometry_added}")
    logger.info(f"Total photometry skipped: {skipped}")
    logger.info(f"Total inaccessible data: {inaccessible}")
    for band, count in band_counts.items():
        logger.info(f"Total entries for PS1.{band} band: {count}")

# Call ingestion function
#ingest_PanSTARRS_photometry_filters()

# Runtime: ~32 seconds per 100 rows
ingest_PanSTARRS_photometry(data, start_idx=2079, chunk_size=10)

# Save updated SQLite database
if save_db:
    db.save_database(directory="data/")
    logger.info("Pan-STARRS Photometry Database saved as SIMPLE.sqlite")