import logging
from astrodb_utils import load_astrodb, AstroDBError
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
save_db = True

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

def ingest_photometry_from_csv(csvpath):
    with open(csvpath, 'r') as f:
        rows = csv.DictReader(f)
        for row in rows:
            try:
                photometry_data = [
                    {
                    "source": row["source"],
                    "band": row["band"],
                    "magnitude": row["magnitude"],
                    "magnitude_error": float(row["magnitude_error"]),
                    "telescope": "Pan-STARRS",
                    "reference": "Best18",
                    }
                ]
                with db.engine.begin() as conn:
                        conn.execute(db.Photometry.insert().values(photometry_data))
                panlogger.info(f"Photometry measurement added: {photometry_data}")
            except sqlalchemy.exc.IntegrityError as e:
                if "UNIQUE constraint failed:" in str(e):
                    msg = "The measurement may be a duplicate."
                    panlogger.warning(f"Error adding photometry for {row['source']}: {msg}")
                else:
                    panlogger.error(f"Error adding photometry for {row['source']}: {e}")

ingest_photometry_from_csv("scripts/ingests/Pan-STARRS/valid_photometry.csv")


# save to database
if save_db:
    db.save_database(directory="data/")
    panlogger.info("Pan-STARRS Photometry Database saved as SIMPLE.sqlite")