import os
import logging
from astrodb_utils import load_astrodb, AstroDBError
from astrodb_utils.instruments import ingest_instrument
import simple
from simple import REFERENCE_TABLES
from simple.utils.spectra import ingest_spectrum

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))
DATA_DIR = os.path.join(BASE_DIR, "data")

# set up logging for ASTRODB
astrodb_utils_logger = logging.getLogger("astrodb_utils")
astrodb_utils_logger.setLevel(logging.INFO)

# set up logging for this ingest script
logger = logging.getLogger("astrodb_utils.WISE_1810")
logger.setLevel(logging.INFO)

# Load Database
recreate_db = True
save_db = False

SCHEMA_PATH = "SIMPLE-db/simple/schema.yaml"
db = load_astrodb(
    "SIMPLE.sqlite",
    recreatedb=recreate_db,
    data_path=DATA_DIR,
    reference_tables=REFERENCE_TABLES,
    felis_schema=SCHEMA_PATH
)

# Ingest Instruments ----
def add_instrument():
    """
    Telescope: GTC (existed)
    Instrument: EMIR (ingestion needed)
    """
    try:
        ingest_instrument(
            db,
            telescope="GTC", 
            instrument="EMIR",
            mode="Missing",
            raise_error=True
        )
        print ("Instrument added successfully")


    except AstroDBError as e:
        logger.error(f"Error adding instruments: {e}")


# same sources with different instruments and FITS file
spectra_data = [{
        #"access_url": spectrum,
        "regime": "optical",
        "instrument": 'OSIRIS',
        #"mode": "optical",
        "observation_date":"2020-09-06T00:00:00",
    },
    {
        #"access_url": spectrum,
        "regime": "NIR",
        "instrument": 'EMIR',
        #"mode": "NIR",
        "observation_date":"2021-06-01T00:00:00",
    }
]

# Ingest Spectra ----
def add_spectra():
    for data in spectra_data:
        try:
            ingest_spectrum(
                db,
                source="CWISEP J181006.00-101001.1",
                #access_url=
                regime=data["regime"],
                mode=data.get("mode"),
                telescope="GTC",
                instrument=data["instrument"],
                observation_date=data["observation_date"],
                reference="Lodi22"
            )
            logger.info(f"Successfully ingested spectrum for {data['source']} with {data['instrument']} ")
        except AstroDBError as e:
            logger.error(f"Error ingesting spectrum from {data['source']}: {e}")




add_instrument()
# add_spectra()

if save_db:
    db.save()
    logger.info("Database saved successfully.")

                
    




