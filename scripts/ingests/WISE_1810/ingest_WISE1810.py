import logging
from astrodb_utils import load_astrodb, AstroDBError
from astrodb_utils.instruments import ingest_instrument
from simple import REFERENCE_TABLES
from simple.utils.spectra import ingest_spectrum
from datetime import datetime

# set up logging for ASTRODB
astrodb_utils_logger = logging.getLogger("astrodb_utils")
astrodb_utils_logger.setLevel(logging.INFO)

# set up logging for this ingest script
logger = logging.getLogger("astrodb_utils.WISE_1810")
logger.setLevel(logging.INFO)

# Load Database
recreate_db = True
save_db = True

SCHEMA_PATH = "simple/schema.yaml"
db = load_astrodb(
    "SIMPLE.sqlite",
    recreatedb=recreate_db,
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

# convert obs date format
format_str = "%Y-%m-%d %H:%M:%S0000"

# same sources with different instruments and FITS file
spectra_data = [{
        "access_url": "https://bdnyc.s3.us-east-1.amazonaws.com/WISE1810m10_OB0001_R1000R_06Sept2020.fits",
        "regime": "optical",
        "instrument": 'OSIRIS',
        "observation_date": datetime.strptime("2020-09-06 00:00:000000", format_str)
    },
    {
        "access_url": "https://bdnyc.s3.us-east-1.amazonaws.com/WISE1810_comb_Jun2021_YJ_STD_bb.fits",
        "regime": "nir",
        "instrument": 'EMIR',
        "observation_date":datetime.strptime("2021-06-01 00:00:000000", format_str)
    }
]

# Ingest Spectra ----
def add_spectra():
    for data in spectra_data:
        try:
            ingest_spectrum(
                db,
                source="CWISEP J181006.00-101001.1",
                spectrum=data["access_url"],
                regime=data["regime"],
                mode="Missing",
                telescope="GTC",
                instrument=data["instrument"],
                obs_date=data["observation_date"],
                reference="Lodi22"
            )
            logger.info(f"Successfully ingested spectrum for CWISEP J181006.00-101001.1 with {data['instrument']}")
        except AstroDBError as e:
            logger.error(f"Error ingesting spectrum: {e}")

# Run ingestion function
add_instrument()
add_spectra()

if save_db: 
    db.save_database(directory="data/")
