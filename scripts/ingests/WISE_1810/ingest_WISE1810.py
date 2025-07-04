import logging
from astrodb_utils import load_astrodb, AstroDBError
from astrodb_utils.instruments import ingest_instrument
from simple import REFERENCE_TABLES
from simple.utils.spectra import ingest_spectrum



# set up logging for ASTRODB
astrodb_utils_logger = logging.getLogger("astrodb_utils")
astrodb_utils_logger.setLevel(logging.INFO)

# set up logging for this ingest script
logger = logging.getLogger("astrodb_utils.WISE_1810")
logger.setLevel(logging.INFO)

# Load Database
recreate_db = False
save_db = False

SCHEMA_PATH = "simple/schema.yaml"
db = load_astrodb(
    "SIMPLE.sqlite",
    recreatedb=recreate_db,
    reference_tables=REFERENCE_TABLES,
    felis_schema=SCHEMA_PATH
)


def add_instrument():
    try:
        ingest_instrument(
            db,
            telescope="GTC",
            instrument="EMIR",
            #mode="imaging",
            raise_error=True
        )
        print ("Instrument added successfully")
        # print out the instrument calue
        instrument = db.query(db.Instruments).filter(
            db.Instruments.c.telescope == "GTC",
            db.Instruments.c.instrument == "EMIR"
        ).table()
        print(instrument)
    except AstroDBError as e:
        logger.error(f"Error adding instruments: {e}")

files=[
    "scripts/ingests/WISE_1810/data_target_WISE1810_comb_Jun2021_YJ_STD_bb.fits",
    "scripts/ingests/WISE_1810/WISE1810m10_OB0001_R1000R_06Sept2020.fits"
]

spectra_data = [{
        "source": "CWISEP J181006.00-101001.1",
        #"access_url": spectrum,
        "regime": "optical",
        "telescope": "GTC",
        "instrument": 'OSIRIS',
        #"mode": "optical",
        "observation_date":"2020-09-06T00:00:00",
        "reference": "Lodi22"
    },
    {
        "source": "CWISEP J181006.00-101001.1",
        #"access_url": spectrum,
        "regime": "NIR",
        "telescope": "GTC",
        "instrument": 'EMIR',
        #"mode": "NIR",
        "observation_date":"2021-06-01T00:00:00",
        "reference": "Lodi22"
    }
]

# Ingest Spectra ----
#def add_spectra()



add_instrument()

if save_db:
    db.save()
    logger.info("Database saved successfully.")

                
    




