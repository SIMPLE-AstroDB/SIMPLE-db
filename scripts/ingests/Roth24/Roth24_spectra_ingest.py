import logging
from astrodb_utils import load_astrodb
import sys
sys.path.append(".")
from simple.schema import REFERENCE_TABLES
from astropy.io import ascii
from simple.utils.spectra import ingest_spectrum
from astropy.io.fits import getheader
from astrodb_utils.utils import ingest_instrument

logger = logging.getLogger(
    "astrodb_utils.roth24"
)  # Sets up a child of the "astrodb_utils" logger
# logger.setLevel(logging.INFO)  # Set logger to INFO level - less verbose
logger.setLevel(logging.INFO)  # Set logger to debug level - more verbose


SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files
# LOAD THE DATABASE
db = load_astrodb(
    "SIMPLE.sqlite", recreatedb=RECREATE_DB, reference_tables=REFERENCE_TABLES
)

ingest_instrument(
    db, telescope="Keck II", instrument="NIRES", mode="spectroscopy"
)  # https://www2.keck.hawaii.edu/inst/nires/
ingest_instrument(
    db, telescope="Magellan I Baade", instrument="FIRE", mode="Prism"
)  # https://web.mit.edu/~rsimcoe/www/FIRE/
ingest_instrument(
    db, telescope="SOAR", instrument="TripleSpec4.1", mode="spectroscopy"
)  # 2020SPIE11447E..6LH
ingest_instrument(
    db, telescope="Lick Shane 3m", instrument="Kast", mode="spectroscopy"
)  # https://mthamilton.ucolick.org/techdocs/instruments/kast/

link = 'scripts/ingests/Roth24/Spectra.csv'
# Link to the CSV file used: https://docs.google.com/spreadsheets/d/1JFa8F4Ngzp3qAW8NOBurkz4bMKo9zXYeF6N1vMtqDZs/edit?usp=sharing
byw_table = ascii.read(
    link,  # change this to the path for the csv file
    format="csv",
    data_start=1,
    header_start=0,
    guess=False,
    fast_reader=False,
    delimiter=",",
)

for row in byw_table:
    header=getheader(row["Spectrum"])
    obs_date = header["DATE-OBS"]

    # fix typo in one date
    if obs_date == "2021-09-111":
        obs_date = "2021-09-11"

    ingest_spectrum(
        db,
        source=row["Source"],
        spectrum=row["Spectrum"],
        regime=row["Regime"],
        telescope=row["Telescope"],
        instrument=row["Instrument"],
        mode=row["Mode"],
        obs_date=obs_date,
        reference=row["ref"],
    )

    # WRITE THE JSON FILES
if SAVE_DB:
    db.save_database(directory="data/")
