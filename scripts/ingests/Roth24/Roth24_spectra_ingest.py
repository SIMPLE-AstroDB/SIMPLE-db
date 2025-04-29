from astrodb_utils import load_astrodb
import sys
sys.path.append(".")
from simple.schema import REFERENCE_TABLES
from astropy.io import ascii
from simple.utils.spectra import ingest_spectrum
from astropy.io.fits import getheader
from astrodb_utils.utils import ingest_instrument

SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files
# LOAD THE DATABASE
db = load_astrodb(
    "SIMPLE.sqlite", recreatedb=RECREATE_DB, reference_tables=REFERENCE_TABLES
)

ingest_instrument(db, telescope="Keck", instrument="NIRES", mode="Missing")
ingest_instrument(db, telescope="Magellan I Baade", instrument="FIRE", mode="Prism")
ingest_instrument(db, telescope="SOAR", instrument="TS4", mode="MISSING")

link = 'scripts/ingests/Roth24/Spectra.csv'
# Link to the CSV file used: https://docs.google.com/spreadsheets/d/1JFa8F4Ngzp3qAW8NOBurkz4bMKo9zXYeF6N1vMtqDZs/edit?usp=sharing
byw_table = ascii.read(
    link, #change this to the path for the csv file
    format="csv",
    data_start=1,
    header_start=0,
    guess=False,
    fast_reader=False,
    delimiter=",",
)

for row in byw_table:
    header=getheader(row["Spectrum"])
    ingest_spectrum(
        db,
        source=row["Source"],
        spectrum=row["Spectrum"],
        regime=row["Regime"],
        telescope=row["Telescope"],
        instrument=row["Instrument"],
        mode=row["Mode"],
        obs_date=header["DATE-OBS"],
        reference=row["ref"],
    )

    # WRITE THE JSON FILES
if SAVE_DB:
    db.save_database(directory="data/")
