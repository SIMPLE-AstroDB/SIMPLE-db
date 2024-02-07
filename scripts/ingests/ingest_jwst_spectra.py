from astrodb_scripts import load_astrodb, ingest_instrument, ingest_publication
from schema.schema import *
from scripts.utils.ingest_spectra_utils import ingest_spectrum

SAVE_DB = True  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files


db = load_astrodb("SIMPLE.sqlite", recreatedb=RECREATE_DB)

file = (
    "https://bdnyc.s3.amazonaws.com/JWST/NIRSpec/jw02124-o051_s00001_nirspec_f290lp-"
    "g395h-s200a1-subs200a1_x1d_manual.fits"
)

ingest_instrument(
    db,
    telescope="JWST",
    instrument="NIRSpec",
    mode="FS",
)

ingest_publication(
    db,
    publication="Fahe24",
    description="W1935-1546 aurora Nature paper",
    ignore_ads=True,
)

ingest_spectrum(
    db,
    source="CWISEP J193518.58-154620.3",
    spectrum=file,
    regime="mir",
    telescope="JWST",
    instrument="NIRSpec",
    mode="FS",
    obs_date="2022-10-17",
    reference="Fahe24",
    comments="F290LP-G395H",
)

# WRITE THE JSON FILES
if SAVE_DB:
    db.save_database(directory="data/")
