from astrodb_scripts import load_astrodb, ingest_instrument, ingest_publication
from schema.schema import *
from scripts.utils.ingest_spectra_utils import ingest_spectrum
from scripts.utils.photometry import ingest_photometry_filter, ingest_photometry
import logging

SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files

logger = logging.getLogger("AstroDB")
logger.setLevel(logging.INFO)

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
    description="Methane Emission From a Cool Brown Dwarf",
    ignore_ads=True,
)

ingest_spectrum(
    db,
    source="CWISEP J193518.58-154620.3",
    spectrum=file,
    regime="nir",
    telescope="JWST",
    instrument="NIRSpec",
    mode="FS",
    obs_date="2022-10-17",
    reference="Fahe24",
    comments="F290LP-G395H",
)

filter_names = ["F1000W", "F1280W", "F1800W"]
for filter_name in filter_names:
    ingest_photometry_filter(
        db,
        telescope="JWST",
        instrument="MIRI",
        filter_name=filter_name,
    )

# MIRI.F1000W - (13.740374118847015, 0.00509224851727859) &
# MIRI.F1280W - (13.125575556995827, 0.00711303904668107) &
# MIRI.F1800W - (12.108791667404923, 0.01817644357837899)

ingest_photometry(
    db,
    source="CWISEP J193518.58-154620.3",
    band="JWST/MIRI.F1000W",
    telescope="JWST",
    magnitude=13.7404,
    magnitude_error=0.0051,
    reference="Fahe24",
)

ingest_photometry(
    db,
    source="CWISEP J193518.58-154620.3",
    band="JWST/MIRI.F1280W",
    telescope="JWST",
    magnitude=13.1256,
    magnitude_error=0.0071,
    reference="Fahe24",
)

ingest_photometry(
    db,
    source="CWISEP J193518.58-154620.3",
    band="JWST/MIRI.F1800W",
    telescope="JWST",
    magnitude=12.109,
    magnitude_error=0.018,
    reference="Fahe24",
)

# WRITE THE JSON FILES
if SAVE_DB:
    db.save_database(directory="data/")
