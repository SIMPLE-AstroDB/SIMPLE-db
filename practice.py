import logging
from pathlib import Path
from astropy.io import fits
from astrodb_scripts import load_astrodb, find_source_in_db
from simple.schema import *
from simple.utils.spectra import ingest_spectrum

SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = False  # recreates the .db file from the data files

logger = logging.getLogger("AstroDB")
logger.setLevel(logging.INFO)

db = load_astrodb("SIMPLE.sqlite", recreatedb=RECREATE_DB)

data_directory = "/Users/kelle/Desktop/processed data set"
logging.info(f"Data directory: {data_directory}")

fits_files = Path(data_directory).glob("*.fits")

skipped = []

for file in fits_files:
    msg = f"\n Processing {file}"
    print(msg)
    # logging.info(f"Processing {file}")
    hdr = fits.getheader(file)

    # Check if source is in the database
    matches = find_source_in_db(db, hdr["OBJECT"], ra=hdr["RA"], dec=hdr["DEC"])
    if len(matches) == 0:
        skipped.append(file.name)
        msg = f"Source {hdr['OBJECT']} not found in the database. Skipping."
        print(msg)
        # logging.info(f"Source {hdr['OBJECT']} not found in the database. Skipping.")
        continue
    elif len(matches) > 1:
        skipped.append(file)
        msg = f"Multiple matches found for {hdr['OBJECT']}. Skipping."
        print(msg)
        continue
    elif len(matches) == 1:
        source = matches[0]
        msg = f"Source {hdr['OBJECT']} found in the database as {source}."
        print(msg)
        # logging.info(f"Source {hdr['OBJECT']} found in the database as {source}.")

print(f"Skipped {len(skipped)} files: \n {skipped}")
