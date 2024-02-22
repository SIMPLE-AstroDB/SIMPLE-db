"""
Script for ingesting IRTF SpeX spectra processed by pyspextool into the SIMPLE database.

Expects spectra files to be uploaded to the S3 bucket in the following structure:
https://bdnyc.s3.amazonaws.com/SpeX/pyspextool/{filename}
"""

import logging
from pathlib import Path
from astropy.io import fits
from astrodb_scripts import load_astrodb, find_source_in_db, AstroDBError
from simple.schema import *
from simple.utils.spectra import ingest_spectrum, spectrum_plottable

SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = False  # recreates the .db file from the data files

logger = logging.getLogger("AstroDB")
logger.setLevel(logging.WARNING)

db = load_astrodb("SIMPLE.sqlite", recreatedb=RECREATE_DB)

data_directory = "/Users/kelle/Desktop/processed data set"
logger.info(f"Data directory: {data_directory}")

fits_files = Path(data_directory).glob("*.fits")

total_files = 0
ingested = []
skipped = []

for file in fits_files:
    total_files += 1
    msg = f"\n Processing {file}"
    logger.info(f"Processing {file}")
    hdr = fits.getheader(file)

    # Check if source is in the database
    matches = find_source_in_db(db, hdr["OBJECT"], ra=hdr["RA"], dec=hdr["DEC"])
    if len(matches) == 0:
        skipped.append(file.name)
        msg = f"Source {hdr['OBJECT']} not found in the database. Skipping."
        logger.warning(msg)
        continue  # exit loop and go to next file
    elif len(matches) > 1:
        skipped.append(file.name)
        msg = f"Multiple matches found for {hdr['OBJECT']}. Skipping."
        logger.warning(msg)
        continue  # exit loop and go to next file
    elif len(matches) == 1:
        source = matches[0]
        msg = f"Source {hdr['OBJECT']} found in the database as {source}."
        print(msg)
        logging.info(msg)

    # Source is in the database, get other needed keywords from the header
    regime = "nir"

    if hdr["TELESCOP"] == "NASA IRTF":
        telescope = "IRTF"
    else:
        skipped.append(file.name)
        msg = f"Telescope {hdr['TELESCOP']} not expected. Skipping. Expected NASA IRTF."
        logger.warning(msg)
        continue

    if hdr["INSTR"] == "SpeX":
        instrument = "SpeX"
    else:
        skipped.append(file.name)
        msg = f"Instrument {hdr['INSTRUME']} not expected. Skipping. Expected SpeX."
        logger.warning(msg)
        continue

    if hdr["MODE"] == "Prism":
        mode = "Prism"
    elif hdr["MODE"] == "SXD":
        mode = "SXD"
    else:
        skipped.append(file.name)
        msg = f"Mode {hdr['MODE']} not expected. Skipping. Expected Prism or SXD."
        logger.warning(msg)

    obs_date = hdr["AVE_DATE"]

    reference = "Missing"

    other_references = f"{hdr['PROG_ID']}: {hdr['OBSERVER']}"

    spectrum = f"https://bdnyc.s3.amazonaws.com/SpeX/pyspextool/{file.name}"

    # check if the spectrum is plottable
    try:
        plottable = spectrum_plottable(spectrum, raise_error=True)
    except AstroDBError as e:
        skipped.append(file.name)
        logger.warning(f"Spectrum not plottable. Skipping {file.name}")
        logger.debug(e)
        continue

    # Ingest the spectrum
    try:
        ingest_spectrum(
            db,
            source=source,
            spectrum=spectrum,
            regime=regime,
            obs_date=obs_date,
            telescope=telescope,
            instrument=instrument,
            mode=mode,
            reference=reference,
            other_references=other_references,
        )
        ingested.append(file.name)
    except AstroDBError as e:
        skipped.append(file.name)
        logger.warning(f"Error ingesting {file.name}. Skipping.")
        logger.debug(e)
        continue

if len(ingested) + len(skipped) != total_files:
    msg = (
        f"Some files were not ingested or skipped. \n"
        f"n_ingested = {len(ingested)}, n_skipped = {len(skipped)}, total_files = {total_files}"
    )
    logger.error(msg)
    raise AstroDBError

if len(skipped) == 0:
    logger.info(f"Ingested {len(ingested)} out of {total_files} files.")

if len(skipped) > 0:
    logger.warning(f"Skipped {len(skipped)} out of {total_files} files: \n {skipped}")
