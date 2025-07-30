import logging
from astrodb_utils import load_astrodb, AstroDBError
from astrodb_utils.sources import find_source_in_db
from astrodb_utils.photometry import ingest_photometry, ingest_photometry_filter
from simple import REFERENCE_TABLES
import pandas as pd
import csv

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

"""
This script is to collect the valid and invalid photometry data for further ingest in ingestphot_PS1.py
For the matched source name, ingest_name is required in ingest_name.py
"""

excel_path = "scripts/ingests/Pan-STARRS/Pan-STARRS Photometry.xlsx"
csv_output1 = "scripts/ingests/Pan-STARRS/valid_photometry.csv"
csv_output2 = "scripts/ingests/Pan-STARRS/invalid_photometry.csv"
matched_csv = "scripts/ingests/Pan-STARRS/matched_sources.csv"
data = pd.read_excel(excel_path)


# ingest Photometry Filters: PAN-STARRS/PS1.g, PAN-STARRS/PS1.r, PAN-STARRS/PS1.i, PAN-STARRS/PS1.z, PAN-STARRS/PS1.y
def ingest_PanSTARRS_photometry_filters():
    ugriz = ['g', 'r', 'i', 'z', 'y']
    for band in ugriz:
        try:
            ingest_photometry_filter(
                db,
                telescope="Pan-STARRS",
                instrument="PS1",
                filter_name=f"{band}",
                wavelength_col_name="effective_wavelength",
                width_col_name="width"
            )
            panlogger.info(f"Photometry filter PS1.{band} added successfully.")
        except AstroDBError as e:
            if "already exists" in str(e):
                panlogger.warning(f"Photometry filter PS1.{band} already exists.")
            else:
                panlogger.error(f"Error adding photometry filter PS1.{band}: {e}")


# Ingest Photometry: Add different bands depends on the ugriz magnitude available in the datasets
# process data in chunks as dataset is large
def ingest_PanSTARRS_photometry(data):
    source_added = 0
    skipped = 0
    inaccessible = 0
    raise_error = False
    band_counts = {band: 0 for band in ['g', 'r', 'i', 'z', 'y']}

    # collect valid & invalid source row by row
    with open(csv_output1, "w", newline='') as valid_f, \
        open(csv_output2, "w", newline='') as invalid_f, \
        open(matched_csv, "w", newline='') as matched_f:

        
        valid_writer = csv.DictWriter(valid_f, fieldnames=["source", "band", "magnitude", "magnitude_error"])
        valid_writer.writeheader()

        invalid_writer = csv.DictWriter(invalid_f, fieldnames=["source", "reason"])
        invalid_writer.writeheader()

        matched_writer = csv.DictWriter(matched_f, fieldnames=["original_source", "matched_source"])
        matched_writer.writeheader()

        for row in data.itertuples():
            print(f"Processing source: {row}")
            source = row.source
        
            try:
                db_name = find_source_in_db(
                    db, 
                    source,
                    ra_col_name="ra", # for SIMPLE db to look for matching col name
                    dec_col_name="dec",
                    ra=row.ra, # for Simbad search
                    dec=row.dec,
                    use_simbad=True
                )
                
                if (len(db_name) == 0):
                    skipped += 1
                    invalid_writer.writerow({"source": source, "reason": "No unique match in SIMPLE db"})
                    panlogger.warning(f"Skipping {source}: No unique match in SIMPLE db.")
                    continue
                
                # Tested: no multiple matches occured in this dataset
                elif (len(db_name) > 1 ):
                    panlogger.warning(f"Skipping {source}, Multiple matches found in SIMPLE db: {e}")
                    invalid_writer.writerow({"source": source, "reason": "Multiple matches found in SIMPLE db"})
                    continue
                
                else:
                    db_name = db_name[0]

                # add matched source name in SIMPLE db corresponding to the Pan-STARRS source name
                matched_writer.writerow({
                    "original_source": source,
                    "matched_source": db_name 
                })
                
                # add band for every ugriz mag
                for band,mag, err in (['g',row.gmag,row.e_gmag],['r',row.rmag,row.e_rmag],['i',row.imag,row.e_imag],['z',row.zmag,row.e_zmag],['y',row.ymag,row.e_ymag]):
                    if not pd.isna(mag):
                        valid_writer.writerow({
                            "source": db_name,
                            "band": f"PAN-STARRS/PS1.{band}",
                            "magnitude": mag,
                            "magnitude_error": err,
                        })
                        band_counts[band] += 1

                source_added += 1
                panlogger.info(f"Collected photometry for {source}\n")


            except Exception as e:
                msg = f"Error adding {source} photometry: {e}"
                if "None of [Index(['ra_deg', 'dec_deg']" in str(e):
                    inaccessible += 1
                    invalid_writer.writerow({"source": source, "reason": str(e)})
                    panlogger.warning(msg)
                else:
                    inaccessible += 1
                    invalid_writer.writerow({"source": source, "reason": str(e)})
                    panlogger.error(msg)


    panlogger.info(f"Total source added: {source_added}")
    panlogger.info(f"Total source skipped: {skipped}")
    panlogger.info(f"Total inaccessible data: {inaccessible}")
    for band, count in band_counts.items():
        panlogger.info(f"Total entries for PS1.{band} band: {count}")

    """
    log output:
    INFO     - astrodb_utils.pan_starrs - Total source matched: 2084
    INFO     - astrodb_utils.pan_starrs - Total source skipped: 7804
    INFO     - astrodb_utils.pan_starrs - Total inaccessible data: 0
    INFO     - astrodb_utils.pan_starrs - Total entries for PS1.g band: 362
    INFO     - astrodb_utils.pan_starrs - Total entries for PS1.r band: 893
    INFO     - astrodb_utils.pan_starrs - Total entries for PS1.i band: 1557
    INFO     - astrodb_utils.pan_starrs - Total entries for PS1.z band: 1939
    INFO     - astrodb_utils.pan_starrs - Total entries for PS1.y band: 2051
    """
    
# Call ingestion function
                
#ingest_PanSTARRS_photometry_filters()

ingest_PanSTARRS_photometry(data)


# save to database
if save_db:
    db.save_database(directory="data/")
    panlogger.info("Pan-STARRS Photometry Database saved as SIMPLE.sqlite")