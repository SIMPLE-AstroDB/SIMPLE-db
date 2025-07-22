from astrodb_utils import load_astrodb
import sys

sys.path.append(".")
from simple import *
from simple import REFERENCE_TABLES
from simple.utils.astrometry import ingest_proper_motions
from astropy.io import ascii
from astrodb_utils.publications import (
    logger,
    find_publication,
    ingest_publication
)
from astrodb_utils.sources import (
    find_source_in_db,
    ingest_source,
    AstroDBError,
    ingest_name
)

from astrodb_utils.photometry import ingest_photometry
from astropy.table import Table
from astroquery.ipac.irsa import Irsa #we will use astroquery 
from astropy.coordinates import SkyCoord
import astropy.units as u


SAVE_DB = True  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files
SCHEMA_PATH = "simple/schema.yaml" 
# LOAD THE DATABASE
db = load_astrodb("SIMPLE.sqlite", recreatedb=RECREATE_DB, reference_tables=REFERENCE_TABLES, felis_schema=SCHEMA_PATH)

link = 'scripts/ingests/ultracool_sheet/UltracoolSheet - Main_070325.csv'

uc_sheet_table = ascii.read(
    link,
    format="csv",
    data_start=1,
    header_start=0,
    guess=False,
    fast_reader=False,
    delimiter=",",
)

#ingest maro21
ingest_publication(
    db = db,
    bibcode = "2021ApJS..253....8M"
)

#ingest eise20
ingest_publication(
    db = db, 
    bibcode = "2020ApJS..247...69E"
)

#ingest schl19
ingest_publication(
    db=db,
    bibcode = "2019ApJS..240...30S"
)

one_match_counter, no_match_counter, multiple_matches_counter, upper_error_counter, duplicate_counter = 0, 0, 0, 0, 0
photo1_counter, photo2_counter, photo3_counter, photo4_counter = 0, 0, 0, 0
no_match, multiple_matches, skipped, reason, bad_flag = [], [], [], [], []
pm_counter, photo_counter, name_counter = 0, 0, 0
name_ingested = False
good_flag_counter, bad_flag_counter = 0, 0



for row in uc_sheet_table:
    good_flag_counter = 0
    name_ingested = False
    match = find_source_in_db(
        db,
        source = row["name"],
        ra = row["ra"],
        dec = row["dec"],
        ra_col_name="ra",
        dec_col_name="dec",
        use_simbad = True
    )
    if len(match) == 1:
        print("match:")
        print(match)
        print("has one match")
        if(str(row["designation_WISE"])!= "null"):
            new_name = ingest_name(
                db,
                source = match[0],
                other_name = row["designation_WISE"]
            )
            if(new_name != None):
                name_counter += 1
                name_ingested = True
        else:
            skipped.append(row["name"])
            reason.append("missing wise designation name")

        if(str(row["pmra_catwise"])!= "nan"):
            ingest_proper_motions(
                db,
                sources = match,
                pm_ras = row["pmra_catwise"],
                pm_ra_errs = row["pmraerr_catwise"],
                pm_decs = row["pmdec_catwise"],
                pm_dec_errs = row["pmdecerr_catwise"],
                pm_references = "Maro21",
            )
            pm_counter += 1
        else:
            skipped.append(row["name"])
            reason.append("missing proper motion values")

        flag_counter = 0
        for flag in row["flag_WISE"]:
            flag_counter+=1
            photometry_band = "W" + str(flag_counter)
            if(str(row[photometry_band + "err"]) == "nan"):
                upper_error_counter += 1
                skipped.append(row["name"] + " " + photometry_band)
                reason.append("only upper error")
                continue
            if flag == "0":
                good_flag_counter += 1
                try:
                    ingest_photometry(
                        db, 
                        source = match[0],
                        band = "WISE."+photometry_band,
                        magnitude = row[photometry_band],
                        magnitude_error = row[photometry_band + "err"],
                        telescope = "WISE",
                        reference = row["ref_" + photometry_band]
                    )
                    photo_counter += 1
                    if flag_counter == 1:
                        photo1_counter += 1
                    if flag_counter == 2:
                        photo2_counter += 1
                    if flag_counter == 3:
                        photo3_counter += 1
                    if flag_counter == 4:
                        photo4_counter += 1
                except AstroDBError as e:
                    if "duplicate" in str(e):
                        duplicate_counter += 1
                        skipped.append(row["name"])
                        reason.append("duplicate, already ingested")
                        continue
                    elif "Eise20;Schn20" in str(e):
                        ingest_photometry(
                            db, 
                            source = match[0],
                            band = "WISE."+photometry_band,
                            magnitude = row[photometry_band],
                            magnitude_error = row[photometry_band + "err"],
                            telescope = "WISE",
                            reference = "Eise20",
                            comments = "Other reference is Schn20"
                        )
                        photo_counter += 1
                        if flag_counter == 1:
                            photo1_counter += 1
                        if flag_counter == 2:
                            photo2_counter += 1
                        if flag_counter == 3:
                            photo3_counter += 1
                        if flag_counter == 4:
                            photo4_counter += 1
                    else:
                        raise e
                
        one_match_counter += 1
        if (good_flag_counter == 0):
            bad_flag.append(row["name"])
            bad_flag_counter += 1
    elif len(match) > 1:
        print("has multiple match")
        multiple_matches.append(row["name"])
        msg = f"Multiple matches found for {row['name']}"
        logger.error(msg)
        multiple_matches_counter += 1
    else:
        no_match.append(row["name"])
        msg = f"No matches found for {row['name']}"
        logger.error(msg)
        no_match_counter += 1
        
   
no_match_table = Table([no_match], names=["No Match"])
no_match_table.write(
    "scripts/ingests/ultracool_sheet/uc_sheet_catwise_no_match.csv",
    delimiter=",",
    overwrite=True,
    format="ascii.ecsv",
)

skipped_table = Table([skipped, reason], names=["Skipped", "Reason"])
skipped_table.write(
    "scripts/ingests/ultracool_sheet/uc_sheet_catwise_skipped.csv",
    delimiter=",",
    overwrite=True,
    format="ascii.ecsv",
)

multiple_matches_table = Table([multiple_matches], names=["Multiple Matches"])
multiple_matches_table.write(
    "scripts/ingests/ultracool_sheet/uc_sheet_catwise_multiple_matches.csv",
    delimiter=",",
    overwrite=True,
    format="ascii.ecsv",
)

bad_flag_table = Table([bad_flag], names=["Bad Flags"])
bad_flag_table.write(
    "scripts/ingests/ultracool_sheet/uc_sheet_catwise_bad_flag.csv",
    delimiter=",",
    overwrite=True,
    format="ascii.ecsv",
)

print(str(one_match_counter) + " photometry ingested") #3142
print(str(no_match_counter) + " no matches") #746
print(str(multiple_matches_counter) + " multiple matches") #1
print(str(duplicate_counter) + " duplicate sources") #22
print(str(upper_error_counter) + " upper error sources") #4588
print(str(photo_counter) + " photometry ingested") #7287 (including all bands)
print(str(pm_counter) + " propermotions ingested") # 2953
print(str(name_counter) + " catwise designation names ingested") # 2971
print(str(photo1_counter) + " photometry band 1 ingested") # 2896
print(str(photo2_counter) + " photometry band 2 ingested") # 2917
print(str(photo3_counter) + " photometry band 3 ingested") # 1310
print(str(photo4_counter) + " photometry band 4 ingested") # 164
print(str(bad_flag_counter) + " bad flags") # 22 with name ingested


logger.info("done")

if SAVE_DB:
    db.save_database(directory="data/")


