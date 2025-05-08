from astrodb_utils import load_astrodb
from astrodb_utils.sources import (
    find_source_in_db,
    AstroDBError,
    ingest_source,
    find_publication,
    ingest_name,
)

from astrodb_utils.publications import (
    ingest_publication,
    logger,
)

import sys

sys.path.append(".")
from astropy.io import ascii
from simple.schema import REFERENCE_TABLES


DB_SAVE = True
RECREATE_DB = True
db = load_astrodb(
    "SIMPLE.sqlite", recreatedb=RECREATE_DB, reference_tables=REFERENCE_TABLES
)


def add_publications(db):
    """
    Add publications to the database.
    """
    # add publication to the database

    # ingest doi publications because it isn't compatible with extractADS function
    # comment out when DB_SAVE = True because publications and sources already exist in db
    ingest_publication(
        db, doi="10.1088/0004-637X/748/2/93", reference="Roja12"
    )  # Roja12
    # ingest_publication(db, doi = "10.1088/0067-0049/203/2/21", reference = "Ahn_12") #ahn_12
    ingest_publication(db, bibcode="2018MNRAS.479.1383Z", reference="Zhan18.1352")
    ingest_publication(db, bibcode="2018MNRAS.480.5447Z", reference="Zhan18.2054")


ingest_source(
    db,
    "ULAS J074431.30+283915.6",
    search_db=True,
    reference="Ahn_12",
    ra_col_name="ra",
    dec_col_name="dec",
    epoch_col_name="epoch",
)

ingest_source(
    db,
    "LHS 292",
    search_db=False,
    reference="Roja12",
    ra_col_name="ra",
    dec_col_name="dec",
    epoch_col_name="epoch",
)

# Load photometry sheet
link = "scripts/ingests/bones_archive/fixed_bones_archive_photometry.csv"

# read the csv data into an astropy table
bones_sheet_table = ascii.read(
    link,
    format="csv",
    data_start=1,
    header_start=0,
    guess=False,
    fast_reader=False,
    delimiter=",",
)

ingested = 2  # 2 sources ingested above
already_exists = 0
skipped = 0
name_added = 0


# helper method for extracting ads key from link
def extractADS(link):
    start = link.find("abs/") + 4
    end = link.find("/abstract")
    ads = link[start:end]
    ads = ads.replace("%26", "&")
    return ads


for source in bones_sheet_table:
    bones_name = source["NAME"].replace("\u2212", "-")
    bones_name = bones_name.replace("\u2212", "-")
    bones_name = bones_name.replace("\u2013", "-")
    bones_name = bones_name.replace("\u2014", "-")
    match = None

    ##tries to find match of name in database
    if match == None:
        match = find_source_in_db(
            db,
            bones_name,
            ra=source["RA"],
            dec=source["DEC"],
            ra_col_name="ra",
            dec_col_name="dec",
        )
        print(match)

    if len(match) == 0:
        # ingest_publications for ads
        ads = extractADS(source["ADS_LINK"])
        ads_match = None
        ref = source["Discovery Ref."]
        ads_match = find_publication(db=db, bibcode=ads)

        # if ads not in db, ingest
        try:
            if ads_match[0] == False:
                ingest_publication(db=db, bibcode=ads)
        except:
            logger.warning("Find and ingest pattern didn't work" + ads)

        try:

            reference = find_publication(db=db, bibcode=ads)
            ra = source["RA"]
            dec = source["DEC"]

            # if publication not found in database, skip
            if reference[1] == 0:
                skipped += 1
                logger.warning("Reference does not exist.")
                continue

            ingest_source(
                db,
                source=str(bones_name),
                reference=reference[1],
                ra=ra,
                dec=dec,
                ra_col_name="ra",
                dec_col_name="dec",
                epoch_col_name="epoch",
                raise_error=True,
                search_db=True,
                comment="Discovery reference from the BONES archive",
            )  # ingest new sources
            ingested += 1
        except AstroDBError as e:
            # None only error is if there is a preexisting source anyways.
            raise AstroDBError from e

    elif len(match) == 1:
        logger.warning("Source already exists in the database.")
        already_exists += 1
        try:
            ingest_name(
                db, source=match[0], other_name=bones_name, raise_error=True
            )  # ingest new names while were at it
            name_added += 1
        except AstroDBError as e:
            logger.warning("ingest alt name failed with error: " + str(e))
    else:
        skipped += 1
        a = AstroDBError
        logger.warning("ingest failed with error: " + str(a))
        raise AstroDBError from a

total = len(bones_sheet_table)
logger.info(f"ingested:{ingested}")  # 118 ingested
logger.info(f"already exists:{already_exists-2}")  # 93-2=91 due to preexisting data
logger.info(f"name added:{name_added}")  # 21 alt names added
logger.info(f"skipped:{skipped}")  # 0 skipped
logger.info(f"total:{total}")  # 209 total
if DB_SAVE:
    db.save_database(directory="data/")
