from simple.schema import *
from simple.schema import REFERENCE_TABLES
from astrodb_utils import (
    load_astrodb,
    AstroDBError,
    find_source_in_db,
    ingest_names,
    ingest_source,
    ingest_publication,
    find_publication
)


import sys
from astrodb_utils.utils import logger
sys.path.append(".")
from astropy.io import ascii

# logger = logging.getLogger(__name__)
names_ingested = 0
sources_ingested = 0
skipped = 0
total = 0
already_exists = 0
multiple_sources = 0
no_sources = 0
inside_if = 0


DB_SAVE = False
RECREATE_DB = True
db = load_astrodb("SIMPLE.sqlite", recreatedb=RECREATE_DB, reference_tables=REFERENCE_TABLES)

#separate for cases that don't work in our code/ads key stuff

ingest_publication(db, doi="10.1088/0004-637X/748/2/93")  # Roja12
ingest_publication(db, doi = "10.1088/0067-0049/203/2/21") # ULAS J074431.30+283915.6 

ingest_publication(db, bibcode="2018MNRAS.479.1383Z", reference="Zhan18.1352")
ingest_publication(db, bibcode="2018MNRAS.480.5447Z", reference="Zhan18.2054")

ingest_source(
    db,
    "LHS 292",
    search_db=False,
    reference="Roja12",
    #ra="ra",
    #dec="deg",
    #epoch="epoch",
)

ingest_source(
    db, 
    "ULAS J074431.30+283915.6",
    search_db= False,
    reference = "AhnC12"
)

link = (
    "scripts/ingests/bones_archive/theBonesArchivePhotometryMain.csv"
)

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

def extractADS(link):
    start = link.find("abs/") + 4
    end = link.find("/abstract")
    ads = link[start:end]
    ads = ads.replace("%26", "&")
    logger.debug(f"ads: {ads}")
    return ads


for source in bones_sheet_table:
    bones_name = source["NAME"].replace("\u2212", "-")
    match = None

    if len(bones_name) > 0 and bones_name != "null":
        match = find_source_in_db(
            db,
            bones_name,
            ra=source["RA"],
            dec=source["DEC"],
    
        )
        if len(match) == 1:
            try:
                ingest_names(
                    db, match[0], bones_name
                )  # ingest new names while were at it
                names_ingested += 1
            except AstroDBError as e:
                raise e  # only error is if there is a preexisting name anyways.

        if match is None:
            match = find_source_in_db(
                db,
                source["NAME"],
                ra=source["RA"],
                dec=source["DEC"],
            )

        if len(match) == 0:
            # ingest_publications for the ADS link
            ads = extractADS(source["ADS_Link"])
            adsMatch = None
            adsRef = source["Discovery Ref."]
            adsMatch = find_publication(
                db,
                bibcode = ads
            )
            logger.debug(f"adsMatch: {adsMatch}")

            if not adsMatch[0]:
                logger.debug(f"ingesting publication {ads}")
                ingest_publication(db, bibcode=ads)

            try:
                source_reference = find_publication(db, bibcode=ads)

                if source_reference[1] == 0:
                    skipped+=1
                    continue

                ingest_source(
                    db,
                    source=source["NAME"],
                    reference=source_reference[1],
                    ra=source["RA"],
                    dec=source["DEC"],
                    raise_error=True,
                    search_db=True,
                )
                sources_ingested +=1
            except AstroDBError as e:
                msg = "ingest failed with error " + str(e)
                logger.warning(msg)
                if "Already in database" in str(e):
                    already_exists += 1
                else: 
                    raise AstroDBError(msg) from e

        elif len(match) == 1:
            skipped+=1
            already_exists += 1

        else:
            skipped+=1
            a = AstroDBError
            logger.warning("ingest failed with error: " + str(a))
            raise AstroDBError(msg) from a

    else:
        skipped += 1


total = len(bones_sheet_table)
logger.info(f"skipped:{skipped}") # 92 skipped
logger.info(f"sources_ingested:{sources_ingested}") # 117 ingsted 
logger.info(f"total: {total}") # 209 total
logger.info(f"already_exists: {already_exists}") # 92 already exists

if DB_SAVE:
    db.save_database(directory="data/")
