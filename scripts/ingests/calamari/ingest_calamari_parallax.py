import sys
import logging
sys.path.append(".")
from astropy.io import ascii
from simple import REFERENCE_TABLES
from astrodb_utils import load_astrodb
from astrodb_utils.sources import (
    logger,
    AstroDBError,
    find_source_in_db
)
from simple.utils.astrometry import (
    ingest_parallax
)

from scripts.ingests.calamari.calamari_publication_helpers import (
    otherReferencesList
)

astrodb_utils_logger = logging.getLogger("astrodb_utils")
logger.setLevel(logging.DEBUG)  # Set logger to INFO/DEBUG/WARNING/ERROR/CRITICAL level
astrodb_utils_logger.setLevel(logging.DEBUG)

SAVE_DB = True  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files
SCHEMA_PATH = "simple/schema.yaml"
db = load_astrodb(
    "SIMPLE.sqlite",
    recreatedb=RECREATE_DB,
    reference_tables=REFERENCE_TABLES,
    felis_schema=SCHEMA_PATH,
)

link = (
    "scripts/ingests/calamari/calamari_data.csv"
)
link_2 = (
    "scripts/ingests/calamari/calamari_refs.csv"
)

calamari_table = ascii.read(
    link,
    format="csv",
    data_start=1,
    header_start=0,
    guess=False,
    fast_reader=False, 
    delimiter=",",
)

ref_table = ascii.read(
    link_2,
    format="csv",
    data_start=0,
    header_start=0,
    guess=False,
    fast_reader=False, 
    delimiter=",",
)

parallax_ingested = 0
parallax_already_exist = 0
skipped= 0

binaries = [
    "HD 130948BC",
    "HD 130948C",
    "HD 130948B",
    "Gl 337CD",
    "Gl 337C",
    "Gl 337D",
    "Gl 417BC",
    "Gl 417B",
    "Gl 417C"
]

for row in calamari_table:
    ignore_neighbors=False
    object = row["Object"]

    #skip these two sources because they have no parallax value or have been ingested beforehand
    if object == "HIP 21152 B" or object == "HD 72946 B":
        skipped+=1
        continue

    if object == "WISE J124332.17+600126.6":
        #ingest parallax for WISE J124332.17+600126.6
        # ingest_parallax(
        #     db=db,
        #     source = "WISE J124332.17+600126.6",
        #     parallax_mas=22.24367115,
        #     parallax_err_mas=0.013506583,
        #     reference = "Fahe21"
        # )
        # parallax_ingested+=1
        continue
    
    if object == "2MASS J00250365+4759191":
        #ingest parallax for 2MASS J00250365+4759191
        # ingest_parallax(
        #     db=db, 
        #     source = "2MASS J00250365+4759191",
        #     parallax_mas=18.5162,
        #     parallax_err_mas=0.1365,
        #     reference="Reid06.891"
        # )
        # parallax_ingested+=1
        continue

    reference = otherReferencesList(db=db, ref = row["Ref"], ref_table=ref_table)[0]
    parallax_mas=row["Parallax (mas)"]

    if object in binaries:
        ignore_neighbors = True

    if not ignore_neighbors:
        object = find_source_in_db(db=db, source = object)[0]
    try:
        ingest_parallax(
            db=db,
            source = object,
            parallax_mas=parallax_mas,
            parallax_err_mas=row["ePlx"],
            reference = reference
        )
        parallax_ingested+=1
    except AstroDBError as e:
        msg = "ingest failed with error: " + str(e)
        logger.warning(msg)
        if "Duplicate measurement exists with same reference" in str(e):
            parallax_already_exist+=1
        else:
            raise e

        
logger.info(f"parallax already exists: {parallax_already_exist}") # 10 already exists in database
logger.info(f"parallax ingested: {parallax_ingested}") #52 parallax ingested
logger.info(f"parallax skipped: {skipped}") # 2 parallax skipped
logger.info(f"total: {parallax_already_exist+parallax_ingested+skipped}")  # 64 total
if SAVE_DB:
    db.save_database(directory="data/")