import sys
import logging
sys.path.append(".")
from astropy.io import ascii
from simple import REFERENCE_TABLES
from astrodb_utils import load_astrodb
from astrodb_utils.sources import (
    logger,
    AstroDBError,
)
from simple.utils.spectral_types import (
    ingest_spectral_type
)
from scripts.ingests.calamari.calamari_publication_helpers import (
    otherReferencesList,
    ingest_resolved_children,
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

spectral_types_ingested = 0
sources_ingested = 0
spectral_types_already_exist = 0
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
#ingest Gl 337CD
ingest_resolved_children(
    db=db,
    source = "Gl 337CD",
    reference = "GaiaEDR3",
    ra = 138.0584919,
    dec=14.9956706,
    ra_col_name="ra",
    dec_col_name="dec",
    other_reference="Wils01"
)
sources_ingested+=1

ingest_resolved_children(
    db=db,
    source = "Gl 417BC",
    reference="GaiaEDR3",
    ra = 168.1055653,
    dec = 35.8028953,
    ra_col_name="ra",
    dec_col_name="dec",
    other_reference="Kirk00"
)
sources_ingested+=1

for row in calamari_table:
    object = row["Object"]
    object_spt = row["SpT_Secondary"]
    ignore_neighbors = False

    if object == "WISE J124332.17+600126.6":
        ingest_spectral_type(
            db=db,
            source = object,
            spectral_type_string=object_spt,
            reference = "Fahe21",
            regime = "unknown",
        )
        spectral_types_ingested+=1
        continue
    if object == "2MASS J00250365+4759191":
        ingest_spectral_type(
            db=db,
            source = object,
            spectral_type_string=object_spt,
            reference="Reid06.891",
            regime = "unknown"
        )
        spectral_types_ingested+=1
        continue
    
    reference = otherReferencesList(db = db, ref = row["Ref"], ref_table=ref_table)[0]

    if object in binaries:
        ignore_neighbors=True
    try:
        #ingest spectral types for the objects
        ingest_spectral_type(
            db=db,
            source = object,
            spectral_type_string=object_spt,
            regime = "unknown",
            reference = reference,
            ignore_neighbors=ignore_neighbors
        )
        spectral_types_ingested+=1
        logger.info(f"ingested spectral type for {object}")
    except AstroDBError as e:
        msg = "ingest failed with error: " + str(e)
        logger.warning(msg)
        if "Spectral type already in the database" in str(e):
            spectral_types_already_exist+=1
        else:
            raise e
        
logger.info(f"Spectral type already exists: {spectral_types_already_exist}") # 0 already exists in database
logger.info(f"Spectral types ingested: {spectral_types_ingested}") # 64 spectral types ingested
logger.info(f"sources ingested: {sources_ingested}") # 2 sources ingested
logger.info(f"total: {spectral_types_ingested+spectral_types_already_exist}")  # 64 total spectral types
if SAVE_DB:
    db.save_database(directory="data/")