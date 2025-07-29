import sys
import logging
sys.path.append(".")
from astropy.io import ascii
from simple import REFERENCE_TABLES
from astrodb_utils import load_astrodb
from astrodb_utils.sources import (
    logger,
    ingest_source
)

from astrodb_utils.publications import (
    find_publication,
)

from simple.utils.companions import (
    ingest_companion_relationships,
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
companions_ingested = 0
companions_already_exists = -9 #we ingest 9 parent-child relationships at the start:
"""
        ["HD 130948BC","HD 130948","Child"],
        ["HD 130948B","HD 130948","Child"],
        ["HD 130948C","HD 130948","Child"],
        ["Gl 337CD", "Gl 337","Child"],
        ["Gl 337C", "Gl 337","Child"],
        ["Gl 337D", "Gl 337","Child"],
        ["Gl 417BC", "Gl 417","Child"],
        ["Gl 417B", "Gl 417","Child"],
        ["Gl 417C", "Gl 417","Child"],
"""
sources_ingested=0

#helper method to check if a companion relationship exists
#returns a boolean
def companionExists(source, companion):
    exists = False
    relationship_search = db.search_object(
        name = source,
        output_table="CompanionRelationships"
    )
    if len(relationship_search) > 0:
        for relationship in relationship_search:
            if relationship["companion_name"] == companion:
                exists = True
                break
    
    return exists

result = [["HD 130948B", "HD 130948C", "Sibling"], 
        ["HD 130948C", "HD 130948B","Sibling"], 
        ["HD 130948BC", "HD 130948B","Unresolved Parent"],
        ["HD 130948BC", "HD 130948C","Unresolved Parent"],
        ["HD 130948B", "HD 130948BC","Resolved Child"],
        ["HD 130948C", "HD 130948BC","Resolved Child"],
        ["HD 130948BC","HD 130948","Child"],
        ["HD 130948B","HD 130948","Child"],
        ["HD 130948C","HD 130948","Child"],
        ["Gl 337C", "Gl 337D","Sibling"],
        ["Gl 337D", "Gl 337C","Sibling"], 
        ["Gl 337CD", "Gl 337C","Unresolved Parent"], 
        ["Gl 337CD", "Gl 337D","Unresolved Parent"],
        ["Gl 337C", "Gl 337CD","Resolved Child"],
        ["Gl 337D", "Gl 337CD","Resolved Child"],
        ["Gl 337CD", "Gl 337","Child"],
        ["Gl 337C", "Gl 337","Child"],
        ["Gl 337D", "Gl 337","Child"],
        ["Gl 417B", "Gl 417C","Sibling"], 
        ["Gl 417C", "Gl 417B","Sibling"], 
        ["Gl 417BC", "Gl 417B","Unresolved Parent"], 
        ["Gl 417BC", "Gl 417C","Unresolved Parent"],
        ["Gl 417B", "Gl 417BC","Resolved Child"],
        ["Gl 417C", "Gl 417BC","Resolved Child"],
        ["Gl 417BC", "Gl 417","Child"],
        ["Gl 417B", "Gl 417","Child"],
        ["Gl 417C", "Gl 417","Child"],
]
for source,companion,relationship in result:
    ingest_companion_relationships(
        db=db,
        source = source,
        companion_name = companion,
        relationship=relationship,
        use_simbad=False
    )
    companions_ingested+=1

#ingest eta Cnc B separately because the projected separation is unclear: 2.2-3.5
ingest_companion_relationships(
    db=db,
    source = "eta Cnc B",
    companion_name="eta Cnc",
    relationship="Child"
)
companions_ingested+=1

#ingest companion relationships
for row in calamari_table:
    object = str(row['Object'])
    primary = str(row['Primary'])
    #we ingested this relationship separately
    if object == "eta Cnc B":
        continue
    separation_arcsec = float(row["Separation (arcsec)"])
    #check if companion relationship exists
    #UCD, or object, as the child
    if not companionExists(object, primary):
        ingest_companion_relationships(
            db=db,
            source = object,
            companion_name = primary,
            relationship = "Child",
            projected_separation_arcsec=separation_arcsec,
        )
        companions_ingested+=1
    else:
        companions_already_exists+=1
    print(f"Companions ingested: {companions_ingested}")

logger.info(f"companion relationships ingested:{companions_ingested}")  # 67 ingested
logger.info(f"companion relationships already exists:{companions_already_exists}")  # 15 due to preexisting data
logger.info(f"total companion relationships:{companions_ingested+companions_already_exists}")  # 82 total
# WRITE THE JSON FILES
if SAVE_DB:
    db.save_database(directory="data/")