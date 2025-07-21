import sys
import logging
import sqlalchemy
sys.path.append(".")
from astropy.io import ascii
from simple import REFERENCE_TABLES
from astrodb_utils import load_astrodb
from astrodb_utils.sources import (
    find_source_in_db,
    ingest_source,
    logger,
    AstroDBError,
    ingest_name
)

from astrodb_utils.publications import (
    find_publication,
)

astrodb_utils_logger = logging.getLogger("astrodb_utils")
logger.setLevel(logging.DEBUG)  # Set logger to INFO/DEBUG/WARNING/ERROR/CRITICAL level
astrodb_utils_logger.setLevel(logging.DEBUG)

SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files
SCHEMA_PATH = "simple/schema.yaml"
db = load_astrodb(
    "SIMPLE.sqlite",
    recreatedb=RECREATE_DB,
    reference_tables=REFERENCE_TABLES,
    felis_schema=SCHEMA_PATH,
)

link = (
    "scripts/ingests/calamari_samples/calamari_data.csv"
)
link_2 = (
    "scripts/ingests/calamari_samples/calamari_refs.csv"
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

age_ingested = 0
age_already_exists = 0
skipped = 0

#helper method to retrieve the publication links from calamari_data
def getRef(ref_index):
    ref = ref_index.split(',')[0]
    ref_link = ref_table[int(ref)]['ADS']
    if 'iopscience' not in ref_link or 'harvard.edu' not in ref_link:
        ref_link = ref_table[int(ref)]['Link']
    return ref_link

#helper method to retrieve the bibcode from a link
def extractADS(link):
    start = link.find('abs/')+4
    end = link.find('/abstract')
    ads = link[start:end]
    ads = ads.replace("%26", "&")
    return ads

#helper method to retrieve the doi from a link
def extractDOI(link):
    link = str(link)
    if 'iopscience' in link:
        start = link.find('article/')+8
        doi = link[start:]
        doi = doi.replace("/pdf", "")
    else:
        start = link.find('doi.org/')+8
        doi=link[start:]
    return doi

def otherReferencesList(ref):
    #get all the ids/indexes of the references
    ids = ref.split(", ")
    result = []
    #for each reference...
    for id in ids:
        link = ref_table[int(id)]['ADS']
        #if bibcode or doi is not directly in the link... go to Link column
        if 'iopscience' not in link or 'harvard.edu' not in link:
            link = ref_table[int(id)]['Link']
        #if bibcode is directly in the link
        if 'harvard.edu' in link:
            bibcode = extractADS(link)
            pub_result=find_publication(
                db=db,
                bibcode=bibcode
            )
            if pub_result[0]:
                result.append(pub_result[1])
            else:
                print(f"Warning: Publication not found for bibcode {bibcode}")
            #if doi code is found directly in the link
        elif 'iopscience' in link or 'doi.org' in link:
            doi=extractDOI(link)
            pub_result=find_publication(
                db=db,
                doi=doi
            )
            if pub_result[0]:
                result.append(pub_result[1])
            else:
                print(f"Warning: Publication not found for doi {doi}")
        #use reference name to find reference
        else:
            reference= ref_table[int(id)]['Ref']
            reference= reference.replace("+", "")
            reference=reference[0:4] + reference[-2:]
            pub_result=find_publication(
                db=db,
                reference=reference
            )
            if pub_result[0]:
                result.append(pub_result[1])
            else:
                print(f"Warning: Publication not found for reference {reference}")
    #return list of references
    return result

def age_exists(db, companion, reference):
    exists = False
    #check if age exists
    age_search = db.search_object(
        name = companion,
        output_table="CompanionParameters",
        table_names={'CompanionRelationships':['companion_name']}
    )
    if len(age_search) > 0:
        for row in age_search:
            if ((row["reference"] == reference) and (row["parameter"] == "age")):
                exists = True
                break

    return exists

def ingest_ages(
        db,
        source: str, 
        companion: str, 
        age: float, 
        reference: str,
        comment: str = None,
        upper_error: float = None,
        lower_error: float = None,
        ):
    #check if the age is already in the database
    already_in_db = age_exists(db=db, companion=companion, reference=reference)
    #if the age is not in the database... ingest the new age
    if not already_in_db:
        # Construct data to be added
        age_data = [
            {
                "source": source,
                "companion": companion,
                "reference": reference,
                "parameter": "age",
                "value": age,
                "unit": "Gyr",
                "comments": comment,
                "upper_error": "{:.2f}".format(upper_error),
                "lower_error": "{:.2f}".format(lower_error),
            }
        ]
        logger.debug(f"   Data: {age_data}.")

        # Try to add the source to the database
        try:
            with db.engine.connect() as conn:
                conn.execute(db.CompanionParameters.insert().values(age_data))
                conn.commit()
            msg = f"Added {age_data}"
            logger.info(f"Added {companion} age")
            logger.debug(msg)
        except sqlalchemy.exc.IntegrityError as e:
            msg = f"Not ingesting {companion} age. IntegrityError: {str(e)}"
            #msg = f"Not ingesting {companion} age. Not sure why. \n"
            msg2 = f"   {age_data} "
            logger.warning(msg)
            logger.debug(msg2)
    return

def getAge(ageString):
    """
    returns:
    age: float
    upper bound: float
    lower bound: float
    range: string
    """
    age = None
    upperBound = 0.0
    lowerBound = 0.0
    range = None

    if '-' in ageString:
        ageRange = ageString.split('-')
        age = (float(ageRange[0])+float(ageRange[1]))/2
        upperBound = float(ageRange[1])-age
        lowerBound = age-float(ageRange[0])
        range = ageString
    elif '$<$ 1' in ageString or '<1' in ageString:
        age = 0.7
        upperBound = 0.3
        lowerBound = 0.3
        range = "0.4-1.0"
    else:
        age = ageString

    return age, upperBound, lowerBound, range

for row in calamari_table:
    references = otherReferencesList(row["Ref"])
    source = row["Object"]
    companion = row["Primary"]
    ageRow = row["Age (Gyr)"]

    #skip all the weird cases for now...
    if '$<$ 10' in ageRow or '$>$ 1.6' in ageRow or '...' in ageRow:
        skipped+=1
        continue

    age_results = getAge(ageRow)
    age = age_results[0]
    upper_error = age_results[1]
    lower_error = age_results[2]
    comment = age_results[3]

    #all cases with Deac14.119 in references uses this publication for the age.
    if("Deac14.119" in references):
        age_in_db = age_exists(db = db,
                            companion = companion,
                            reference="Deac14.119")
        if not age_in_db:
            ingest_ages(
                db=db,
                source = source,
                companion = companion,
                age = age,
                upper_error=upper_error,
                lower_error=lower_error,
                reference = "Deac14.119",
                comment=comment
            )
            age_ingested+=1
        else:
            age_already_exists+=1

    # for cases with 2 references with GaiaEDR3 as one of them, use the other reference
    elif len(references) == 2 and 'GaiaEDR3' in references:
        references_copy = references.copy()
        references_copy.remove('GaiaEDR3')
        age_in_db = age_exists(db = db,
                            companion = companion,
                            reference = references_copy[0])
        if not age_in_db:
            ingest_ages(
                db=db,
                source = source,
                companion = companion,
                age = age,
                upper_error=upper_error,
                lower_error=lower_error,
                reference = references_copy[0],
                comment=comment
            )
            age_ingested+=1
        else:
            age_already_exists+=1
    else:
        skipped+=1
        continue
    

logger.info(f"ages ingested:{age_ingested}")  # 36 ages ingested
logger.info(f"ages already exists:{age_already_exists}")  # 0 due to preexisting data
logger.info(f"ages skpped:{skipped}")  # 28 skipped
logger.info(f"total ages:{age_ingested+age_already_exists+ skipped}")  # 64 ages total
# WRITE THE JSON FILES
if SAVE_DB:
    db.save_database(directory="data/")