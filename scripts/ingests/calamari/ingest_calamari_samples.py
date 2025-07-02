import sys
import logging
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
    ingest_publication
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
link_3 = (
    "scripts/ingests/calamari/primary_sources.csv"
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

primary_sources_table = ascii.read(
    link_3,
    format = "csv",
    data_start=1,
    header_start=0,
    guess=False,
    fast_reader=False, 
    delimiter=",",
    )

sources_ingested = 0
sources_already_exists = -3 #ingest 3 sources at the start. They will not be counted as already existing in the database
ref_ingested = 0
companions_ingested = 0
companions_already_exists = 0
ref_already_exists = 2 #Roth24 and Schl03 already exist in database

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

#ingest publication: Curr20
ingest_publication(
    db=db,
    doi = "10.3847/2041-8213/abc631"
)
ref_ingested+=1

for row in ref_table:
    #ingest publications
    #get the ADS link
    pub = row['ADS']
    #if the link doesn't provide the ADS key directly...
    pub_2 = row['Link']
    #if link provides ADS
    if 'harvard.edu' in pub:
        bib = extractADS(pub)
        print(bib)
        pub_found = find_publication(
            db = db,
            bibcode = bib
        )
        if pub_found[0] == False:
            ingest_publication(
                db=db,
                bibcode= bib
            )
            ref_ingested+=1
        else:
            ref_already_exists+=1
    #if link provides doi
    if 'iopscience' in pub:
        doi = extractDOI(pub)
        pub_found = find_publication(
            db=db,
            doi=doi,
        )
        if pub_found[0] == False:
            ingest_publication(
                db=db,
                doi= doi
            )
            ref_ingested+=1
        else:
            ref_already_exists+=1
    if 'doi.org' in pub_2:
        doi = extractDOI(pub_2)
        pub_found = find_publication(
            db=db,
            doi=doi,
        )
        if pub_found[0] == False:
            ingest_publication(
                db=db,
                doi= doi
            )
            ref_ingested+=1
        else:
            ref_already_exists+=1
    if pub == None:
        continue


#ingest source WISE J124332.17+600126.6
ingest_source(
    db=db,
    source = "WISE J124332.17+600126.6",
    reference="Fahe21",
    ra=190.88386,
    dec=60.023957,
    ra_col_name="ra",
    dec_col_name="dec"
)
sources_ingested+=1

# #ingest source BD+60 1417
ingest_source(
    db=db,
    source = "BD+60 1417",
    reference="Fahe21",
    ra = 190.888634,
    dec = 60.01464,
    ra_col_name="ra",
    dec_col_name="dec",
    search_db=False
)
sources_ingested+=1

#ingest source HD 2057
ingest_source(
    db=db,
    source = "HD 2057",
    reference="Reid06.891",
    ra = 6.286472,
    dec = 48.047403,
    ra_col_name="ra",
    dec_col_name="dec"
)
sources_ingested+=1

object_index=0
#ingest the sources
for row in calamari_table:
    #read in row
    Dec = row['Dec']
    RA = row['RA']
    object = str(row['Object'])

    # check the object in the row is in the DB
    #if not, ingest
    obj_result = find_source_in_db(db=db, source = object, ra=RA, dec=Dec, ra_col_name="ra", dec_col_name="dec")
    ref_list = otherReferencesList(calamari_table[object_index]['Ref'])
    if len(obj_result)==0:
        #if the source has multiple references
        if(len(ref_list)>1):
            ingest_source(
                db=db,
                source = object,
                reference = ref_list[0],
                other_reference= ",".join(map(str, ref_list[1:])),
                ra = RA,
                dec = Dec,
                ra_col_name="ra",
                dec_col_name="dec"
            )
            sources_ingested+=1
        else:
            #if the source has one reference
            ingest_source(
                db=db,
                source = object,
                reference=ref_list[0],
                ra = RA,
                dec = Dec,
                ra_col_name = "ra",
                dec_col_name = "dec"
            )
            sources_ingested+=1
    elif len(obj_result)==1:
        #ingest names
        ingest_name(
            db=db,
            source=obj_result[0],
            other_name=object
        )
        sources_already_exists+=1
    else: 
        sources_already_exists+=1
    object_index+=1

primary_index=0
for row in primary_sources_table:
    #read in the data
    primary = str(row['Primary'])
    ref_list = otherReferencesList(calamari_table[primary_index]['Ref'])
    RA = row['Primary RA']
    Dec = row['Primary Dec']
    # check the primary in the row is in the DB
    #if not, ingest
    primary_result = find_source_in_db(db=db, source = primary, ra=RA, dec=Dec, ra_col_name="ra", dec_col_name="dec")
    if len(primary_result)==0:
        #if source has multiple references
        if(len(ref_list)>1):
            ingest_source(
                db=db,
                source = primary,
                reference = ref_list[0],
                other_reference= ",".join(map(str, ref_list[1:])),
                ra = RA,
                dec = Dec,
                ra_col_name="ra",
                dec_col_name="dec"
            )
            sources_ingested+=1
        else:
            #if source only has one reference
            ingest_source(
                db=db,
                source = primary,
                reference=ref_list[0],
                ra = RA,
                dec = Dec,
                ra_col_name = "ra",
                dec_col_name = "dec"
            )
            sources_ingested+=1
    elif len(primary_result)==1:
        #ingest names
        ingest_name(
            db=db,
            source=primary_result[0],
            other_name=primary
        )
        sources_already_exists+=1
    else: 
        sources_already_exists+=1
    primary_index+=1

#ingest companion relationships
for row in calamari_table:
    object = str(row['Object'])
    primary = str(row['Primary'])
    #check if companion relationship exists
    #UCD, or object, as the child
    if not companionExists(object, primary):
        ingest_companion_relationships(
            db=db,
            source = object,
            companion_name = primary,
            relationship = "Child",
        )
        companions_ingested+=1
    else:
        companions_already_exists+=1
    if not companionExists(primary, object):
        #Primary as the parent
        ingest_companion_relationships(
            db=db,
            source = primary,
            companion_name = object,
            relationship = "Parent",
        )
        companions_ingested+=1
    else:
        companions_already_exists+=1
    print(f"Companions ingested: {companions_ingested}")

logger.info(f"references ingested:{ref_ingested}")  # 10 references ingested
logger.info(f"references already exists:{ref_already_exists}")  # 24 references due to preexisting data
logger.info(f"total references:{ref_ingested+ref_already_exists}")  # 34 references total
logger.info(f"sources ingested:{sources_ingested}")  # 42 ingested
logger.info(f"sources already exists:{sources_already_exists}")  # 74 due to preexisting data
logger.info(f"total sources:{sources_ingested+sources_already_exists}")  # 116 sources total
logger.info(f"companion relationships ingested:{companions_ingested}")  # 101 ingested
logger.info(f"companion relationships already exists:{companions_already_exists}")  # 15 due to preexisting data
logger.info(f"total companion relationships:{companions_ingested+companions_already_exists}")  # 116 total
# WRITE THE JSON FILES
if SAVE_DB:
    db.save_database(directory="data/")