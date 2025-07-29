import sys
import sqlalchemy
sys.path.append(".")
from astrodb_utils.sources import (
    logger,
    AstroDBError,
    find_source_in_db,
    ingest_name
)
from astrodb_utils.publications import (
    find_publication,
)
from simple.utils.astrometry import (
    ingest_parallax
)

#helper method to retrieve the publication links from calamari_data
def getRef(ref_index, ref_table):
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

def otherReferencesList(db, ref, ref_table):
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


# sources Gl 337CD and Gl 417BC do not exist in the database. ingest them.
def ingest_resolved_children(
    db,
    source,
    reference: str,
    *,
    ra: float = None,
    dec: float = None,
    epoch: str = None,
    equinox: str = None,
    other_reference: str = None,
    comment: str = None,
    raise_error: bool = True,
    ra_col_name: str = "ra",
    dec_col_name: str = "dec",
    epoch_col_name: str = "epoch",
):
    # Construct data to be added
    source_data = [
        {
            "source": source,
            ra_col_name: ra,
            dec_col_name: dec,
            "reference": reference,
            epoch_col_name: epoch,
            "equinox": equinox,
            "other_references": other_reference,
            "comments": comment,
        }
    ]
    logger.debug(f"   Data: {source_data}.")

    # Try to add the source to the database
    try:
        with db.engine.connect() as conn:
            conn.execute(db.Sources.insert().values(source_data))
            conn.commit()
        msg = f"Added {source_data}"
        logger.info(f"Added {source}")
        logger.debug(msg)
    except sqlalchemy.exc.IntegrityError:
        msg = f"Not ingesting {source}. Not sure why. \n"
        msg2 = f"   {source_data} "
        logger.warning(msg)
        logger.debug(msg2)

    # Add the source name to the Names table
    ingest_name(db, source=source, other_name=source, raise_error=raise_error)
    return
