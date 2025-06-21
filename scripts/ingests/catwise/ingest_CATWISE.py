from astrodb_utils import load_astrodb
import sys
sys.path.append(".")
from simple import *
from simple import REFERENCE_TABLES
from astropy.io import ascii
from astrodb_utils.publications import (
    logger,
    find_publication,
    ingest_publication
)
from astrodb_utils.sources import (
    find_source_in_db,
    ingest_source,
    AstroDBError
)
from sqlalchemy import select
from astroquery.ipac.irsa import Irsa #we will use astroquery 
from astropy.coordinates import SkyCoord
import astropy.units as u


SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files
SCHEMA_PATH = "simple/schema.yaml" 
# LOAD THE DATABASE
db = load_astrodb("SIMPLE.sqlite", recreatedb=RECREATE_DB, reference_tables=REFERENCE_TABLES, felis_schema=SCHEMA_PATH)

# generates all the sources in the db
sources_table = select(db.Sources)
with db.engine.connect() as conn:
    sources = conn.execute(sources_table).mappings().all()

# should be 3598
logger.info(f"Found {len(sources)} sources to process.")

# manually ingest the publication that the catalog uses
ingest_publication(db = db, doi = "10.3847/1538-4365/abd805")

pub = find_publication(db = db, doi = "10.3847/1538-4365/abd805")[1]


for row in sources:
    #create skycoord object because one of the parameters for query region for position
    coord = SkyCoord(ra = row["ra"], dec = row["dec"], unit = "deg", frame = "icrs")

    # generates a list of objects from the catwise2020 catalogs that are within this radius of a certain position/coordinate
    results = Irsa.query_region(coordinates=coord, spatial='Cone', catalog='catwise_2020', radius=2 * u.arcmin, columns="source_name,PMRA,sigPMRA,PMDec,sigPMDec,ab_flags,cc_flags,w1mpro,w1sigmpro,w2mpro,w2sigmpro,ra,dec")

    # generates a list of filtered results
    filtered_results = []
    for source in results:
        if (source["ab_flags"] == '00') & (source["cc_flags"] == '0000'):
            filtered_results.append(source["source_name"])

    #use closest/best match
        try:
            match = filtered_results[0]
            logger.info("source match found")
       
            source_row = results[results["source_name"] == match]


            ingested_source = find_source_in_db(
                db = db,
                source = source_row[0][0],
                ra = source_row[0][11],
                dec = source_row[0][12],
                ra_col_name="ra",
                dec_col_name="dec"
            )

            if(len(ingested_source)==0):
                ingest_source(
                    db = db,
                    source = source_row[0][0],
                    reference = pub,
                    ra = source_row[0][11],
                    dec = source_row[0][12],
                    ra_col_name="ra",
                    dec_col_name="dec",
                    raise_error=True,
                    search_db=True,
                )  # ingest new sources

            ingested_source = find_source_in_db(
                db = db,
                source = source_row[0][0],
                ra = source_row[0][11],
                dec = source_row[0][12],
                ra_col_name="ra",
                dec_col_name="dec"
            )[0]

            with db.engine.connect() as conn:
                #ingest proper motions
                conn.execute(
                    db.ProperMotions.insert().values(
                        {
                            "source": ingested_source,
                            "mu_ra": source_row[0][1],
                            "mu_ra_error": source_row[0][2],
                            "mu_dec": source_row[0][3],
                            "mu_dec_error": source_row[0][4],
                            "reference": pub
                        }
                    )
                )
                logger.info(f"{[ingested_source]} propermotions ingested")
                #ingest WISE W1 photometry
                conn.execute(
                    db.Photometry.insert().values(
                        {
                            "source": ingested_source,
                            "band": "WISE.W1",
                            "magnitude": source_row[0][7],
                            "magnitude_error": source_row[0][8],
                            "reference": pub
                        }
                    )
                )
                logger.info(f"{[ingested_source]} photometry band 1 ingested")
                #ingest WISE W2 photometry
                conn.execute(
                    db.Photometry.insert().values(
                        {
                            "source": ingested_source,
                            "band": "WISE.W2",
                            "magnitude": source_row[0][9],
                            "magnitude_error": source_row[0][10],
                            "reference": pub
                        }
                    )
                )
                logger.info(f"{[ingested_source]} photometry band 2 ingested")
            conn.commit()
        except IndexError:
            logger.warning("no source match found")
            

logger.info("done")


