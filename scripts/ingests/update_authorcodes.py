from scripts.ingests.ingest_utils import *
from sqlalchemy import select, or_
import pandas as pd

SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files

db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)

logger.setLevel(logging.INFO)

#  creating a query that finds all the author codes that need to be updated
stm = select([db.Publications.c.publication, db.Publications.c.bibcode]).where(
    or_(db.Publications.c.publication.like('______a'), db.Publications.c.publication.like('______b'),
        db.Publications.c.publication.like('______c'), db.Publications.c.publication.like('______d'),
        db.Publications.c.publication.like('______e'), db.Publications.c.publication.like('______f'),
        db.Publications.c.publication.like('______g'), db.Publications.c.publication.like('______h')))
result = db.session.execute(stm)
df = pd.DataFrame(result.all())
print(df)
# start at bidcode[17:-1]
