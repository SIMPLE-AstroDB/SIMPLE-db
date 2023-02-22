from scripts.ingests.ingest_utils import *
from sqlalchemy import select, or_, and_

SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files

db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)

logger.setLevel(logging.INFO)

# Start the dictionary of all the author codes that need to be updated

stm = select([db.Publications.c.publication, db.Publications.c.bibcode]).where(or_
                                                                                 (db.Publications.c.publication.like(
                                                                                     '______a'),
                                                                                  db.Publications.c.publication.like(
                                                                                      '______b')))
result = db.session.execute(stm)
# print(result.all())
# start at bidcode[17:-1]
