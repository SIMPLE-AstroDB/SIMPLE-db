from scripts.ingests.utils import *
from sqlalchemy import select, or_
import pandas as pd
import numpy as np

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
print(np.where(df[0]))
new_publications = []
small_names = []
for publication, bibcode in zip(df[0], df[1]):
    if bibcode is None:
        print(publication)
    # author_last_name = publication[:4]
    # new_publication_name = author_last_name + bibcode[17:-1]
    # new_publications.append(new_publications)
# print(new_publications)
# start at bidcode[17:-1]
