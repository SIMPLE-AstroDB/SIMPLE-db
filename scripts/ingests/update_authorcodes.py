from scripts.ingests.utils import *
from sqlalchemy import select, or_
import pandas as pd

SAVE_DB = True  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files

db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)

logger.setLevel(logging.INFO)

if RECREATE_DB:
    db.Publications.update().where(db.Publications.c.publication == 'Luyt79a').values(
        publication='Luyt79.5').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Luyt79b').values(
        publication='Luyt79.2').execute()

#  creating a query that finds all the author codes that need to be updated
stm = select([db.Publications.c.publication, db.Publications.c.bibcode]).where(
    or_(db.Publications.c.publication.like('______a'), db.Publications.c.publication.like('______b'),
        db.Publications.c.publication.like('______c'), db.Publications.c.publication.like('______d'),
        db.Publications.c.publication.like('______e'), db.Publications.c.publication.like('______f'),
        db.Publications.c.publication.like('______g'), db.Publications.c.publication.like('______h')))

result = db.session.execute(stm)
df = pd.DataFrame(result.all())

# Looping through old author codes and updating them
for publication, bibcode in zip(df[0], df[1]):
    if publication != "Missing":
        ind = bibcode.rfind('.')  # find index of the last period
        if ind != -1:
            # if a match was found, generate the new name
            new_publication_name = publication[0:6] + '.' + bibcode[ind + 1:-1]
            db.Publications.update().where(db.Publications.c.publication == publication).values(
                publication=new_publication_name).execute()

# WRITE THE JSON FILES
if SAVE_DB:
    db.save_database(directory='data/')