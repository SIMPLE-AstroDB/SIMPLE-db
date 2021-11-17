from scripts.ingests.ingest_utils import *

db = load_simpledb('SIMPLE.db', recreatedb=False)

# Fix Duplicate reference with Mart99b and Mart99e
# Rename to Mart99.107
mart99 = find_publication(db, bibcode='1999AJ....118.2466M')

# Change Mart99e instantes to Mart99b
stmt = db.Sources.update().where(db.Sources.c.reference == 'Mart99e').values(reference = 'Mart99b')
db.engine.execute(stmt)

# Delete Mart99e
db.Publications.delete().where(db.Publications.c.name == 'Mart99e').execute()

# Update Mart99b to be Mart99.107
stmt = db.Publications.update().where(db.Publications.c.name == 'Mart99b').values(name = 'Mart99.107')
db.engine.execute(stmt)

db.save_database('data')
