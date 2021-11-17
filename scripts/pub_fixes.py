from scripts.ingests.ingest_utils import *

db = load_simpledb('SIMPLE.db', recreatedb=True)

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

# Schm10b is in the database twice, both as Schm10 and Schm10b.
# Rename Schm10 to Schm10.1808. This propogates to the Sources table
stmt = db.Publications.update().where(db.Publications.c.name == 'Schm10').values(name = 'Schm10.1808')
db.engine.execute(stmt)

# Change Schm10b instantes to Schm10.1808
stmt = db.Sources.update().where(db.Sources.c.reference == 'Schm10b').values(reference = 'Schm10.1808')
db.engine.execute(stmt)

stmt = db.ProperMotions.update().where(db.ProperMotions.c.reference == 'Schm10b').values(reference = 'Schm10.1808')
db.engine.execute(stmt)

# Delete Schm10b
db.Publications.delete().where(db.Publications.c.name == 'Schm10b').execute()

# Rename Schm10a to Schm10.1045
stmt = db.Publications.update().where(db.Publications.c.name == 'Schm10a').values(name = 'Schm10.1045')
db.engine.execute(stmt)

db.save_database('data')
