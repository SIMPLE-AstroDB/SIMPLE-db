# Script to identify and fix duplicate publications in the database
import logging
from sqlalchemy import func, and_
from scripts.ingests.utils import load_simpledb, logger, find_publication

logger.setLevel(logging.INFO)

db = load_simpledb('SIMPLE.db', recreatedb=True)

# ---------------------------------------------------------------------------
# Identify publications with same bibcode, removing blanks
t = db.query(db.Publications.c.bibcode, func.count(db.Publications.c.bibcode).label('counts')). \
    filter(and_(db.Publications.c.bibcode.is_not(None), db.Publications.c.bibcode.is_not(''))). \
    group_by(db.Publications.c.bibcode). \
    having(func.count(db.Publications.c.bibcode) > 1). \
    astropy()

for bibcode in t['bibcode']:
    find_publication(db, bibcode=bibcode)

# ---------------------------------------------------------------------------
# Manual fixes to publications

# This transformation dictionary contains the references that are to be converted
# Additional entries can be added
to_transform = {'Gold99': 'Eros99c',
                'Luhm06a': 'Luhm09',
                'Geli11a': 'Geli11',
                'MeHi06': 'Metc06',
                'MeHi04': 'Metc04',
                'Zapa04b': 'Zapa04',
                'Gizi00c': 'Gizi00'}

# Loop through transformation dictionary to identify and delete cases
dry_run = False
for key, value in to_transform.items():
    data = db.search_string(key, verbose=False)
    if len(data) > 0:
        print(f'Tranforming {key} -> {value}')
        # Fix the table entries
        for table_name in data.keys():
            if table_name == 'Publications':
                continue
            print(f'Updating {table_name}')
            print(data[table_name])
            table = db.metadata.tables[table_name]  # this selects the Table object in the database by name
            if not dry_run:
                table.update().where(table.c.reference == key).values(reference=value).execute()

        # Delete the publication entry
        if not dry_run:
            db.Publications.delete().where(db.Publications.c.publication == key).execute()

# Save the modified database
db.save_database('data/')
