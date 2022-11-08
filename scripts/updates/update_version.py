# Script to show how to update the version number

import logging
from scripts.ingests.utils import logger, load_simpledb

SAVE_DB = True  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files

logger.setLevel(logging.INFO)

db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)

# Check all versions
print(db.query(db.Versions).table())

# Add new version, add new entries as appropriate
# Note that start_date and end_date are strings of the date in format YYYY-MM-DD
data = [{'version': '2022.4',
         'start_date': '2022-10-14',
         'end_date': '2022-10-16',
         'description': ''}]
db.Versions.insert().execute(data)

# Fetch data of latest release
latest_date = db.query(db.Versions.c.end_date).order_by(db.Versions.c.end_date.desc()).limit(1).table()
latest_date = latest_date['end_date'][0]

latest = db.query(db.Versions).filter(db.Versions.c.version == 'latest').count()
if latest == 1:
    db.Versions.delete().where(db.Versions.c.version == 'latest').execute()

# Add latest
data = [{'version': 'latest', 'start_date': latest_date, 'description': 'Version in development'}]
db.Versions.insert().execute(data)

print(db.query(db.Versions).table())

# Save the database
if SAVE_DB:
    db.save_database(directory='data/')
