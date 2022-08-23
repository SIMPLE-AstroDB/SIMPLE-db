# Script to show how to update the version number

import logging
from astrodbkit2 import REFERENCE_TABLES
from datetime import datetime
from scripts.ingests.utils import logger, load_simpledb

logger.setLevel(logging.INFO)

db = load_simpledb('SIMPLE.db', recreatedb=True, reference_tables=REFERENCE_TABLES+['Versions'])

# Check all versions
print(db.query(db.Versions).table())

# Add new version, add new entries as appropriate
data = [{'version': '2022.1', 'start_date': datetime(2020, 6, 22), 'end_date': datetime(2022, 8, 23),
         'description': 'Initial release'}]
db.Versions.insert().execute(data)

# Fetch data of latest release
latest_date = db.query(db.Versions.c.end_date).order_by(db.Versions.c.end_date.desc()).limit(1).table()
latest_date = latest_date['end_date'].value[0]

latest = db.query(db.Versions).filter(db.Versions.c.version == 'latest').count()
if latest == 0:
    # Add latest if not present
    data = [{'version': 'latest', 'start_date': latest_date, 'description': 'Version in development'}]
    db.Versions.insert().execute(data)
else:
    # Update if present
    db.Versions.update().where(db.Versions.c.version == 'latest').values(start_date='latest_date').execute()

# Save the database
db.save_database('data/')
