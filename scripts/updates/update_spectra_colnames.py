"""Script to update a column name in the database"""

import sqlalchemy as sa
from astrodb_utils import load_astrodb
from simple.schema import REFERENCE_TABLES

# Establish connection to database
db = load_astrodb("SIMPLE.sqlite", recreatedb=True, reference_tables=REFERENCE_TABLES)

# Perform column rename
with db.engine.connect() as conn:
    conn.execute(sa.text('ALTER TABLE Spectra RENAME COLUMN spectrum TO access_url;'))
    conn.commit()

# Reflect table changes, because of the ALTER commands
db = load_astrodb('SIMPLE.sqlite', recreatedb=False, reference_tables=REFERENCE_TABLES)

# Inspect change
db.query(db.Spectra).limit(10).table()

# Save changes
db.save_database('data/')