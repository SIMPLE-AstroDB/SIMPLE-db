# Script to handle some rework of the PhotometryFilters table

# pylint: disable=all

import sqlalchemy as sa
from scripts.ingests.utils import *

logger.setLevel(logging.INFO)

db = load_simpledb('SIMPLE.db', recreatedb=False)

# Drop the instrument and telescope column
with db.engine.connect() as conn:
    conn.execute(sa.text('ALTER TABLE PhotometryFilters DROP COLUMN instrument'))
    conn.execute(sa.text('ALTER TABLE PhotometryFilters DROP COLUMN telescope'))
    conn.commit()

# Move UCD values from Photometry to PhotometryFilters
photometry = db.query(db.Photometry.c.band, db.Photometry.c.ucd).distinct().where(db.Photometry.c.ucd != None).pandas()
phot_filters = db.query(db.PhotometryFilters).pandas()

with db.engine.connect() as conn:
    for i, row in photometry.iterrows():
        print(f"Setting {row['ucd']} for {row['band']}")
        # Make update command
        conn.execute(db.PhotometryFilters.update().where(db.PhotometryFilters.c.band == row['band']).values(ucd=row['ucd']))
    # Run all update
    conn.commit()

# Drop the ucd and instrument column from Photometry
with db.engine.connect() as conn:
    conn.execute(sa.text('ALTER TABLE Photometry DROP COLUMN ucd'))
    conn.execute(sa.text('ALTER TABLE Photometry DROP COLUMN instrument'))
    conn.commit()

# Re-instantiate with changed DB, but don't build from JSON files
db = load_simpledb('SIMPLE.db', recreatedb=False)

# Save modified database
db.save_database('data/')
