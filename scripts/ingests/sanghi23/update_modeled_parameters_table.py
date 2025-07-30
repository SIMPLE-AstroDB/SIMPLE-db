"""Script to update ModeledParameters table structure in the database"""
import sys
sys.path.append(".")
import sqlalchemy as sa
from astrodb_utils import load_astrodb
from simple import REFERENCE_TABLES

# Establish connection to database
db = load_astrodb("SIMPLE.sqlite", recreatedb=False, reference_tables=REFERENCE_TABLES)

with db.engine.begin() as conn:
    # Step 1: Add new columns (if they don't already exist)
    conn.execute(sa.text('ALTER TABLE ModeledParameters ADD COLUMN model TEXT;'))
    conn.execute(sa.text('ALTER TABLE ModeledParameters ADD COLUMN upper_error REAL;'))
    conn.execute(sa.text('ALTER TABLE ModeledParameters ADD COLUMN lower_error REAL;'))

    # Step 2: Copy value_error into upper_error and lower_error
    conn.execute(sa.text('''
        UPDATE ModeledParameters
        SET upper_error = value_error,
            lower_error = value_error
        WHERE value_error IS NOT NULL;
    '''))

    # Step 3: Create a temporary table without value_error
    conn.execute(sa.text('''
        CREATE TABLE ModeledParameters_temp AS
        SELECT source, model, parameter, value,
               upper_error, lower_error, unit,
               comments, reference
        FROM ModeledParameters;
    '''))

    # Step 4: Drop original table and rename temp table
    conn.execute(sa.text('DROP TABLE ModeledParameters;'))
    conn.execute(sa.text('ALTER TABLE ModeledParameters_temp RENAME TO ModeledParameters;'))

# Reconnect to reflect changes
db = load_astrodb("SIMPLE.sqlite", recreatedb=False, reference_tables=REFERENCE_TABLES)

# Confirm table change
db.query(db.ModeledParameters).limit(10).table()

# Save updated database
db.save_database('data/')
