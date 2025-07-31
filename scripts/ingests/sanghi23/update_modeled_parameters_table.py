"""Script to update ModeledParameters table structure in the database"""
import sys
sys.path.append(".")
import sqlalchemy as sa
from astrodb_utils import load_astrodb
from simple import REFERENCE_TABLES

# Establish connection to database
db = load_astrodb("SIMPLE.sqlite", recreatedb=False, reference_tables=REFERENCE_TABLES)

with db.engine.begin() as conn:
    # Add new columns
    conn.execute(sa.text('ALTER TABLE ModeledParameters ADD COLUMN other_references TEXT;'))

    # Create a temporary table without value_error
    conn.execute(sa.text('''
        CREATE TABLE ModeledParameters_temp AS
        SELECT source, model, parameter, value,
               lower_error, upper_error, unit, adopted,
               comments, reference, other_references
        FROM ModeledParameters;
    '''))

    # Drop original table and rename temp table
    conn.execute(sa.text('DROP TABLE ModeledParameters;'))
    conn.execute(sa.text('ALTER TABLE ModeledParameters_temp RENAME TO ModeledParameters;'))

# Reconnect to reflect changes
db = load_astrodb("SIMPLE.sqlite", recreatedb=False, reference_tables=REFERENCE_TABLES)

# Confirm table change
db.query(db.ModeledParameters).limit(10).table()

# Save updated database
db.save_database('data/')


