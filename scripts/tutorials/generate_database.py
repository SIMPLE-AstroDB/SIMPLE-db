# Script to generate database from JSON contents
# This gets run automatically with Github Actions

import sys
import os
from astrodbkit2.astrodb import create_database, Database
sys.path.append(os.getcwd())  # hack to be able to discover simple
from simple.schema import *

# Location of source data
DB_PATH = 'data'
# Postgres settings
# DB_TYPE = 'postgres'
# DB_NAME = 'localhost/SIMPLE'
# SQLlite settings
DB_TYPE = 'sqlite'
DB_NAME = 'SIMPLE.db'

# Set correct connection string
if DB_TYPE == 'sqlite':
    # First, remove the existing database in order to recreate it from the schema
    # If the schema has not changed, this part can be skipped
    if os.path.exists(DB_NAME):
        os.remove(DB_NAME)
    connection_string = 'sqlite:///' + DB_NAME
elif DB_TYPE == 'postgres':
    # For Postgres, we connect and drop all database tables
    connection_string = 'postgresql://' + DB_NAME
    try:
        db = Database(connection_string)
        db.base.metadata.drop_all()
        db.session.close()
        db.engine.dispose()
    except RuntimeError:
        # Database already empty or doesn't yet exist
        pass

create_database(connection_string)

# Now that the database is created, connect to it and load up the JSON data
db = Database(connection_string)
db.load_database(DB_PATH, verbose=False)

print('New database generated.')

# Close all connections
db.session.close()
db.engine.dispose()
