# Script to generate database from JSON contents
# This gets run automatically with Github Actions

import os
from astrodbkit2.astrodb import create_database, Database
from simple.schema import *

DB_NAME = 'SIMPLE.db'
DB_PATH = 'data'

# First, remove the existing database in order to recreate it from the schema
# If the schema has not changed, this part can be skipped
if os.path.exists(DB_NAME):
    os.remove(DB_NAME)
connection_string = 'sqlite:///' + DB_NAME
create_database(connection_string)

# Now that the database is created, connect to it and load up the JSON data
db = Database(connection_string)
db.load_database(DB_PATH, verbose=False)

# Close all connections
db.session.close()
db.engine.dispose()
