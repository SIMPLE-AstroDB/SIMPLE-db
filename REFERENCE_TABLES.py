from astrodbkit2.astrodb import create_database,Database
from simple.schema import *

connection_string = 'sqlite:///SIMPLE.db.py' # connection string for a SQLite database named SIMPLE.db.py
create_database(connection_string)
db = Database(connection_string)

db.load_database('data/')