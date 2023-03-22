from astrodbkit2.astrodb import create_database
from astrodbkit2.astrodb import Database
from astropy.table import Table

connection_string = 'sqlite:///SIMPLE.db'  # connection string for a SQLite database named SIMPLE.db
create_database(connection_string)
db = Database(connection_string)
db.load_database('data/')

filip_table = Table.read("Fili15_table9.csv", data_start=1)
