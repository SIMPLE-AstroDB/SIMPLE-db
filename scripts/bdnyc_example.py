# Example on how to use the BDNYC database with Astrodbkit2

from astrodbkit2.astrodb import *
from sqlalchemy import types  # for BDNYC column overrides

# Establish connection to database
# Note that special parameters have to be passed to allow the BDNYC schema work properly
connection_string = 'sqlite:///../BDNYCdb-1/bdnyc_database.db'
db = Database(connection_string,
              reference_tables=['changelog', 'data_requests', 'publications', 'ignore', 'modes',
                                'systems', 'telescopes', 'versions', 'instruments'],
              primary_table='sources',
              primary_table_key='id',
              foreign_key='source_id',
              column_type_overrides={'spectra.spectrum': types.TEXT(),
                                     'spectra.local_spectrum': types.TEXT()})

# Query similarly to SIMPLE
results = db.query(db.sources).limit(10).all()
for row in results: print(row)

# The spectra table contains columns of type SPECTRUM, the column_type_overrides allows us to work with them as text
for c in db.spectra.columns: print(c, c.type)
db.query(db.spectra).limit(10).all()

_ = db.inventory(11, pretty_print=True)

# Can output the full contents of BDNYC as json files
db.save_db('bdnyc')
