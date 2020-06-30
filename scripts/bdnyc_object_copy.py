# Script to copy over BDNYC database sources into SIMPLE

from astrodbkit2.astrodb import Database, copy_database_schema
from sqlalchemy import types  # for BDNYC column overrides

# Establish connection to databases

# Note that special parameters have to be passed to allow the BDNYC schema work properly
connection_string = 'sqlite:///../BDNYCdevdb/bdnycdev.db'
bdnyc = Database(connection_string,
                 reference_tables=['changelog', 'data_requests', 'publications', 'ignore', 'modes',
                                   'systems', 'telescopes', 'versions', 'instruments'],
                 primary_table='sources',
                 primary_table_key='id',
                 foreign_key='source_id',
                 column_type_overrides={'spectra.spectrum': types.TEXT(),
                                        'spectra.local_spectrum': types.TEXT()})

# SIMPLE
connection_string = 'sqlite:///SIMPLE.db'
db = Database(connection_string)

# Copy first publications that are not already in SIMPLE
temp = db.query(db.Publications.c.name).all()
existing_simple = [s[0] for s in temp]
temp = bdnyc.query(bdnyc.publications)\
    .filter(db.publications.c.shortname.notin_(existing_simple))\
    .all()

# Reformat data into something easier for SIMPLE to import
new_db_mapping = {'DOI': 'doi', 'shortname': 'name'}
data = [{new_db_mapping.get(k, k): x.__getattribute__(k)
         for k in x.keys() if k not in 'id'
         }
        for x in temp]

db.Publications.insert().execute(data)

# Verify insert and save to disk
db.query(db.Publications).count()
db.save_db('data')
