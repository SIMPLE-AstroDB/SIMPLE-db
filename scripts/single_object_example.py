# Example on how to load a single, manually created object into the database

from astrodbkit2.astrodb import *

# Establish connection to database
connection_string = 'postgresql://localhost/SIMPLE'  # Postgres
connection_string = 'sqlite:///SIMPLE.db'  # SQLite
db = Database(connection_string)

# If brand new database, run the following
# NOTE: Some databases, like Postgres, will need an empty database created first before running this
from astrodbkit2.schema import *
create_database(connection_string)

# Adding information for 2MASS J13571237+1428398

# Add references
publications_data = [{'name': 'Schm10',
                      'bibcode': '2010AJ....139.1808S',
                      'doi': '10.1088/0004-6256/139/5/1808',
                      'description': 'Colors and Kinematics of L Dwarfs From the Sloan Digital Sky Survey'},
                     {'name': 'Cutr12',
                      'bibcode': '2012yCat.2311....0C',
                      'doi': None,
                      'description': 'WISE All-Sky Data Release'}]
db.Publications.insert().execute(publications_data)

# Add telescope
db.Telescopes.insert().execute([{'name': 'WISE'}])

# Add source
sources_data = [{'ra': 209.301675, 'dec': 14.477722,
                 'source': '2MASS J13571237+1428398',
                 'reference': 'Schm10',
                 'shortname': '1357+1428'}]
db.Sources.insert().execute(sources_data)

# Additional names
names_data = [{'source': '2MASS J13571237+1428398',
               'other_name': 'SDSS J135712.40+142839.8'},
              {'source': '2MASS J13571237+1428398',
               'other_name': '2MASS J13571237+1428398'},
              ]
db.Names.insert().execute(names_data)

# Add Photometry
phot_data = [{'source': '2MASS J13571237+1428398',
              'band': 'WISE_W1',
              'magnitude': 13.348,
              'magnitude_error': 0.025,
              'telescope': 'WISE',
              'reference': 'Cutr12'
              },
             {'source': '2MASS J13571237+1428398',
              'band': 'WISE_W2',
              'magnitude': 12.990,
              'magnitude_error': 0.028,
              'telescope': 'WISE',
              'reference': 'Cutr12'
              },
             {'source': '2MASS J13571237+1428398',
              'band': 'WISE_W3',
              'magnitude': 12.476,
              'magnitude_error': 0.279,
              'telescope': 'WISE',
              'reference': 'Cutr12'
              },
             {'source': '2MASS J13571237+1428398',
              'band': 'WISE_W4',
              'magnitude': 9.560,
              'magnitude_error': None,
              'telescope': 'WISE',
              'reference': 'Cutr12'
              }]
db.Photometry.insert().execute(phot_data)

# Checking object
_ = db.inventory('2MASS J13571237+1428398', pretty_print=True)

# Save single object
db.save_json('2MASS J13571237+1428398', 'data')

# Save entire database to directory 'data'
db.save_db('data')
