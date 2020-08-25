# Query examples against the database

from astrodbkit2.astrodb import Database, and_, or_

# Establish connection to database
connection_string = 'sqlite:///SIMPLE.db'
db = Database(connection_string)

# Query for all sources
results = db.query(db.Sources).all()
print(results)

# Alternative output formats
db.query(db.Sources).astropy()
db.query(db.Sources).table()  # equivalent to astropy
db.query(db.Sources).pandas()

# Query for all publications
db.query(db.Publications).all()

# Query for sources with declinations larger than 0
db.query(db.Sources).filter(db.Sources.c.dec > 0).all()

# Query and sort sources by declination
db.query(db.Sources.c.source).order_by(db.Sources.c.dec).all()

# Query for join Sources and Publications and return just several of the columns
results = db.query(db.Sources.c.source, db.Sources.c.reference, db.Publications.c.name)\
            .join(db.Publications, db.Sources.c.reference == db.Publications.c.name)\
            .all()
print(results)

# Query with AND
db.query(db.Sources).filter(and_(db.Sources.c.dec > 0, db.Sources.c.ra > 200)).all()

# Query with OR
db.query(db.Sources).filter(or_(db.Sources.c.dec < 0, db.Sources.c.ra > 200)).all()

# Update a row
stmt = db.Sources.update()\
         .where(db.Sources.c.source == '2MASS J13571237+1428398')\
         .values(shortname='1357+1428')
db.engine.execute(stmt)

# Testing foreign key updates
stmt = db.Publications.update()\
         .where(db.Publications.c.name == 'Cutr12')\
         .values(name='FAKEREF')
db.engine.execute(stmt)
db.query(db.Publications).filter(db.Publications.c.name == 'FAKEREF').all()
results = db.query(db.Photometry).filter(db.Photometry.c.reference == 'FAKEREF').all()
print(results)

# Direct SQL queries
results = db.sql_query('select * from sources')
print(results)
print(results[0].keys())

# Use inventory to check a single object (output is dictionary)
data = db.inventory('2MASS J13571237+1428398', pretty_print=True)
print(type(data))

# Search method
db.search_object('twa 27', fmt='astropy')
db.search_object('twa 27', fmt='astropy', resolve_simbad=True, output_table='Names')
db.search_object('1357+1428', output_table='Photometry', fmt='astropy')

# Delete a row
for row in db.query(db.Photometry).all():
    print(row)
db.Photometry.delete().where(db.Photometry.c.band == 'WISE_W1').execute()
db.query(db.Photometry).all()
db.query(db.Photometry).count()

# Delete entire table
# NOTE: data linked via foreign keys are also deleted
db.query(db.Telescopes).all()
db.Telescopes.delete().execute()
db.query(db.Telescopes).count()

# Reload via json file
db.load_table('Telescopes', 'data')

# Reload everything
# NOTE: this clears existing DB values to replace them with on-disk file data
db.load_database('data', verbose=True)
