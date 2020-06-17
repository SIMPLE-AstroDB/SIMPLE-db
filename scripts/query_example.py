# Query examples against the database

from astrodbkit2.astrodb import *

# Establish connection to database
connection_string = 'sqlite:///SIMPLE.db'
db = Database(connection_string)

# Query for all sources
results = db.query(db.Sources).all()
print(results)

# Query for all publications
print(db.query(db.Publications).all())

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

# Direct SQL queries
db.engine.execute('SELECT * FROM sources').fetchall()

# Use inventory to check a single object (output is dictionary)
data = db.inventory('2MASS J13571237+1428398', pretty_print=True)
print(type(data))

# TODO: Delete a row
