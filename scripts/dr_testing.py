# Using schema.py and db.py to handle the basic db connection and the database schema

from simple.core import load_connection, Database
from sqlalchemy import or_, and_

# Establish connection to database
# connection_string = 'postgresql://localhost/SIMPLE'
connection_string = 'postgresql+psycopg2://localhost/SIMPLE'  # postgres with psycopg2
# connection_string = 'sqlite://'  # in memory sqlite
connection_string = 'sqlite:///SIMPLE.db'

# Object approach
db = Database(connection_string)

# If brand new database:
from simple.schema import *
db.create_database()

# Add references
publications_data = [{'name': 'Penguin 101'},
                     {'name': 'Fake 201'}]
db.Publications.insert().execute(publications_data)

# Add sources
sources_data = [{'ra': 12, 'dec': 23, 'source': 'Fake 1', 'reference': 'Penguin 101'},
                {'ra': 212, 'dec': -23, 'source': 'Fake 2', 'reference': 'Fake 201'}
                ]
db.Sources.insert().execute(sources_data)

# Add source without valid reference- should through an foreign key error
sources_data = [{'ra': 32, 'dec': 54, 'source': 'Fake 3', 'reference': 'Unknown'}]
db.Sources.insert().execute(sources_data)
# Empty references will also fail
sources_data = [{'ra': 32, 'dec': 54, 'source': 'Fake 4'}]
db.Sources.insert().execute(sources_data)

# Spectral Types need regime (an enumeration) supplied in addition to a valid source and reference
sp_data = [{'source': 'Fake 1', 'spectral_type': 13, 'regime': 'optical', 'reference': 'Fake 201'}]
db.SpectralTypes.insert().execute(sp_data)

# Query examples
results = db.query(db.Sources).all()
print(results)
print(db.query(db.Publications).all())
print(db.query(db.SpectralTypes).all())

results = db.query(db.Sources).filter(db.Sources.c.dec > 0).all()
print(results)

results = db.query(db.Sources.c.name).order_by(db.Sources.c.dec).all()
print(results)

results = db.query(db.Sources.c.source, db.Sources.c.reference, db.Publications.c.name)\
    .join(db.Publications, db.Sources.c.reference == db.Publications.c.name).all()
print(results)

# Update a row
stmt = db.SpectralTypes.update().\
            where(and_(db.SpectralTypes.c.source == 'Fake 1', db.SpectralTypes.c.reference == 'Fake 201')).\
            values(spectral_type=15)
db.engine.execute(stmt)

# Direct SQL queries
results = db.engine.execute('SELECT * FROM sources').fetchall()
print(results)

# Alternative query style
stmt = db.Sources.select()
results = db.engine.execute(stmt)
print(list(results))

from sqlalchemy import select
select_st = select([db.Sources]).where(or_(db.Sources.c.name == 'Fake 1', db.Sources.c.name == 'Fake 2')).order_by(db.Sources.c.ra)
results = db.engine.execute(select_st)
for res in results:
    print(res)

# Inventory of all values for a source (still in progress)
db.inventory('Fake 1')


# Older scripts
session, base, engine = load_connection(connection_string)


# Create database (can skip if already done)
from simple.schema import *
base.metadata.drop_all()  # drop all the tables
base.metadata.create_all()  # this explicitly create the SQLite file


# Get tables
metadata = base.metadata
metadata.reflect(bind=engine)
print(metadata.tables)
Sources = metadata.tables['Sources']
Publications = metadata.tables['Publications']
SpectralTypes = metadata.tables['SpectralTypes']
print(Sources)

# Alternative, manual way of defining table
# from sqlalchemy import Table
# Sources = Table('Sources', base.metadata, autoload=True)
# Publications = Table('Publications', base.metadata, autoload=True)

# Add references
publications_data = [{'name': 'Penguin 101'},
                     {'name': 'Fake 201'}]
Publications.insert().execute(publications_data)

# Add sources
sources_data = [{'ra': 12, 'dec': 23, 'name': 'Fake 1', 'reference': 'Penguin 101'},
                {'ra': 212, 'dec': -23, 'name': 'Fake 2', 'reference': 'Fake 201'}
                ]
Sources.insert().execute(sources_data)

# Add source without valid reference- should through an foreign key error
sources_data = [{'ra': 32, 'dec': 54, 'name': 'Fake 3', 'reference': 'Unknown'}]
Sources.insert().execute(sources_data)
# Empty references will also fail
sources_data = [{'ra': 32, 'dec': 54, 'name': 'Fake 4'}]
Sources.insert().execute(sources_data)

# Spectral Types need regime (an enumeration) supplied in addition to a valid source and reference
sp_data = [{'source': 'Fake 1', 'spectral_type': 13, 'regime': 'optical', 'reference': 'Fake 201'}]
SpectralTypes.insert().execute(sp_data)

# Query examples
results = session.query(Sources).all()
print(results)
print(session.query(Publications).all())
print(session.query(SpectralTypes).all())

results = session.query(Sources).filter(Sources.c.dec > 0).all()
print(results)

results = session.query(Sources.c.name).order_by(Sources.c.dec).all()
print(results)

results = session.query(Sources.c.name, Sources.c.reference, Publications.c.name)\
    .join(Publications, Sources.c.reference == Publications.c.name).all()
print(results)

# Update a row
stmt = SpectralTypes.update().\
            where(and_(SpectralTypes.c.source == 'Fake 1', SpectralTypes.c.reference == 'Fake 201')).\
            values(spectral_type=15)
engine.execute(stmt)

# Direct SQL queries
results = engine.execute('SELECT * FROM sources').fetchall()
print(results)

# Alternative query style
stmt = Sources.select()
results = engine.execute(stmt)
print(list(results))

from sqlalchemy import select
select_st = select([Sources]).where(or_(Sources.c.name == 'Fake 1', Sources.c.name == 'Fake 2')).order_by(Sources.c.ra)
results = engine.execute(select_st)
for res in results:
    print(res)

