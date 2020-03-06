# Using schema.py and db.py to handle the basic db connection and the database schema

from simple.core import load_connection
from sqlalchemy import or_, and_

# Establish connection to database
# connection_string = 'postgresql://localhost/SIMPLE'
connection_string = 'postgresql+psycopg2://localhost/SIMPLE'  # postgres with psycopg2
# connection_string = 'sqlite://'  # in memory sqlite
connection_string = 'sqlite:///SIMPLE.db'

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

