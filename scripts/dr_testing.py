# Using schema.py and db.py to handle the basic db connection and the database schema

from simple.core import load_connection

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


# Add references
from sqlalchemy import Table
Sources = Table('sources', base.metadata, autoload=True)
Publications = Table('publications', base.metadata, autoload=True)

publications_data = [{'shortname': 'Penguin 101'},
                     {'shortname': 'Fake 201'}]
Publications.insert().execute(publications_data)

# Add sources
sources_data = [{'ra': 12, 'dec': 23, 'designation': 'Fake 1', 'reference': 'Penguin 101'},
                {'ra': 212, 'dec': -23, 'designation': 'Fake 2', 'reference': 'Fake 201'}
                ]
Sources.insert().execute(sources_data)

# Add source without valid reference- should through an foreign key error
# empty reference should be fine unless I set the column as nullable=False
sources_data = [{'ra': 32, 'dec': 54, 'designation': 'Fake 3', 'reference': 'Unknown'}]
Sources.insert().execute(sources_data)

# Query examples
results = session.query(Sources).all()
print(results)

results = session.query(Sources).filter(Sources.c.dec > 0).all()
print(results)

results = session.query(Sources.c.designation).order_by(Sources.c.dec).all()
print(results)

results = session.query(Sources.c.designation, Sources.c.reference, Publications.c.shortname)\
    .join(Publications, Sources.c.reference == Publications.c.shortname).all()
print(results)

results = engine.execute('SELECT * FROM sources').fetchall()
print(results)



