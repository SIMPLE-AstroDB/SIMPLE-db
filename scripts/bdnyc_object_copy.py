# Script to copy over BDNYC database sources into SIMPLE

from astrodbkit2.astrodb import Database, and_
from sqlalchemy import types  # for BDNYC column overrides
from collections import Counter

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

# ----------------------------------------------------------------------------------------
# Add Sources that are not already in SIMPLE
temp = db.query(db.Publications.c.name).all()
publications_simple = [s[0] for s in temp]
temp = db.query(db.Sources.c.source).all()
existing_simple = [s[0] for s in temp]

temp = bdnyc.query(bdnyc.sources.c.designation,
                   bdnyc.sources.c.names,
                   bdnyc.sources.c.ra,
                   bdnyc.sources.c.dec,
                   bdnyc.sources.c.shortname,
                   bdnyc.sources.c.publication_shortname, )\
    .filter(and_(bdnyc.sources.c.names.notin_(existing_simple),
                 bdnyc.sources.c.designation.notin_(existing_simple),
                 bdnyc.sources.c.publication_shortname.in_(publications_simple),
                 bdnyc.sources.c.version <= 2,
                 bdnyc.sources.c.designation.isnot(None),
                 bdnyc.sources.c.ra.isnot(None),
                 bdnyc.sources.c.dec.isnot(None),
                 bdnyc.sources.c.publication_shortname.isnot(None)))\
    .all()

# Restructure for input into SIMPLE
new_db_mapping = {'designation': 'source', 'name': 'shortname', 'publication_shortname': 'reference'}
data = [{new_db_mapping.get(k, k): x.__getattribute__(k)
         for k in x.keys() if k not in ('id', 'names')
         }
        for x in temp]

# Eliminate duplicated rows (SIMPLE requires that 'source' be unique)
counter = Counter([s['source'].strip() for s in data])
counter = {k:v for k,v in counter.items() if v > 1}  # select duplicates
data = [d for d in data if d['source'].strip() not in counter.keys()]

db.Sources.insert().execute(data)
db.query(db.Sources).count()

# ----------------------------------------------------------------------------------------
# Names table
# TODO: This needs some further work
name_data = []
for row in temp:
    if row.designation not in [s['source'] for s in data]: continue
    name_list = set([row.designation] + [s.strip() for s in row.names.split(',')])
    for n in name_list:
        name_data.append({'source': row.designation, 'other_name': n})

db.Names.insert().execute(name_data)
db.query(db.Names).count()
