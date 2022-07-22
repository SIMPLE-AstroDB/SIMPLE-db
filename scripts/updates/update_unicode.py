# Script to address unicode issues from some incorrectly inserted data
import logging
from scripts.ingests.utils import logger, load_simpledb

logger.setLevel(logging.INFO)

db = load_simpledb('SIMPLE.db', recreatedb=True)

# Character to fix:
data = db.search_string('\u2013')

# Update Sources
if 'Sources' in data.keys():
    for row in data['Sources']:
        name = row['source']
        new_name = name.replace(u'\u2013', '-')
        print(f'{name} -> {new_name}')
        db.Sources.update().where(db.Sources.c.source == name).values(source=new_name).execute()

# Update Names
if 'Names' in data.keys():
    for row in data['Names']:
        name = row['other_name']
        new_name = name.replace(u'\u2013', '-')
        print(f'{name} -> {new_name}')
        db.Names.update().where(db.Names.c.other_name == name).values(other_name=new_name).execute()

# Single fix in Spectra
url = 'https://bdnyc.s3.amazonaws.com/XShooter/J11123099\u20137653342.txt'
new_url = url.replace(u'\u2013', '-')
db.Spectra.update().where(db.Spectra.c.spectrum == url).values(spectrum=new_url).execute()

db.save_database('data/')
