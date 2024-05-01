# Script to show how to update the version number

from astrodb_utils import load_astrodb

from simple.schema import REFERENCE_TABLES, Versions

SAVE_DB = True  # save the data files in addition to modifying the .db file

db = load_astrodb("SIMPLE.sqlite", recreatedb=True, reference_tables=REFERENCE_TABLES)

# Check all versions
print(db.query(db.Versions).table())

# Add new version, add new entries as appropriate
# Note that start_date and end_date are strings of the date in format YYYY-MM-DD
obj = Versions(version="2024.4", 
               start_date="2024-04-17", 
               end_date="2024-04-19", 
               description="Renaming Spectra.spectrum to Spectra.access_url")
with db.session as session:
    session.add(obj)
    session.commit()

# Fetch data of latest release
latest_date = db.query(db.Versions.c.end_date).order_by(db.Versions.c.end_date.desc()).limit(1).table()
latest_date = latest_date['end_date'][0]

latest = db.query(db.Versions).filter(db.Versions.c.version == 'latest').count()
if latest == 1:
    with db.engine.connect() as conn:
        conn.execute(db.Versions.delete().where(db.Versions.c.version == 'latest'))
        conn.commit()

# Add latest
obj = Versions(version="latest", 
               start_date=latest_date, 
               description="Version in development")
with db.session as session:
    session.add(obj)
    session.commit()

print(db.query(db.Versions).table())

# Save the database
if SAVE_DB:
    db.save_database(directory='data/')
