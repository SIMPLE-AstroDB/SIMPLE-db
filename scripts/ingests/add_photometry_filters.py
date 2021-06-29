# Script to add new references to the database
# https://github.com/SIMPLE-AstroDB/SIMPLE-db/issues/144
import requests
import os
from simple.schema import *
from io import BytesIO
from astropy.io.votable import parse
from astrodbkit2.astrodb import Database, create_database
from sqlalchemy.sql.expression import bindparam
from pathlib import Path


SAVE_DB = True
RECREATE_DB = True
REFERENCE_TABLES = ['Publications', 'Telescopes', 'Instruments', 'Modes', 'PhotometryFilters']


def fetch_svo(telescope, instrument, filter_name):
    url = f'http://svo2.cab.inta-csic.es/svo/theory/fps3/fps.php?ID={telescope}/{instrument}.{filter_name}'
    r = requests.get(url)

    if r.status_code != 200:
        print(f'Error retrieving {url}')
        return

    # Parse VOTable contents
    content = BytesIO(r.content)
    votable = parse(content)

    # Get effective wavelength and FWHM
    eff_wave = votable.get_field_by_id('WavelengthEff').value
    fwhm = votable.get_field_by_id('FWHM').value

    return eff_wave, fwhm


filters_to_add = [{'telescope': 'WISE', 'instrument': 'WISE', 'filter': 'W1'},
                  {'telescope': 'WISE', 'instrument': 'WISE', 'filter': 'W2'},
                  {'telescope': 'WISE', 'instrument': 'WISE', 'filter': 'W3'},
                  {'telescope': 'WISE', 'instrument': 'WISE', 'filter': 'W4'},
                  ]

# Fill out information for the specified filters
for datum in filters_to_add:
    datum['band'] = f"{datum['instrument']}.{datum['filter']}"
    eff_wave, fwhm = fetch_svo(datum['telescope'], datum['instrument'], datum['filter'])
    datum['effective_wavelength'] = round(eff_wave, 1)
    datum['width'] = round(fwhm, 1)
    print(datum)

# ---------------------------------------------------------------------------------
# Establish connection to database
db_file = 'SIMPLE.db'
db_file_path = Path(db_file)
db_connection_string = 'sqlite:///SIMPLE.db'

# Remove existing database if it exists and we are recreating it
if RECREATE_DB and db_file_path.exists():
    os.remove(db_file)

# Connect to the database; if creating it, also populate it
# Note that we need to specify this is a new reference table (until AstrodbKit2 gets updated for it)
if not db_file_path.exists():
    create_database(db_connection_string)
    db = Database(db_connection_string, reference_tables=REFERENCE_TABLES)
    db.load_database('data/')
else:
    db = Database(db_connection_string, reference_tables=REFERENCE_TABLES)

# ---------------------------------------------------------------------------------
# Add telescope/instrument information first

# Fetch existing telescopes, add if missing
telescopes = list(set([s['telescope'] for s in filters_to_add]))
existing = db.query(db.Telescopes).filter(db.Telescopes.c.name.in_(telescopes)).table()
if len(existing) > 0:
    existing = existing['name'].tolist()
new_telescopes = list(set(telescopes)-set(existing))
insert_data = [{'name': s} for s in new_telescopes]
if len(insert_data) > 0:
    db.Telescope.insert().execute(insert_data)

# Fetch existing instruments, add if missing
instruments = list(set([s['instrument'] for s in filters_to_add]))
existing = db.query(db.Instruments).filter(db.Instruments.c.name.in_(instruments)).table()
if len(existing) > 0:
    existing = existing['name'].tolist()
new_instruments = list(set(instruments)-set(existing))
insert_data = [{'name': s} for s in new_instruments]
if len(insert_data) > 0:
    db.Instruments.insert().execute(insert_data)

# ---------------------------------------------------------------------------------
# Add to the database

# Get entries to be added and/or updated
full_bands = [s['band'] for s in filters_to_add]
existing_bands = db.query(db.PhotometryFilters).filter(db.PhotometryFilters.c.band.in_(full_bands)).table()
if len(existing_bands) > 0:
    existing_bands = existing_bands['band'].tolist()
new_bands = list(set(full_bands)-set(existing_bands))

# Data to be inserted
insert_data = list(filter(lambda d: d['band'] in new_bands, filters_to_add))
if len(insert_data) > 0:
    print(f'Entries to insert: {insert_data}')
    db.PhotometryFilters.insert().execute(insert_data)

# Data to be updated
update_data = list(filter(lambda d: d['band'] in existing_bands, filters_to_add))
if len(update_data) > 0:
    print(f'Entries to update: {update_data}')
    # Use bindparam to handle multiple UPDATE commands
    # had to rename band since bindparam complains about it being the same as the column name
    update_data = [{'_band': s['band'], **s} for s in update_data]
    stmt = db.PhotometryFilters.update().where(db.PhotometryFilters.c.band == bindparam('_band')).\
        values(telescope=bindparam('telescope'),
               instrument=bindparam('instrument'),
               effective_wavelength=bindparam('effective_wavelength'),
               width=bindparam('width')
               )
    db.engine.execute(stmt, update_data)

# Save output
if SAVE_DB:
    db.save_database(directory='data/')
