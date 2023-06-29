# This script reworks the instruments table

# pylint: disable=all

import sqlalchemy as sa
from scripts.ingests.utils import *

logger.setLevel(logging.INFO)

db = load_simpledb('SIMPLE.db', recreatedb=False)

# Add mode, telescope columns to Instruments; these can't be a primary key yet
# Can also be done in schema.py without setting primary keys
with db.engine.connect() as conn:
    conn.execute(sa.text('ALTER TABLE Instruments ADD COLUMN mode text;'))
    conn.execute(sa.text('ALTER TABLE Instruments ADD COLUMN telescope text;'))
    conn.commit()

# Move information to Instruments table
instruments = db.query(db.Instruments).pandas()
modes = db.query(db.Modes).pandas()
with db.engine.connect() as conn:
    for i, row in modes.iterrows():
        print(f"To update: {row['instrument']} add mode {row['mode']} from telescope {row['telescope']}")
        # Construct update statement; have to use SQLite since the database model is not fully updated
        conn.execute(sa.text(f"UPDATE Instruments SET mode='{row['mode']}', telescope='{row['telescope']}' WHERE instrument='{row['instrument']}'"))
        
    # Run all updates
    conn.commit()

# Commenting this out because for now it's not a primary key
# Because mode is going to be a primary key, can't have it NULL
with db.engine.connect() as conn:
    conn.execute(sa.text("UPDATE Instruments SET mode='Unknown' WHERE mode IS NULL"))
    conn.commit()

# Reflect table changes, if doing ALTER commands
db = load_simpledb('SIMPLE.db', recreatedb=False)
instruments = db.query(db.Instruments).pandas()


# Telescope settings
with db.engine.connect() as conn:
    conn.execute(db.Instruments.update().where(db.Instruments.c.instrument == 'IRAC').values(telescope='Spitzer'))
    conn.execute(db.Instruments.update().where(db.Instruments.c.instrument == 'WISE').values(telescope='WISE'))
    conn.execute(db.Instruments.update().where(db.Instruments.c.instrument == '2MASS').values(telescope='2MASS'))
    conn.execute(db.Instruments.update().where(db.Instruments.c.instrument == 'Gaia').values(telescope='Gaia'))
    conn.execute(db.Instruments.update().where(db.Instruments.c.instrument == 'GALEX').values(telescope='GALEX'))
    conn.execute(db.Instruments.update().where(db.Instruments.c.instrument == 'PS1').values(telescope='Pan-STARRS 1'))
    conn.commit()

# Setting unknown for everything else
with db.engine.connect() as conn:
    conn.execute(db.Instruments.update().where(db.Instruments.c.telescope == None).values(telescope='Unknown'))
    conn.commit()

# Update Spectra values since they MUST have a mode from now on
with db.engine.connect() as conn:
    conn.execute(db.Spectra.update().where(db.Spectra.c.mode == None).values(mode='Unknown'))
    conn.execute(db.Spectra.update().where(db.Spectra.c.instrument == None).values(instrument='Unknown'))
    conn.commit()

# Fix to account for missing modes based on Spectra table
spectra = db.query(db.Spectra.c.telescope, db.Spectra.c.instrument, db.Spectra.c.mode).distinct().pandas()
data_list = []
for i, row in spectra.iterrows():
    check = db.query(db.Instruments).where(sa.and_(db.Instruments.c.instrument == row['instrument'],
                                                   db.Instruments.c.mode == row['mode'])
    ).count()

    if check == 0:
        # Missing entry, adding to Instruments
        datum = {'instrument': row['instrument'],
                 'telescope': row['telescope'],
                 'mode': row['mode']}
        data_list.append(datum)

# Ingest to database
with db.engine.connect() as conn:
    conn.execute(db.Instruments.insert().values(data_list))
    conn.commit()

# Can't drop the Modes table due to existing foreign key constraints. 
# Easier to do so manually (delete Modes.json) and rebuild the database.
db.save_database('data/')

