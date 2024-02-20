# File to fix spectra units in database
from scripts.ingests.utils import load_simpledb
from sqlalchemy import func

# Establish connection to database
db = load_simpledb('SIMPLE.db', recreatedb=True)

# Get list of units
t = db.query(db.Spectra.c.flux_units, 
             func.count(db.Spectra.c.flux_units).label('Counts')).\
    group_by(db.Spectra.c.flux_units).\
    all()
for row in t:
    print(row)

with db.engine.connect() as conn:
    # Fix units
    conn.execute(db.Spectra.update().where(db.Spectra.c.flux_units == 'Wm-2um-1').values(flux_units='W m-2 um-1'))
    conn.execute(db.Spectra.update().where(db.Spectra.c.flux_units == 'ergs-1cm-2A-1').values(flux_units='erg s-1 cm-2 A-1'))
    conn.execute(db.Spectra.update().where(db.Spectra.c.flux_units == 'ergs s-1 cm-2 A-1').values(flux_units='erg s-1 cm-2 A-1'))
    conn.commit()

# Save database
db.save_database('data/')
