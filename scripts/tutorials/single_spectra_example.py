# Example adding a single spectrum to the database

from astrodbkit2.astrodb import Database
from datetime import datetime

connection_string = 'sqlite:///SIMPLE.db'  # SQLite
db = Database(connection_string)

# Add missing telescopes, etc
db.Telescopes.insert().execute([{'name': 'IRTF'}])
db.Instruments.insert().execute([{'name': 'SpeX'}])
db.Modes.insert().execute([{'name': 'Prism',
                            'instrument': 'SpeX',
                            'telescope': 'IRTF'}])

# Add actual Spectra
spec_data = [{'source': '2MASS J00192626+4614078',
              'regime': 'infrared',
              'spectrum': 'https://s3.amazonaws.com/bdnyc/SpeX/Prism/U10013_SpeX.fits',
              'telescope': 'IRTF',
              'instrument': 'SpeX',
              'mode': 'Prism',
              'reference': 'Cruz18',
              'wavelength_units': 'um',
              'flux_units': 'erg s-1 cm-2 A-1',
              'observation_date': datetime.fromisoformat('2004-11-08')
              }]
db.Spectra.insert().execute(spec_data)

_ = db.inventory('2MASS J00192626+4614078', pretty_print=True)

# Getting spectrum object
db.query(db.Spectra.c.spectrum).filter(db.Spectra.c.source == '2MASS J00192626+4614078').table(spectra=['spectrum'])
t = db.query(db.Spectra.c.spectrum).filter(db.Spectra.c.source == '2MASS J00192626+4614078').limit(1).spectra()

# Confirm that it is a Spectrum1D object
spec = t[0][0]
print(type(spec))

# Save into database folder
db.save_database('data')

