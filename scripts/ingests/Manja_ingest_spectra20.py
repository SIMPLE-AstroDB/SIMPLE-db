import logging

# from astroquery.simbad import conf
from scripts.ingests.ingest_utils import *
from scripts.ingests.utils import *
# conf.server = 'simbad.harvard.edu'
# Simbad.SIMBAD_URL='http://harvard.simbad.edu/simbad/sim-script'
# Simbad.URL = 'harvard.simbad.edu'
SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files

logger.setLevel(logging.DEBUG)

db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)

# Read in CSV file as Astropy table
# file = 'Manja19_spectra9.csv'
file = 'Manja20_spectra - Sheet1.csv'
data = Table.read('scripts/ingests/' + file)

ingest_publication(db, bibcode='2010A&A...510A..27B ')
ingest_publication(db, bibcode='1997MNRAS.287..180P')
ingest_publication(db, bibcode='2012MNRAS.426.3419B')
ingest_publication(db, bibcode='1998A&A...336..490B')
db.Publications.delete().where(db.Publications.c.publication == 'Mart98c').execute()
ingest_publication(db, bibcode='1998ApJ...509L.113M', publication='Mart98.775')
ingest_publication(db, bibcode='1996ApJ...469L..53R')
ingest_sources(db, data['Source'], data['discovery reference'])


# Add the spectra to the database
# ingest_spectra(db, data['Source'], data['Spectrum'], 'em.IR.NIR', 'ESO VLT', 'XShooter', 'Echelle',
#                data['observation_date'],
#                'Manj20', 'nm', 'erg/cm^2^/s/micron', comments=data['spectrum comments'],
#                other_references=data['Other spectrum refs'])

# WRITE THE JSON FILES
if SAVE_DB:
    db.save_database(directory='data/')
