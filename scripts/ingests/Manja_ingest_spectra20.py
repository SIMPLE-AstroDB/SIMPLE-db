import logging

# from astroquery.simbad import conf
from scripts.ingests.ingest_utils import *
from scripts.ingests.utils import *

# conf.server = 'simbad.harvard.edu'
# Simbad.SIMBAD_URL='http://harvard.simbad.edu/simbad/sim-script'
# Simbad.URL = 'harvard.simbad.edu'
SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = False  # recreates the .db file from the data files

logger.setLevel(logging.DEBUG)

db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)

# Read in CSV file as Astropy table
# file = 'Manja19_spectra9.csv'
file = 'Manja20_spectra - Sheet1.csv'
data = Table.read('scripts/ingests/' + file)
#try to skip the first source

def ingest_pubs():
    ingest_publication(db, bibcode='2010A&A...510A..27B ')
    ingest_publication(db, bibcode='1997MNRAS.287..180P')
    ingest_publication(db, bibcode='2012MNRAS.426.3419B')
    ingest_publication(db, bibcode='1998A&A...336..490B')
    # deleting a copy of the shortname
    db.Publications.delete().where(db.Publications.c.publication == 'Mart98c').execute()
    ingest_publication(db, bibcode='1998ApJ...509L.113M', publication='Mart98.775')
    ingest_publication(db, bibcode='1996ApJ...469L..53R')
    ingest_publication(db, bibcode='2003MNRAS.343.1263N')
    ingest_publication(db, bibcode='2012MNRAS.424.3178B')
    ingest_publication(db, bibcode='2006A&A...458..805B')
    ingest_publication(db, bibcode='2004AJ....127..449M', publication='Mart04.226')
    ingest_publication(db, bibcode='2000A&A...357..219P')
    ingest_publication(db, bibcode='2004A&A...416..555L')
    ingest_publication(db, bibcode='2001AJ....121..974G')
    ingest_publication(db, bibcode='2004A&A...417..583C')


# ingest_pubs()

ingest_sources(db, data['Source'], data['discovery reference'])

# update the name in the names table and in the sources table, the names table may be first and then the sources table
# Add the spectra to the database
ingest_spectra(db, data['Source'], data['Spectrum'], 'em.IR.NIR', 'ESO VLT', 'XShooter', 'Echelle',
               data['observation_date'],
               'Manj20', wavelength_units='nm', flux_units='erg cm-2 s-1 micron-1')

# WRITE THE JSON FILES
if SAVE_DB:
    db.save_database(directory='data/')
