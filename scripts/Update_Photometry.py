from scripts.ingests.utils import *
from astropy.table import Table
from sqlalchemy import and_

SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files

logger.setLevel(logging.INFO)

db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)

# Change MKO Knap-4 names
db.Photometry.update(). \
    where(and_(db.Photometry.c.band == 'MKO.H', db.Photometry.c.reference == 'Knap04')). \
    values(band='UKIRT/UFTI.H').execute()

db.Photometry.update(). \
    where(and_(db.Photometry.c.band == 'MKO.J', db.Photometry.c.reference == 'Knap04')). \
    values(band='UKIRT/UFTI.J').execute()

db.Photometry.update(). \
    where(and_(db.Photometry.c.band == 'MKO.Y', db.Photometry.c.reference == 'Knap04')). \
    values(band='UKIRT/UFTI.Y').execute()

db.Photometry.update(). \
    where(and_(db.Photometry.c.band == 'MKO.K', db.Photometry.c.reference == 'Knap04')). \
    values(band='UKIRT/UFTI.K').execute()
