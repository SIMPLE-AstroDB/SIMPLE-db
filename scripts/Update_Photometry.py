from scripts.ingests.utils import *
from astropy.table import Table
from sqlalchemy import and_

SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files

logger.setLevel(logging.INFO)

db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)

# Knap04
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

# Update Filter name itself

db.PhotometryFilters.update().where(db.PhotometryFilters.c.band == 'MKO.H').values(band='UKIRT/UFTI.H')
# Legg02a
db.Photometry.update(). \
    where(and_(db.Photometry.c.band == 'MKO.H', db.Photometry.c.reference == 'Legg02a')). \
    values(band='UKIRT/UFTI.H').execute()

db.Photometry.update(). \
    where(and_(db.Photometry.c.band == 'MKO.J', db.Photometry.c.reference == 'Legg02a')). \
    values(band='UKIRT/UFTI.J').execute()

db.Photometry.update(). \
    where(and_(db.Photometry.c.band == 'MKO.Y', db.Photometry.c.reference == 'Legg02a')). \
    values(band='UKIRT/UFTI.Y').execute()

db.Photometry.update(). \
    where(and_(db.Photometry.c.band == 'MKO.K', db.Photometry.c.reference == 'Legg02a')). \
    values(band='UKIRT/UFTI.K').execute()

# Goli04a
db.Photometry.update(). \
    where(and_(db.Photometry.c.band == 'MKO.H', db.Photometry.c.reference == 'Goli04a')). \
    values(band='UKIRT/UFTI.H').execute()

db.Photometry.update(). \
    where(and_(db.Photometry.c.band == 'MKO.J', db.Photometry.c.reference == 'Goli04a')). \
    values(band='UKIRT/UFTI.J').execute()

db.Photometry.update(). \
    where(and_(db.Photometry.c.band == 'MKO.Y', db.Photometry.c.reference == 'Goli04a')). \
    values(band='UKIRT/UFTI.Y').execute()

db.Photometry.update(). \
    where(and_(db.Photometry.c.band == 'MKO.K', db.Photometry.c.reference == 'Goli04a')). \
    values(band='UKIRT/UFTI.K').execute()

# Hewe06  on wfcam

db.Photometry.update(). \
    where(and_(db.Photometry.c.band == 'MKO.H', db.Photometry.c.reference == 'Hewe06')). \
    values(band='UKIRT/WFCAM.H').execute()

db.Photometry.update(). \
    where(and_(db.Photometry.c.band == 'MKO.J', db.Photometry.c.reference == 'Hewe06')). \
    values(band='UKIRT/WFCAM.J').execute()

db.Photometry.update(). \
    where(and_(db.Photometry.c.band == 'MKO.Y', db.Photometry.c.reference == 'Hewe06')). \
    values(band='UKIRT/WFCAM.Y').execute()

db.Photometry.update(). \
    where(and_(db.Photometry.c.band == 'MKO.K', db.Photometry.c.reference == 'Hewe06')). \
    values(band='UKIRT/WFCAM.K').execute()

# Burn10 is ukidds

db.Photometry.update(). \
    where(and_(db.Photometry.c.band == 'MKO.H', db.Photometry.c.reference == 'Burn10')). \
    values(band='UKIRT/UKIDSS.H').execute()

db.Photometry.update(). \
    where(and_(db.Photometry.c.band == 'MKO.J', db.Photometry.c.reference == 'Burn10')). \
    values(band='UKIRT/UKIDSS.J').execute()

db.Photometry.update(). \
    where(and_(db.Photometry.c.band == 'MKO.Y', db.Photometry.c.reference == 'Burn10')). \
    values(band='UKIRT/UKIDSS.Y').execute()

db.Photometry.update(). \
    where(and_(db.Photometry.c.band == 'MKO.K', db.Photometry.c.reference == 'Burn10')). \
    values(band='UKIRT/UKIDSS.K').execute()


# Best 20a and Best 20b
mkod = {f'MKO.{band}': f'UKIRT/WFCAM.{band}' for band in ('Y', 'J', 'H', 'K')}
for ref in ('Best20a', 'Best20b'):
    for mkoband, ukirtband in mkod.items():
        db.Photometry.update().where(and_(db.Photometry.c.band == mkoband, db.Photometry.c.reference == ref)).values(band=ukirtband).execute()


# Stra99
# Curr13a
# Legg10
# Legg98
# Legg01
# Geba01
# Chiu06
# Reid02b
# Naud14
# Chau04
