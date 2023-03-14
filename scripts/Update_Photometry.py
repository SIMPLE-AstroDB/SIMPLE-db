from scripts.ingests.utils import *
from astropy.table import Table
from sqlalchemy import and_

SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files

logger.setLevel(logging.INFO)

db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)

# Add New Filters Themselves
# Take out the old filters

# Fix the Reid02.2806 Publication in the Photometry table
db.Photometry.update().where(db.Photometry.c.reference == 'Reid02.2806').values(reference='Reid02.466').execute()

# UKIRT/UFTI
mko_ufti = {f'MKO.{band}': f'UKIRT/UFTI.{band}' for band in ('Y', 'J', 'H', 'K')}
for ref in ('Goli04.3516', 'Knap04', 'Stra99', 'Geba01', 'Chiu06', 'Naud14', 'Legg00'):
    for mkoband, uftiband in mko_ufti.items():
        db.Photometry.update().where(and_(db.Photometry.c.band == mkoband, db.Photometry.c.reference == ref)).values(
            band=uftiband).execute()

# UKIRT/UKIDSS
mko_uki = {f'MKO.{band}': f'UKIRT/UKIDSS.{band}' for band in ('Y', 'J', 'H', 'K')}
for ref in ('Burn10.1952', 'Legg10'):
    for mkoband, ukidband in mko_uki.items():
        db.Photometry.update().where(and_(db.Photometry.c.band == mkoband, db.Photometry.c.reference == ref)).values(
            band=ukidband).execute()

# UKIRT/WFCAM
mko_wfc = {f'MKO.{band}': f'UKIRT/WFCAM.{band}' for band in ('Y', 'J', 'H', 'K')}
for mkoband, wfcband in mko_wfc.items():
    db.Photometry.update().where(and_(db.Photometry.c.band == mkoband, db.Photometry.c.reference == 'Hewe06')).values(
        band=wfcband).execute()

# Paranal/NACO
for ref in ('Curr13.15', 'Chau04'):
    db.Photometry.update().where(and_(db.Photometry.c.band == "MKO.L'", db.Photometry.c.reference == ref)).values(
        band='Paranal/NACO.Lp').execute()

# MKO/NSFCam
mko_nsf = {f'MKO.{band}': f'MKO/NSFCam.{band}' for band in ("J", "H", "K", "L'")}
for ref in ('Legg98', 'Legg01', 'Legg02.452','Geba01', 'Reid02.466','Goli04.3516', 'Jone96', 'Legg07.1079','Luhm07.570'):
    for mkoband, nsfband in mko_nsf.items():
        db.Photometry.update().where(and_(db.Photometry.c.band == mkoband, db.Photometry.c.reference == ref)).values(
            band=nsfband).execute()

# WRITE THE JSON FILES
if SAVE_DB:
    db.save_database(directory='data/')

