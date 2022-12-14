from scripts.ingests.utils import *
from astropy.table import Table
from sqlalchemy import and_

SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files

logger.setLevel(logging.INFO)

db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)

# Add New Filters Themselves

# UKIRT/UFTI
mko_ufti = {f'MKO.{band}': f'UKIRT/UFTI.{band}' for band in ('Y', 'J', 'H', 'K')}
for ref in ('Goli04a', 'Legg02a', 'Knap04', 'Stra99', 'Geba01', 'Chiu06', 'Naud14'):
    for mkoband, uftiband in mko_ufti.items():
        db.Photometry.update().where(and_(db.Photometry.c.band == mkoband, db.Photometry.c.reference == ref)).values(
            band=uftiband).execute()

# UKIRT/UKIDSS
mko_uki = {f'MKO.{band}': f'UKIRT/UKIDSS.{band}' for band in ('Y', 'J', 'H', 'K')}
for ref in ('Burn10', 'Legg10'):
    for mkoband, ukidband in mko_uki.items():
        db.Photometry.update().where(and_(db.Photometry.c.band == mkoband, db.Photometry.c.reference == ref)).values(
            band=ukidband).execute()

# UKIRT/WFCAM
mko_wfc = {f'MKO.{band}': f'UKIRT/WFCAM.{band}' for band in ('Y', 'J', 'H', 'K')}
for ref in ('Hewe06', 'Best20'):
    for mkoband, wfcband in mko_wfc.items():
        db.Photometry.update().where(and_(db.Photometry.c.band == mkoband, db.Photometry.c.reference == ref)).values(
            band=wfcband).execute()

# Paranal/NACO
for ref in ('Curr13a', 'Chau04'):
    db.Photometry.update().where(and_(db.Photometry.c.band == "MKO.L'", db.Photometry.c.reference == ref)).values(
        band='Paranal/NACO.Lp').execute()

# MKO/NSFCam
mko_nsf = {f'MKO.{band}': f'MKO/NSFCam.{band}' for band in ('J', 'H', 'K')}
for ref in ('Legg98', 'Legg01'):
    for mkoband, nsfband in mko_nsf.items():
        db.Photometry.update().where(and_(db.Photometry.c.band == mkoband, db.Photometry.c.reference == ref)).values(
            band=nsfband).execute()

# Lp NSFCam
for ref in ('Geba01', 'Legg98', 'Legg01'):
    db.Photometry.update().where(and_(db.Photometry.c.band == "MKO.L'", db.Photometry.c.reference == ref)).values(
        band='MKO/NSFCam.Lp').execute()

# Reid02b not sure
# Goli04a
# Jone96
# Legg02a
# Legg07b
# Legg00
# Luhm07a
# Legg01
