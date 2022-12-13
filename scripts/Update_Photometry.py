from scripts.ingests.utils import *
from astropy.table import Table
from sqlalchemy import and_

SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files

logger.setLevel(logging.INFO)

db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)

# Add New Filters Themselves
# Todo : Ukidss and ufti y band

# UKIRT/UFTI
mko_ufti = {f'MKO.{band}': f'UKIRT/UFTI.{band}' for band in ('Y', 'J', 'H', 'K')}
for ref in ('Goli04a', 'Legg02a', 'Knap04', 'Stra99'):
    for mkoband, ukirtband in mko_ufti.items():
        db.Photometry.update().where(and_(db.Photometry.c.band == mkoband, db.Photometry.c.reference == ref)).values(band=ukirtband).execute()

# UKIRT/UKIDSS
mko_uki = {f'MKO.{band}': f'UKIRT/UKIDSS.{band}' for band in ('Y', 'J', 'H', 'K')}
for ref in ('Burn10', 'Best20b', 'Legg10'):
    for mkoband, ukirtband in mko_uki.items():
        db.Photometry.update().where(and_(db.Photometry.c.band == mkoband, db.Photometry.c.reference == ref)).values(band=ukirtband).execute()

# UKIRT/WFCAM
mko_wfc = {f'MKO.{band}': f'UKIRT/WFCAM.{band}' for band in ('Y', 'J', 'H', 'K')}
for ref in ('Best20a', 'Best20b', 'Hewe06'):
    for mkoband, ukirtband in mko_wfc.items():
        db.Photometry.update().where(and_(db.Photometry.c.band == mkoband, db.Photometry.c.reference == ref)).values(band=ukirtband).execute()


# Curr13a
# Legg10
# Legg98
# Legg01
# Geba01
# Chiu06
# Reid02b
# Naud14
# Chau04
