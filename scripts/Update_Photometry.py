from scripts.ingests.utils import *
from sqlalchemy import and_

SAVE_DB = True  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files

logger.setLevel(logging.INFO)

db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)

# Fix the Reid02.2806 Publication in the Photometry table and take out old MKO photometry filters
if RECREATE_DB:
    with db.engine.connect() as conn:
        conn.execute(
            db.Photometry.update().where(db.Photometry.c.reference == 'Reid02.2806').values(reference='Reid02.466'))
        new_instrument = [{'name': 'NACO', 'reference': None}]
        new_telescope = [{'name': 'ESO VLT U4', 'reference': None}]
        new_filter = [{'band': 'NACO.Lp',
                       'effective_wavelength': 37701.21,
                       'instrument': 'NACO',
                       'telescope': 'ESO VLT U4'}]
        new_filter_u = [{'band': 'UFTI.Y',
                       'effective_wavelength': 10170.36	,
                       'instrument': 'UFTI',
                       'telescope': 'UKIRT'}]
        mko_old = {f'MKO.{band}' for band in ("Y", "J", "H", "K", "L'")}
        for mkoband in mko_old:
            conn.execute(db.PhotometryFilters.delete().where(db.PhotometryFilters.c.band == mkoband))
        conn.execute(db.Instruments.insert().values(new_instrument))
        conn.execute(db.Telescopes.insert().values(new_telescope))
        conn.execute(db.PhotometryFilters.insert().values(new_filter))
        conn.execute(db.PhotometryFilters.insert().values(new_filter_u))
        conn.execute(db.PhotometryFilters.delete().where(db.PhotometryFilters.c.band == 'MKO.Y'))
        conn.execute(db.PhotometryFilters.delete().where(db.PhotometryFilters.c.band == 'MKO.Y'))
        conn.commit()

# UKIRT/UFTI
mko_ufti = {f'MKO.{band}' for band in ("Y", "J", "H", "K", "L'")}
mko_ufti_u = {f'UFTI.{band}' for band in ("Y", "J", "H", "K", "Lp")}
for ref in (
        'Goli04.3516', 'Knap04', 'Stra99', 'Geba01', 'Chiu06', 'Naud14', 'Legg00', 'Legg98', 'Legg01', 'Legg02.452',
        'Geba01',
        'Reid02.466', 'Goli04.3516', 'Jone96', 'Legg07.1079', 'Luhm07.570'):
    for mkoband, uftiband in zip(mko_ufti, mko_ufti_u):
        with db.engine.begin() as conn:
            conn.execute(db.Photometry.update().where(
                and_(db.Photometry.c.band == mkoband, db.Photometry.c.reference == ref)).values(
                band=uftiband))

# UKIRT/UKIDSS
mko_uki = {f'MKO.{band}' for band in ("Y", "J", "H", "K", "L'")}
mko_uki_u = {f'UKIDSS.{band}' for band in ('Y', 'J', 'H', 'K', 'Lp')}
for ref in ('Burn10.1952', 'Legg10'):
    for mkoband, ukidband in zip(mko_uki, mko_uki_u):
        with db.engine.begin() as conn:
            conn.execute(db.Photometry.update().where(
                and_(db.Photometry.c.band == mkoband, db.Photometry.c.reference == ref)).values(
                band=ukidband))

# UKIRT/WFCAM
mko_wfc = {f'MKO.{band}': f'WFCAM.{band}' for band in ('Y', 'J', 'H', 'K')}
for mkoband, wfcband in mko_wfc.items():
    with db.engine.begin() as conn:
        conn.execute(db.Photometry.update().where(
            and_(db.Photometry.c.band == mkoband, db.Photometry.c.reference == 'Hewe06')).values(
            band=wfcband))

# Paranal/NACO
for ref in ('Curr13.15', 'Chau04'):
    with db.engine.begin() as conn:
        conn.execute(db.Photometry.update().where(
            and_(db.Photometry.c.band == "MKO.L'", db.Photometry.c.reference == ref)).values(
            band='NACO.Lp'))

# WRITE THE JSON FILES
if SAVE_DB:
    db.save_database(directory='data/')
