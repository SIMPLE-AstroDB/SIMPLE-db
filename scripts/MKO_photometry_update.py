from scripts.ingests.utils import *
from astropy.table import Table
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
        filters = Table(names=('band', 'weff', 'instrument', 'telescope'), dtype=('S2', 'f8', 'S2', 'S2'))
        filters.add_row(('NSFCam.Lp', 37301.73, 'NSFCam', 'MKO'))
        filters.add_row(('NSFCam.J', 12417.40, 'NSFCam', 'MKO'))
        filters.add_row(('NSFCam.H', 16141.06, 'NSFCam', 'MKO'))
        filters.add_row(('NSFCam.K', 21840.44, 'NSFCam', 'MKO'))
        filters.add_row(('NSFCam.Ks', 21307.65, 'NSFCam', 'MKO'))
        filters.add_row(('NSFCam.Mp', 46803.76, 'NSFCam', 'MKO'))
        filters.add_row(('NSFCam.M', 48257.57, 'NSFCam', 'MKO'))
        filters.add_row(('UFTI.Y', 10170.36, 'UFTI', 'UKIRT'))
        filters.add_row(('NACO.Lp', 37701.21, 'NACO', 'ESO VLT U4'))
        new_instruments = ['NSFCam', 'NACO']
        new_telescopes = ['ESO VLT U4', 'MKO']

        # ingest new Instruments
        for instrument in new_instruments:
            new_instrument = [{'name': instrument,
                               'reference': None}]
            conn.execute(db.Instruments.insert().values(new_instrument))

        # ingest new Telescopes
        for telescope in new_telescopes:
            new_telescope = [{'name': telescope, 'reference': None}]
            conn.execute(db.Telescopes.insert().values(new_telescope))

        # ingest new filters
        for filter in filters:
            new_filter = [{'band': filter['band'],
                           'effective_wavelength': filter['weff'],
                           'instrument': filter['instrument'],
                           'telescope': filter['telescope']
                           }]
            conn.execute(db.PhotometryFilters.insert().values(new_filter))
        mko_old = {f'MKO.{band}' for band in ("Y", "J", "H", "K", "L'")}
        for mkoband in mko_old:
            conn.execute(db.PhotometryFilters.delete().where(db.PhotometryFilters.c.band == mkoband))

        conn.commit()

# UKIRT/UFTI
mko_ufti = {f'MKO.{band}': f'UFTI.{band}' for band in ('Y', 'J', 'H', 'K')}
for ref in ('Goli04.3516', 'Knap04', 'Stra99', 'Geba01', 'Chiu06', 'Naud14', 'Legg00','Legg02.78'):
    for mkoband, uftiband in mko_ufti.items():
        with db.engine.begin() as conn:
            conn.execute(db.Photometry.update().where(
                and_(db.Photometry.c.band == mkoband, db.Photometry.c.reference == ref)).values(
                band=uftiband))

# UKIRT/UKIDSS
mko_uki = {f'MKO.{band}': f'UKIDSS.{band}' for band in ('Y', 'J', 'H', 'K')}
for ref in ('Burn10.1952', 'Legg10'):
    for mkoband, ukidband in mko_uki.items():
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

# MKO/NSFCam
mko_nsf = {f'MKO.{band}': f'NSFCam.{band}' for band in ("J", "H", "K")}
for ref in ('Legg98', 'Legg01', 'Legg02.452', 'Geba01', 'Reid02.466', 'Goli04.3516', 'Jone96', 'Legg07.1079', 'Luhm07.570'):
    for mkoband, nsfband in mko_nsf.items():
        with db.engine.begin() as conn:
            conn.execute(db.Photometry.update().where(and_(db.Photometry.c.band == mkoband, db.Photometry.c.reference == ref)).values(
                band=nsfband))
# MKO/Lp
for ref in ('Legg98', 'Legg01', 'Legg10', 'Legg02.452', 'Geba01', 'Reid02.466', 'Goli04.3516', 'Jone96', 'Legg07.1079', 'Luhm07.570'):
    with db.engine.begin() as conn:
        conn.execute(db.Photometry.update().where(
            and_(db.Photometry.c.band == "MKO.L'", db.Photometry.c.reference == ref)).values(
            band='NSFCam.Lp'))
# Paranal/NACO
for ref in ('Curr13.15', 'Chau04'):
    with db.engine.begin() as conn:
        conn.execute(db.Photometry.update().where(
            and_(db.Photometry.c.band == "MKO.L'", db.Photometry.c.reference == ref)).values(
            band='NACO.Lp'))

# WRITE THE JSON FILES
if SAVE_DB:
    db.save_database(directory='data/')
