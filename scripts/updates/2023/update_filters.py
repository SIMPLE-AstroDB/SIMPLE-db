from scripts.ingests.utils import *
from astropy.table import Table

SAVE_DB = True  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files

logger.setLevel(logging.INFO)

db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)
conn = db.engine.connect()

# TODO in data files
# TODO: change MKO to UFTI for Leggett and Knapp data
# TODO: change Johnson.V and Cousins.I and R to SMARTS for Diet14 data

# add UCDs

# TODO in PhotometryFilters
# TODO: Add references

# Change IRAC band names
conn.execute(db.Photometry.\
    update().\
    where(db.Photometry.c.band == 'IRAC.ch1').\
    values(band='IRAC.I1'))

conn.execute(db.Photometry.\
    update().\
    where(db.Photometry.c.band == 'IRAC.ch2').\
    values(band='IRAC.I2'))

conn.execute(db.Photometry.\
    update().\
    where(db.Photometry.c.band == 'IRAC.ch3').\
    values(band='IRAC.I3'))

conn.execute(db.Photometry.\
    update().\
    where(db.Photometry.c.band == 'IRAC.ch4').\
    values(band='IRAC.I4'))


# table with columns named band, weff, instrument, telescope
filters = Table(names=('band', 'weff', 'instrument', 'telescope'),
                dtype=('S2', 'f8', 'S2', 'S2'))
# filters.add_row(('NSFCam.Lp', 37301.73, 'NSFCam', 'MKO'))
# filters.add_row(('NSFCam.J', 12417.40, 'NSFCam', 'MKO'))
# filters.add_row(('NSFCam.H', 16141.06, 'NSFCam', 'MKO'))
# filters.add_row(('NSFCam.K', 21840.44, 'NSFCam', 'MKO'))
# filters.add_row(('NSFCam.Ks', 21307.65, 'NSFCam', 'MKO'))
# filters.add_row(('NSFCam.Mp', 46803.76, 'NSFCam', 'MKO'))
# filters.add_row(('NSFCam.M', 48257.57, 'NSFCam', 'MKO'))

filters.add_row(('SDSS.g', 4671.78, 'SDSS', 'Sloan'))
filters.add_row(('SDSS.r', 6141.12, 'SDSS', 'Sloan'))
filters.add_row(('SDSS.u', 3608.04, 'SDSS', 'Sloan'))

filters.add_row(('DENIS.I', 7862.10, 'DENIS', 'DENIS'))
filters.add_row(('DENIS.J', 12210.60, 'DENIS', 'DENIS'))
filters.add_row(('DENIS.Ks', 21465.01, 'DENIS', 'DENIS'))

filters.add_row(('UFTI.J', 12418.99, 'UFTI', 'UKIRT'))
filters.add_row(('UFTI.H', 16206.18, 'UFTI', 'UKIRT'))
filters.add_row(('UFTI.K', 21874.57, 'UFTI', 'UKIRT'))

filters.add_row(('NICMOS1.F110W', 10826.77, 'NICMOS1', 'HST'))
filters.add_row(('NICMOS1.F090M', 9006.59, 'NICMOS1', 'HST'))

filters.add_row(('GALEX.FUV', 1549.02, 'GALEX', 'GALEX'))
filters.add_row(('GALEX.NUV', 2304.74, 'GALEX', 'GALEX'))
filters.add_row(('MIPS.24mu', 232096.04, 'MIPS', 'Spitzer'))

filters.add_row(('WFCAM.Y', 10305.00, 'WFCAM', 'UKIRT'))
filters.add_row(('WFCAM.J', 12483.00, 'WFCAM', 'UKIRT'))
filters.add_row(('WFCAM.H', 16313.00, 'WFCAM', 'UKIRT'))
filters.add_row(('WFCAM.K', 22010.00, 'WFCAM', 'UKIRT'))

filters.add_row(('UKIDSS.Z', 8817.00, 'UKIDSS', 'UKIRT'))
filters.add_row(('UKIDSS.Y', 10305.00, 'UKIDSS', 'UKIRT'))
filters.add_row(('UKIDSS.J', 12483.00, 'UKIDSS', 'UKIRT'))
filters.add_row(('UKIDSS.H', 16313.00, 'UKIDSS', 'UKIRT'))
filters.add_row(('UKIDSS.K', 22010.00, 'UKIDSS', 'UKIRT'))

filters.add_row(('PS1.g', 4810.88, 'PS1', 'Pan-STARRS'))
filters.add_row(('PS1.r', 6156.36, 'PS1', 'Pan-STARRS'))
filters.add_row(('PS1.i', 7503.68, 'PS1', 'Pan-STARRS'))
filters.add_row(('PS1.z', 8668.56, 'PS1', 'Pan-STARRS'))
filters.add_row(('PS1.y', 9613.45, 'PS1', 'Pan-STARRS'))

filters.add_row(('Johnson.U', 3656.0, 'Unknown', 'Unknown'))
filters.add_row(('Johnson.B', 4353.0, 'Unknown', 'Unknown'))
filters.add_row(('Johnson.V', 5477.0, 'Unknown', 'Unknown'))
filters.add_row(('Cousins.R', 6349.0, 'Unknown', 'Unknown'))
filters.add_row(('Cousins.I', 8797.0, 'Unknown', 'Unknown'))

filters.add_row(("MKO.Y", 10170.58, 'Unknown', 'Unknown'))
filters.add_row(("MKO.J", 12419.0, 'Unknown', 'Unknown'))
filters.add_row(("MKO.H", 16206.0, 'Unknown', 'Unknown'))
filters.add_row(("MKO.K", 21875.0, 'Unknown', 'Unknown'))
filters.add_row(("MKO.L'", 37301.73, 'Unknown', 'Unknown'))


instruments = ['DENIS', 'SOI', 'SMARTS', 'UFTI', 'NICMOS1', 'GALEX',
               'MIPS', 'WFCAM', 'PS1', 'UKIDSS', 'Unknown']

telescopes = ['Sloan', 'DENIS', 'Soar', 'CTIO', 'Pan-STARRS', 'Unknown']

# ingest new Instruments
for instrument in instruments:
    new_instrument = [{'name': instrument,
                       'reference': None}]
    conn.execute(db.Instruments.insert().values(new_instrument))

# ingest new Telescopes
for telescope in telescopes:
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

conn.execute(db.Photometry.\
    update().\
    where(db.Photometry.c.band == 'HST.F110W').\
    values(band='NICMOS1.F110W', ucd='em.opt.J'))

conn.execute(db.Photometry.\
    update().\
    where(db.Photometry.c.band == 'HST.F090M').\
    values(band='NICMOS1.F090M', ucd='em.opt.I'))

conn.execute(db.Photometry.\
    update().\
    where(db.Photometry.c.band == 'MIPS.ch1').\
    values(band='MIPS.24mu', ucd='em.IR.15-30um'))

conn.execute(db.Photometry.\
    update().\
    where(db.Photometry.c.band == 'y').\
    values(band='PS1.y', ucd='em.IR.I'))

conn.execute(db.Photometry.\
    update().\
    where(db.Photometry.c.band == 'Z').\
    values(band='UKIDSS.Z', ucd='em.IR.I'))

conn.execute(db.Photometry.\
    update().\
    where(db.Photometry.c.band == 'MKO.Y').\
    where(db.Photometry.c.reference == 'Lawr07').\
    values(band='UKIDSS.Y', ucd='em.opt.I'))

conn.execute(db.Photometry.\
    update().\
    where(db.Photometry.c.band == 'MKO.J').\
    where(db.Photometry.c.reference == 'Lawr07').\
    values(band='UKIDSS.J', ucd='em.IR.J'))

conn.execute(db.Photometry.\
    update().\
    where(db.Photometry.c.band == 'MKO.H').\
    where(db.Photometry.c.reference == 'Lawr07').\
    values(band='UKIDSS.H', ucd='em.IR.H'))

conn.execute(db.Photometry.\
    update().\
    where(db.Photometry.c.band == 'MKO.K').\
    where(db.Photometry.c.reference == 'Lawr07').\
    values(band='UKIDSS.K', ucd='em.IR.K'))

conn.execute(db.Photometry.\
    update().\
    where(db.Photometry.c.band == 'MKO.K').\
    where(db.Photometry.c.reference == 'Liu_13b').\
    values(band='WFCAM.K', ucd='em.IR.J'))

conn.execute(db.Photometry.\
    update().\
    where(db.Photometry.c.band == 'MKO.J').\
    where(db.Photometry.c.reference == 'Liu_13b').\
    values(band='WFCAM.J', ucd='em.IR.J'))

conn.execute(db.Photometry.\
    update().\
    where(db.Photometry.c.band == 'MKO.H').\
    where(db.Photometry.c.reference == 'Liu_13b').\
    values(band='WFCAM.H', ucd='em.IR.H'))

conn.execute(db.Photometry.\
    update().\
    where(db.Photometry.c.band == 'MKO.Y').\
    where(db.Photometry.c.reference == 'Liu_13b').\
    values(band='WFCAM.Y', ucd='em.opt.I'))

conn.execute(db.Photometry.\
    update().\
    where(db.Photometry.c.band == 'SDSS.g').\
    where(db.Photometry.c.reference == 'Liu_13b').\
    values(band='PS1.g',ucd='em.opt.B'))

conn.execute(db.Photometry.\
    update().\
    where(db.Photometry.c.band == 'SDSS.i').\
    where(db.Photometry.c.reference == 'Liu_13b').\
    values(band='PS1.i', ucd='em.opt.I'))

conn.execute(db.Photometry.\
    update().\
    where(db.Photometry.c.band == 'SDSS.r').\
    where(db.Photometry.c.reference == 'Liu_13b').\
    values(band='PS1.r', ucd='em.opt.R'))

conn.execute(db.Photometry.\
    update().\
    where(db.Photometry.c.band == 'SDSS.z').\
    where(db.Photometry.c.reference == 'Liu_13b').\
    values(band='PS1.z', ucd='em.opt.I'))

conn.execute(db.PhotometryFilters.\
    update().\
    where(db.PhotometryFilters.c.telescope == 'SDSS').\
    values(telescope='Sloan'))

conn.execute(db.Photometry.\
    update().\
    where(db.Photometry.c.telescope == 'SDSS').\
    values(telescope='Sloan'))

conn.execute(db.Telescopes.delete().where(db.Telescopes.c.name == 'SDSS'))

# Actually send all the queued up commands
conn.commit()

# WRITE THE JSON FILES
if SAVE_DB:
    db.save_database(directory='data/')