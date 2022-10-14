from scripts.ingests.utils import *
from astropy.table import Table

SAVE_DB = True  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files

logger.setLevel(logging.INFO)

db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)

# Cousins.I not in PhotometryFilters
# Cousins.R not in PhotometryFilters
# Johnson.V not in PhotometryFilters

# TODO in data files
# TODO: change MKO to UFTI for Leggett and Knapp data
# TODO: change Cousins to SMARTS for Diet14 data
# TODO: change some SDSS filters to use Sloan as the telscope
# TODO: What is MKO.Y?

# TODO in PhotometryFilters
# TODO: Add references

# Change IRAC band names
db.Photometry.update().\
    where(db.Photometry.c.band == 'IRAC.ch1').\
    values(band='IRAC.I1').execute()

db.Photometry.update().\
    where(db.Photometry.c.band == 'IRAC.ch2').\
    values(band='IRAC.I2').execute()

db.Photometry.update().\
    where(db.Photometry.c.band == 'IRAC.ch3').\
    values(band='IRAC.I3').execute()

db.Photometry.update().\
    where(db.Photometry.c.band == 'IRAC.ch4').\
    values(band='IRAC.I4').execute()


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

instruments = ['DENIS', 'SOI', 'SMARTS', 'UFTI','NICMOS1', 'GALEX']
telescopes = ['Sloan', 'DENIS', 'Soar', 'CTIO']

# ingest new Instruments
for instrument in instruments:
    new_instrument = [{'name': instrument,
                       'reference': None}]
    db.Instruments.insert().execute(new_instrument)

# ingest new Telescopes
for telescope in telescopes:
    new_telescope = [{'name': telescope, 'reference': None}]
    db.Telescopes.insert().execute(new_telescope)

# ingest new filters
for filter in filters:
    new_filter = [{'band': filter['band'],
                   'effective_wavelength': filter['weff'],
                   'instrument': filter['instrument'],
                   'telescope': filter['telescope']
                   }]

    db.PhotometryFilters.insert().execute(new_filter)

db.Photometry.update().\
    where(db.Photometry.c.band == 'HST.F110W').\
    values(band='NICMOS1.F110').execute()

db.Photometry.update().\
    where(db.Photometry.c.band == 'HST.F090M').\
    values(band='NICMOS1.F090M').execute()