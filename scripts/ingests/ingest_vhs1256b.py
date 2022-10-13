from scripts.ingests.ingest_utils import *
from scripts.ingests.utils import *
from astropy.time import Time

SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files

logger.setLevel(logging.INFO)

db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)

source = '2MASS J12560183-1257276'

# Add other name
other_name_data = [{'source': source, 'other_name': 'VHS 1256-1257b'}]
db.Names.insert().execute(other_name_data)

wise_name = [{'source': source, 'other_name': 'WISEA J125601.66-125728.7'}]
db.Names.insert().execute(wise_name)

# Ingesting missing publications
ingest_publication(db, bibcode='2018ApJ...869...18M') # Miles 2018
ingest_publication(db, bibcode='2008SPIE.7014E..6XB', publication='ACAM')
ingest_publication(db, bibcode='2004SPIE.5489..638M', publication='VISTA')
ingest_publication(db, bibcode='2006SPIE.6269E..0XD', publication='VIRCAM')
ingest_publication(db, doi='10.1051/0004-6361/202243940', publication='GaiaDR3',
                   ignore_ads=True,
                   description='Gaia Data Release 3: Summary of the contents and survey properties')

# Spectral type already ingested, updated adopted and uncertanties
stmt = db.SpectralTypes.update()\
    .where(db.SpectralTypes.c.source == source)\
    .where(db.SpectralTypes.c.spectral_type_code == 77.0)\
    .values(adopted=True, spectral_type_error=1.5,
            spectral_type_string='L7 VL-G', regime='nir')
db.engine.execute(stmt)

stmt = db.SpectralTypes.update()\
    .where(db.SpectralTypes.c.source == source)\
    .where(db.SpectralTypes.c.spectral_type_code == 78.0)\
    .values(adopted=False, spectral_type_error=2)
db.engine.execute(stmt)

# TODO: add original spectra

# ingest Gauz15 nir SofI spectrum
sofi_instrument = [{'name': 'SofI',
                    'reference': None}]
db.Instruments.insert().execute(sofi_instrument)
ntt_telescope = [{'name': 'NTT'}]
db.Telescopes.insert().execute(ntt_telescope)
# nir_spectrum_file = '/Users/kelle/Dropbox (Personal)/Mac (3)/Downloads/vhs1256b/vhs1256b_nir_SOFI.fits'
nir_spectrum_file = 'https://bdnyc.s3.amazonaws.com/nir_spectra/vhs1256b_nir_SOFI.fits'
ingest_spectrum_from_fits(db, source, nir_spectrum_file)

# ingest Gauz15 optical OSIRIS spectrum
gtc_telescope = [{'name': 'GTC'}]
db.Telescopes.insert().execute(gtc_telescope)
# optical_spectrum_file = '/Users/kelle/Dropbox (Personal)/Mac (3)/Downloads/vhs1256b/vhs1256b_opt_Osiris.fits'
optical_spectrum_file = 'https://bdnyc.s3.amazonaws.com/optical_spectra/vhs1256b_opt_Osiris.fits'
ingest_spectrum_from_fits(db, source, optical_spectrum_file)

# ingest Miles18 Keck/NIRSPEC spectrum
nirspec_instrument = [{'name': 'NIRSPEC',
                    'reference': None}]
db.Instruments.insert().execute(nirspec_instrument)
# keck_nir_spectrum_file = \
#    '/Users/kelle/Dropbox (Personal)/Mac (3)/Downloads/vhs1256b/vhs1256b_spectra_Figure8_Miles2018.fits'
keck_nir_spectrum_file = 'https://bdnyc.s3.amazonaws.com/nir_spectra/vhs1256b_spectra_Figure8_Miles2018.fits'
ingest_spectrum_from_fits(db, source, keck_nir_spectrum_file)

# ingest SDSS photometry
# add SDSS.i and SDSS.z bands to PhotometryFilters table
# not used here, but will be useful for the future
sdss_instrument = [{'name': 'SDSS'}]
db.Instruments.insert().execute(sdss_instrument)

sdss_i = [{'band': 'SDSS.i',
          'effective_wavelength': '7458',
           'instrument': 'SDSS',
           'telescope': 'SDSS',
           'width': '1103'}]

sdss_z = [{'band': 'SDSS.z',
           'effective_wavelength': '8923',
           'instrument': 'SDSS',
           'telescope': 'SDSS',
           'width': '1164'}]

db.PhotometryFilters.insert().execute(sdss_i)
db.PhotometryFilters.insert().execute(sdss_z)

# ingest WHT/ACAM SDSS i and z photometry
acam_instrument = [{'name': 'ACAM',
                    'reference': 'ACAM'}]
db.Instruments.insert().execute(acam_instrument)

wht_telescope = [{'name': 'WHT'}]
db.Telescopes.insert().execute(wht_telescope)

wht_i = [{'band': 'ACAM.i',
          'effective_wavelength': '7458',
          'instrument': 'ACAM',
          'telescope': 'WHT',
          'width': '1103'}]

wht_z = [{'band': 'ACAM.z',
          'effective_wavelength': '8923',
          'instrument': 'ACAM',
          'telescope': 'WHT',
          'width': '1164'}]

db.PhotometryFilters.insert().execute(wht_i)
db.PhotometryFilters.insert().execute(wht_z)

bands = ['ACAM.i', 'ACAM.z']
magnitudes = [22.494, 20.095]
magnitude_errors = [0.315, 0.090]
sources = [source] * 2

acam_epoch = Time('2014-07-17')
acam_epoch.format = 'decimalyear'

ingest_photometry(db, sources, bands, magnitudes, magnitude_errors, 'Gauz15',
                  telescope='WHT', instrument='ACAM', epoch=acam_epoch.value.astype(np.float32))

# ingest WISE photometry: WISEA J125601.66-125728.7
wise_bands = ['WISE.W1', 'WISE.W2']
wise_mags = [13.6, 12.8]
wise_mag_errors = [0.5, 0.5]

ingest_photometry(db, sources, wise_bands, wise_mags, wise_mag_errors, 'Gauz15',
                  telescope='WISE')

# ingest VISTA photometry
vircam_instrument = [{'name': 'VIRCAM',
                      'reference': 'VIRCAM'}]
db.Instruments.insert().execute(vircam_instrument)

vista_telescope = [{'name': 'VISTA', 'reference': 'VISTA'}]
db.Telescopes.insert().execute(vista_telescope)

vista_y = [{'band': 'VISTA.Y',
            'effective_wavelength': '10196.43',
            'instrument': 'VIRCAM',
            'telescope': 'VISTA',
            'width': '870.63'}]

vista_j = [{'band': 'VISTA.J',
            'effective_wavelength': '12481.00',
            'instrument': 'VIRCAM',
            'telescope': 'VISTA',
            'width': '1542.53'}]

vista_h = [{'band': 'VISTA.H',
            'effective_wavelength': '16348.19',
            'instrument': 'VIRCAM',
            'telescope': 'VISTA',
            'width': '2674.02'}]

vista_k = [{'band': 'VISTA.Ks',
            'effective_wavelength': '21435.46',
            'instrument': 'VIRCAM',
            'telescope': 'VISTA',
            'width': '2793.85'}]

db.PhotometryFilters.insert().execute(vista_y)
db.PhotometryFilters.insert().execute(vista_j)
db.PhotometryFilters.insert().execute(vista_h)
db.PhotometryFilters.insert().execute(vista_k)

sources = [source] * 4
vista_bands = ['VISTA.Y', 'VISTA.J', 'VISTA.H', 'VISTA.Ks']
vista_magnitudes = [18.558, 17.136, 15.777, 14.665]
vista_mag_unc = [0.051, 0.020, 0.015, 0.010]
vista_epoch = Time('2011-07-01')
vista_epoch.format = 'decimalyear'

ingest_photometry(db, sources, vista_bands, vista_magnitudes, vista_mag_unc, 'Gauz15',
                  telescope='VISTA', instrument='VIRCAM', epoch=vista_epoch.value.astype(np.float32))

# Ingest Gaia parallax from primary 2MASS J12560215-1257217, Gaia DR3 3526198184723289472
gaia_data = get_gaiadr3('3526198184723289472', verbose=False)
ingest_parallaxes(db, source, gaia_data['parallax'], gaia_data['parallax_error'], 'GaiaDR3',
                  comments='adopted from primary VHS J1256-1257')

# ingest Gauz15 parallax
ingest_parallaxes(db, source, 78.8, 6.4, 'Gauz15')

# WRITE THE JSON FILES
if SAVE_DB:
    db.save_database(directory='data/')
