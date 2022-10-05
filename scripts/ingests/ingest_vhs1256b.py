from scripts.ingests.ingest_utils import *
from scripts.ingests.utils import *

SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files

logger.setLevel(logging.DEBUG)

db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)

source = '2MASS J12560183-1257276'

# Add other name
other_name_data = [{'source': source, 'other_name': 'VHS 1256-1257b'}]
db.Names.insert().execute(other_name_data)

# TODO: add Gaia and WISE designations as other names.

# Ingesting missing publications
ingest_publication(db, bibcode='2018ApJ...869...18M')

# Ingest spectral type
ingest_spectral_types(db,
                      source=source,
                      spectral_type_string = "L7 VL-G",
                      spectral_types=77,
                      spectral_type_error=1.5,
                      references='Gauz15')

# Ingest spectra
#ingest_spectra(db, source, data['spectrum'], 'mir', 'Spitzer', 'IRS', 'SL',
#               data['observation_date'],
#               'Suar22', original_spectra=data['original_spectrum'], wavelength_units='um', flux_units='Jy', comments=data['spectrum comments'])

# ingest Gauz15 spectrum
# ingest Miles18 spectrum
# ingest Miles18 version of Gauz15 data

# TODO: ingest SDSS photometry
#add SDSS.i and SDSS.z bands to PhotometryFilters table
SDSS_i = [{'band': 'SDSS.i',
          'effective_wavelength': '7458',
           'instrument': 'Sloan',
           'telescope': 'Sloan',
           'width': '1103'}]

SDSS_z = [{'band': 'SDSS.z',
           'effective_wavelength': '8923',
           'instrument': 'Sloan',
           'telescope': 'Sloan',
           'width': '1164'}]

# add filters to PhotometryFilters table

bands = ['SDSS.i', 'SDSS.z']
magnitudes = [22.494, 20.095]
magnitude_errors = [0.315, 0.090]

ingest_photometry(db, source, bands, magnitudes, magnitude_errors, 'Gauz15',
                      telescope='WHT', instrument='ACAM', epoch='2014-17-07')

# TODO: ingest WISE photometry: WISEA J125601.66-125728.7
wise_bands = ['WISE.W1', 'WISE.W2']
wise_mags = [13.6, 12.8]
wise_mag_erros = [0.5, 0.5]

# TODO: ingest VISTA photometry
vista_y = [{'band': '',
           'effective_wavelength': '',
           'instrument': '',
           'telescope': '',
           'width': ''}]

vista_j = [{'band': '',
           'effective_wavelength': '',
           'instrument': '',
           'telescope': '',
           'width': ''}]

vista_h = [{'band': '',
           'effective_wavelength': '',
           'instrument': '',
           'telescope': '',
           'width': ''}]

vista_k = [{'band': '',
           'effective_wavelength': '',
           'instrument': '',
           'telescope': '',
           'width': ''}]

vista_bands = []
vista_magnitudes = [18.558, 17.136, 15.777, 14.665]
vista_mag_unc = [0.051, 0.020, 0.015, 0.010]

ingest_photometry(db, source, vista_bands, vista_magnitudes, vista_mag_unc, 'Gauz15',
                      telescope='VISTA', instrument='VIRCAM', epoch='2001-1-07')


# ingest 2MASS photometry 2MASS J12560183-1257276
twomass_bands = ['2MASS.J', '2MASS.H', '2MASS.Ks']
twomass_magnitudes = [16.662, 15.595, 14.568]
twomass_magnitude_errors = [0.287, 0.209, 0.121]
ingest_photometry(db, source, twomass_bands, twomass_magnitudes, twomass_magnitude_errors, 'Gauz15',
                      telescope='2MASS', instrument='2MASS')


# Gaia parallax already ingested
# ingest Gauz15 parallax
ingest_parallaxes(db, source, 78.8, 6.4, 'Gauz15')

# WRITE THE JSON FILES
if SAVE_DB:
    db.save_database(directory='data/')