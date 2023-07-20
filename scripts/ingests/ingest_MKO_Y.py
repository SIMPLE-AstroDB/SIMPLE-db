from scripts.ingests.MKO_utils import *
from astropy.utils.exceptions import AstropyWarning
import pandas as pd

warnings.simplefilter('ignore', category=AstropyWarning)

SAVE_DB = True  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files

UCS = pd.read_csv('scripts/ingests/UltracoolSheet-Main.csv')

db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)

logger.setLevel(logging.INFO)  # Can also set to debug


# Adding UKIDSS names
def add_ukidss_names(db):
    if entry['designation_ukidss'] == 'null':
        pass
    else:
        other_name_data = [{'source': entry['name'], 'other_name': entry['designation_ukidss']}]
        db.Names.insert().execute(other_name_data)

    # if entry['designation_WISE'] == 'null':
    #     pass
    # else:
    #     wise_name = [{'source': entry['name'], 'other_name': entry['designation_WISE']}]
    #     db.Names.insert().execute(wise_name)


# You may need to add filters to the Photometry Filters table
# https://github.com/SIMPLE-AstroDB/SIMPLE-db/blob/main/documentation/PhotometryFilters.md
def add_filters(db):
    lco_telescope = [{'name': 'LCO'}]
    db.Telescopes.insert().execute(lco_telescope)

    wircam_instrument = [{'name': 'Wircam',
                          'mode': 'Imaging',
                          'telescope': 'CFHT'}]
    db.Instruments.insert().execute(wircam_instrument)

    niri_instrument = [{'name': 'NIRI',
                        'mode': 'Imaging',
                        'telescope': 'Gemini North'}]
    db.Instruments.insert().execute(niri_instrument)

    gpi_instrument = [{'name': 'GPI',
                       'mode': 'Imaging',
                       'telescope': 'Gemini South'}]
    db.Instruments.insert().execute(gpi_instrument)

    visao_instrument = [{'name': 'VisAO',
                         'mode': 'Imaging',
                         'telescope': 'LCO'}]
    db.Instruments.insert().execute(visao_instrument)

    wircam_y = [{'band': 'Wircam.Y',
                 'ucd': 'em.IR.NIR',
                 'effective_wavelength': '10220.90',
                 'width': '1084.17'}]

    db.PhotometryFilters.insert().execute(wircam_y)

    niri_y = [{'band': 'NIRI.Y',
               'ucd': 'em.IR.NIR',
               'effective_wavelength': '10211.18',
               'width': '943.58'}]

    db.PhotometryFilters.insert().execute(niri_y)

    gpi_y = [{'band': 'GPI.Y',
              'ucd': 'em.IR.NIR',
              'effective_wavelength': '10375.56',
              'width': '1707.30'}]

    db.PhotometryFilters.insert().execute(gpi_y)

    visao_ys = [{'band': 'VisAO.Ys',
                 'ucd': 'em.IR.NIR',
                 'effective_wavelength': '9793.50',
                 'width': '907.02'}]

    db.PhotometryFilters.insert().execute(visao_ys)


# Execute the ingests!
add_filters(db)
for i in range(len(UCS)):
    entry = UCS.iloc[i]
    if entry['Y_MKO'] == 'NaN' or entry['Yerr_MKO'] == 'NaN':
        pass
    else:
        add_ukidss_names(db)
        photometry_mko(db, entry)

# WRITE THE JSON FILES
if SAVE_DB:
    db.save_database(directory='data/')