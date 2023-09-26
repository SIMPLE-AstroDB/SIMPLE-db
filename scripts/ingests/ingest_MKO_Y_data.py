from scripts.ingests.ingest_utils import *
from astropy.utils.exceptions import AstropyWarning
import pandas as pd

warnings.simplefilter('ignore', category=AstropyWarning)

SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files

df = pd.read_csv('scripts/ingests/UltracoolSheet-Main.csv')

db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)

logger.setLevel(logging.INFO)  # Can also set to debug


def references(data_ref):
    # changes reference of the data sheet in the appropriate format used in database
    ref = data_ref
    if data_ref == 'Best20b':
        ref = 'Best20.42'
    elif data_ref == 'Burn10b':
        ref = 'Burn10.1885'
    elif data_ref == 'Deac11':
        ref = 'Deac11.6319'
    elif data_ref == 'Deac12b':
        ref = 'Deac12.100'
    elif data_ref == 'Deac14b':
        ref = 'Deac14.119'
    elif data_ref == 'Deac17a':
        ref = 'Deac17.1126'
    elif data_ref == 'Delo08a':
        ref = 'Delo08.961'
    elif data_ref == 'Delo12b':
        ref = 'Delo12'
    elif data_ref == 'Dupu12':
        ref = 'Dupu12.19'
    elif data_ref == 'Dupu15a':
        ref = 'Dupu15.102'
    elif data_ref == 'Garc17a':
        ref = 'Garc17.162'
    elif data_ref == 'Legg00b':
        ref = 'Legg00'
    elif data_ref == 'Legg02a':
        ref = 'Legg02.452'
    elif data_ref == 'Legg02b':
        ref = 'Legg02.78'
    elif data_ref == 'Legg10a':
        ref = 'Legg10'
    elif data_ref == 'Liu_13b':
        ref = 'Liu_13.20'
    elif data_ref == 'Lodi07a':
        ref = 'Lodi07.372'
    elif data_ref == 'Lodi12d':
        ref = 'Lodi12.53'
    elif data_ref == 'Lodi13a':
        ref = 'Lodi13.2474'
    elif data_ref == 'Pinf14a':
        ref = 'Pinf14.1009'
    elif data_ref == 'Warr07b':
        ref = 'Warr07.1400'
    else:
        pass
    return ref


def photometry_mko(data):
    # Uses reference to determine the band and telescope used for photometry
    ref = references(data['ref_Y_MKO'])
    mag = data['Y_MKO']
    mag_err = data['Yerr_MKO']
    band = None
    tel = None

    wircam_ref_list = ['Albe11', 'Delo08.961', 'Delo12', 'Dupu19', 'Liu_16', 'Naud14']
    wircam_name_list = ['ULAS J115038.79+094942.9']
    wfcam_ref_list = ['Burn08', 'Burn14', 'Card15', 'Deac11.6319', 'Deac12.100', 'Deac17.1126', 'Lawr07', 'Lawr12',
                      'Liu_13.20', 'Lodi07.372', 'Luca10', 'Pinf08', 'Warr07.1400']
    wfcam_name_list = ['HD 253662B', 'NLTT 27966B']
    niri_ref_list = ['Dupu15.102', 'Legg13', 'Legg15', 'Legg16', 'Liu_12']
    vista_ref_list = ['Edge16', 'Gauz12', 'Kell16', 'McMa13', 'Minn17', 'Lodi12.53', 'Lodi13.2474', 'Pena11', 'Pena12',
                      'Smit18']
    gpi_ref_list = ['Garc17.162']
    visao_ref_list = ['Male14']
    ufti_name_list = ['ULAS J133502.11+150653.5', 'ULAS J115338.74-014724.1', 'ULAS J120257.05+090158.8',
                      'ULAS J120744.65+133902.7', 'ULAS J123327.45+121952.2', 'ULAS J130217.21 + 130851.2',
                      'ULAS J131943.77 + 120900.2', 'ULAS J134940.81 + 091833.3', 'ULAS J135607.41 + 085345.2',
                      'ULAS J144555.24 + 125735.1', 'ULAS J145935.25 + 085751.2', 'ULAS J230601.02 + 130225.0',
                      'ULAS J231557.61 + 132256.2', 'ULAS J232035.28+144829.8', 'ULAS J232123.79+135454.9',
                      'ULAS J232802.03+134544.8', 'ULAS J234827.94+005220.5']

    if ref in wircam_ref_list or data['name'] in wircam_name_list:
        band = 'Wircam.Y'
        tel = 'CFHT'
    elif ref in wfcam_ref_list:
        band = 'WFCAM.Y'
        tel = 'UKIRT'
    elif ref in niri_ref_list:
        band = 'NIRI.Y'
        tel = 'Gemini North'
    elif ref in vista_ref_list:
        band = 'VISTA.Y'
        tel = 'VISTA'
    elif ref in gpi_ref_list:
        band = 'GPI.Y'
        tel = 'Gemini South'
    elif ref in visao_ref_list:
        band = 'VisAO.Ys'
        tel = 'LCO'
    elif ref == 'Burn09':
        # Data taken from table in reference
        mag = 19.020
        mag_err = 0.080
        band = 'WFCAM.Y'
        tel = 'UKIRT'
    elif ref == 'Burn10.1885' or ref == 'Burn13':
        if data['name'] in ufti_name_list:
            band = 'UFTI.Y'
            tel = 'UKIRT'
        else:
            band = 'WFCAM.Y'
            tel = 'UKIRT'
    elif ref == 'Deac14.119':
        if data['name'] in wfcam_name_list:
            band = 'WFCAM.Y'
            tel = 'UKIRT'
        else:
            band = 'VISTA.Y'
            tel = 'VISTA'

    return data['name'], band, mag, mag_err, tel, ref


def ingest_mko(db, data):
    source, band, mag, mag_err, tel, ref = photometry_mko(data)
    if band or tel is not None:
        print(source, band, mag, mag_err, tel, ref)
        ingest_photometry(db=db, sources=source, bands=band, magnitudes=mag, magnitude_errors=mag_err,
                          reference=ref, telescope=tel, raise_error=False)


# Adding UKIDSS names
def add_ukidss_names(db, data):
    if pd.notna(data['designation_ukidss']):
        try:
            ingest_names(db, data['name'], data['designation_ukidss'])
        except SimpleError:
            pass


def add_filters(db):
    with db.engine.connect() as conn:
        conn.execute(db.Telescopes.insert().values([{'telescope': 'LCO'}]))

        wircam_instrument = [{'instrument': 'Wircam',
                              'mode': 'Imaging',
                              'telescope': 'CFHT'}]
        conn.execute(db.Instruments.insert().values(wircam_instrument))

        niri_instrument = [{'instrument': 'NIRI',
                            'mode': 'Imaging',
                            'telescope': 'Gemini North'}]
        conn.execute(db.Instruments.insert().values(niri_instrument))

        gpi_instrument = [{'instrument': 'GPI',
                           'mode': 'Imaging',
                           'telescope': 'Gemini South'}]
        conn.execute(db.Instruments.insert().values(gpi_instrument))

        visao_instrument = [{'instrument': 'VisAO',
                             'mode': 'Imaging',
                             'telescope': 'LCO'}]
        conn.execute(db.Instruments.insert().values(visao_instrument))

        wircam_y = [{'band': 'Wircam.Y',
                     'ucd': 'em.IR.NIR',
                     'effective_wavelength': '10220.90',
                     'width': '1084.17'}]

        conn.execute(db.PhotometryFilters.insert().values(wircam_y))

        niri_y = [{'band': 'NIRI.Y',
                   'ucd': 'em.IR.NIR',
                   'effective_wavelength': '10211.18',
                   'width': '943.58'}]

        conn.execute(db.PhotometryFilters.insert().values(niri_y))

        gpi_y = [{'band': 'GPI.Y',
                  'ucd': 'em.IR.NIR',
                  'effective_wavelength': '10375.56',
                  'width': '1707.30'}]

        conn.execute(db.PhotometryFilters.insert().values(gpi_y))

        visao_ys = [{'band': 'VisAO.Ys',
                     'ucd': 'em.IR.NIR',
                     'effective_wavelength': '9793.50',
                     'width': '907.02'}]

        conn.execute(db.PhotometryFilters.insert().values(visao_ys))
        conn.commit()


# Execute the ingests!
add_filters(db)
for i in range(len(df)):
    entry = df.iloc[i]
    if pd.notnull([entry['Y_MKO'], entry['Yerr_MKO']]).all():
        ingest_mko(db, entry)
        add_ukidss_names(db, entry)

# found 1 photometry measurements for GPI.Y
# found 21 photometry measurements for NIRI.Y
# found 13 photometry measurements for UFTI.Y
# found 29 photometry measurements for Wircam.Y
# found 854 photometry measurements for WFCAM.Y
# found 1 photometry measurements for VisAO.Ys
# found 59 photometry measurements for VISTA.Y
# 969 entries detected with test

# WRITE THE JSON FILES
if SAVE_DB:
    db.save_database(directory='data/')
