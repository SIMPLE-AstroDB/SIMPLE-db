from scripts.ingests.ingest_utils import *


def references(data_ref):
    # changes reference of the data sheet in the appropriate format used in database
    ref = data_ref
    if data_ref == 'Best20b':
        ref = 'Best20.42'
    if data_ref == 'Burn10b':
        ref = 'Burn10.1885'
    if data_ref == 'Deac11':
        ref = 'Deac11.6319'
    if data_ref == 'Deac12b':
        ref = 'Deac12.100'
    if data_ref == 'Deac14b':
        ref = 'Deac14.119'
    if data_ref == 'Deac17a':
        ref = 'Deac17.1126'
    if data_ref == 'Delo08a':
        ref = 'Delo08.961'
    if data_ref == 'Delo12b':
        ref = 'Delo12'
    if data_ref == 'Dupu12':
        ref = 'Dupu12.19'
    if data_ref == 'Dupu15a':
        ref = 'Dupu15.102'
    if data_ref == 'Garc17a':
        ref = 'Garc17.162'
    if data_ref == 'Legg00b':
        ref = 'Legg00'
    if data_ref == 'Legg02a':
        ref = 'Legg02.452'
    if data_ref == 'Legg02b':
        ref = 'Legg02.78'
    if data_ref == 'Legg10a':
        ref = 'Legg10'
    if data_ref == 'Liu_13b':
        ref = 'Liu_13.20'
    if data_ref == 'Lodi07a':
        ref = 'Lodi07.372'
    if data_ref == 'Lodi12d':
        ref = 'Lodi12.53'
    if data_ref == 'Lodi13a':
        ref = 'Lodi13.2474'
    if data_ref == 'Pinf14a':
        ref = 'Pinf14.1009'
    if data_ref == 'Warr07b':
        ref = 'Warr07.1400'
    else:
        pass
    return ref


def photometry_mko(db, data):
    # ref_skip = 'Best20.42', 'Bowl15', 'Chiu06'
    name_skip = []
    # uses reference to determine the instrument used for photometry
    ref = references(data['ref_Y_MKO'])
    mag = data['Y_MKO']
    mag_err = data['Yerr_MKO']

    wircam_ref_list = ['Albe11', 'Delo08.961', 'Delo12', 'Dupu19']
    wircam_name_list = ['ULAS J115038.79+094942.9']
    wfcam_ref_list = ['Burn08', 'Burn14', 'Card15', 'Deac11.6319', 'Deac12.100', 'Deac17.1126', 'Lawr07', 'Lawr12',
                      'Liu_13.20', 'Lodi07.372']
    # irac_name_list = ['2MASS J00501994-3322402', '2MASS J11145133-2618235', '2MASS J12373919+6526148']
    niri_ref_list = ['Dupu15.102', 'Lach15', 'Legg13', 'Legg15', 'Legg16', 'Liu_12']
    vista_ref_list = ['Edge16', 'Gauz12', 'Kell16', 'McMa13', 'Minn17', 'Lodi12.53']

    if ref in wircam_ref_list or data['name'] in wircam_name_list:
        band = 'Wircam.Y'
        tel = 'CFHT'
    if ref in wfcam_ref_list:
        band = 'WFCAM.Y'
        tel = 'UKIRT'
    if ref in niri_ref_list:
        band = 'NIRI.Y'
        tel = 'Gemini North'
    if ref in vista_ref_list:
        band = 'VISTA.Y'
        tel = 'VISTA'
    if ref == 'Burn13':
        if data['name'] == 'ULAS J133502.11+150653.5':
            band = 'UFTI.Y'
            tel = 'UKIRT'
        else:
            band = 'WFCAM.Y'
            tel = 'UKIRT'
    if ref == 'Burn09':
        # data taken from table in reference
        mag = 19.020
        mag_err = 0.080
        band = 'WFCAM.Y'
        tel = 'UKIRT'
    if ref == 'Deac14.119':
        if data['name'] == 'HD 253662B' or data['name'] == 'NLTT 27966B':
            band = 'WFCAM.Y'
            tel = 'UKIRT'
        else:
            band = 'VISTA.Y'
            tel = 'VISTA'
    else:
        name_skip.append(data['name'])

    if data['name'] not in name_skip:
        ingest_photometry(db=db, sources=data['name'], bands=band, magnitudes=mag,
                          magnitude_errors=mag_err, telescope=tel, reference=ref)
