from scripts.ingests.utils import *
from astroquery.gaia import Gaia
from astropy.table import Table, setdiff
from astropy import table
from sqlalchemy import func
import numpy as np

# GLOBAL VARIABLES
SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files
VERBOSE = False
DATE_SUFFIX = 'Sep2021'


# FUNCTIONS
def query_gaiadr2(input_table):
    print('Gaia DR2 query started')
    gaia_query_string = "SELECT *,upload_table.db_names FROM gaiadr2.gaia_source " \
                             "INNER JOIN tap_upload.upload_table ON " \
                        "gaiadr2.gaia_source.designation = tap_upload.upload_table.designation  "
    job_gaia_query = Gaia.launch_job(gaia_query_string, upload_resource=input_table,
                                     upload_table_name="upload_table", verbose=VERBOSE)

    gaia_data = job_gaia_query.get_results()

    print('Gaia DR2 query complete')

    return gaia_data


def query_gaiadr3_names_from_dr2(input_table):
    gaiadr3_query_string = "SELECT * FROM gaiaedr3.dr2_neighbourhood " \
                        "INNER JOIN tap_upload.upload_table ON " \
                        "gaiaedr3.dr2_neighbourhood.dr2_source_id = tap_upload.upload_table.source_id"

    job_gaiadr3_query = Gaia.launch_job(gaiadr3_query_string, upload_resource=input_table,
                                     upload_table_name="upload_table", verbose=VERBOSE)

    gaiadr3_names = job_gaiadr3_query.get_results()

    print("length of eDR3 names table: ", len(gaiadr3_names))

    # Find DR2 sources with only one EDR3 entry
    gaiadr3_unique = table.unique(gaiadr3_names, keys='dr2_source_id', keep='none')
    print("Number of unique sources: ", len(gaiadr3_unique))

    # Find the DR2 sources with multiple EDR3 matches
    dr3_dupes = setdiff(gaiadr3_names, gaiadr3_unique, keys='dr2_source_id')
    dr3_dupes_grouped = dr3_dupes.group_by('dr2_source_id')

    # Find the best EDR3 match for DR2 sources with multiple matches
    for group in dr3_dupes_grouped.groups:
        # Find the EDR3 matches with the smallest magnitude difference (if one exists) and angular distance.
        if not np.ma.is_masked(min(abs(group['magnitude_difference']))):
            min_mag_index = abs(group['magnitude_difference']).tolist().index(min(abs(group['magnitude_difference'])))
        min_angdist_index = group['angular_distance'].tolist().index(min(group['angular_distance']))

        if min_angdist_index == min_mag_index:
            # Source with smallest mag difference is the same as the closest source
            verboseprint('best one found \n', group['dr2_source_id', 'dr3_source_id'][min_angdist_index], '\n')
            gaiadr3_unique.add_row(group[min_mag_index])
        elif np.ma.is_masked(min(abs(group['magnitude_difference']))):
            # Magnitude differences aren't available for all sources so just using closest one
            verboseprint('best one found just with angular distance \n',
                  group['dr2_source_id', 'dr3_source_id'][min_angdist_index], '\n')
            gaiadr3_unique.add_row(group[min_mag_index])
        else:
            verboseprint('no choice for \n',
                  group['dr2_source_id', 'dr3_source_id', 'magnitude_difference', 'angular_distance'], '\n')

    verboseprint("Number of unique sources after dupe fix:", len(gaiadr3_unique))

    return gaiadr3_unique


def query_gaiaedr3(input_table):
    print('Gaia eDR3 query started')
    gaia_query_string = "SELECT *,upload_table.db_names FROM gaiaedr3.gaia_source " \
                             "INNER JOIN tap_upload.upload_table ON " \
                        "gaiaedr3.gaia_source.source_id = tap_upload.upload_table.dr3_source_id  "
    job_gaia_query = Gaia.launch_job(gaia_query_string, upload_resource=input_table,
                                     upload_table_name="upload_table", verbose=VERBOSE)

    gaia_data = job_gaia_query.get_results()

    print('Gaia eDR3 query complete')

    return gaia_data


def update_ref_tables():
    add_publication(db, doi='10.1051/0004-6361/201629272', name='Gaia')
    db.Publications.delete().where(db.Publications.c.name == 'GaiaDR2').execute()
    db.Publications.update().where(db.Publications.c.name == 'Gaia18').values(name='GaiaDR2').execute()
    add_publication(db, doi='10.1051/0004-6361/202039657', name='GaiaEDR3')

    gaia_instrument = [{'name': 'Gaia', 'reference': 'Gaia'}]
    gaia_telescope = gaia_instrument
    db.Instruments.insert().execute(gaia_instrument)
    db.Telescopes.insert().execute(gaia_telescope)

    # For future ingests, consider using the add_photometry_filters.py script
    gaia_filters = [{'band': 'GAIA2.G',
                     'instrument': 'Gaia',
                     'telescope': 'Gaia',
                     'effective_wavelength': 6230.,
                     'width': 4183.},
                    {'band': 'GAIA2.Grp',
                     'instrument': 'Gaia',
                     'telescope': 'Gaia',
                     'effective_wavelength': 7730.,
                     'width': 2757.},
                    {'band': 'GAIA3.G',
                     'instrument': 'Gaia',
                     'telescope': 'Gaia',
                     'effective_wavelength': 5822.,
                     'width': 4053.},
                    {'band': 'GAIA3.Grp',
                     'instrument': 'Gaia',
                     'telescope': 'Gaia',
                     'effective_wavelength': 7620.,
                     'width': 2924.}
                    ]

    gaia_dr3filters = [{'band': 'GAIA3.G',
                     'instrument': 'Gaia',
                     'telescope': 'Gaia',
                     'effective_wavelength': 5822.,
                     'width': 4053.},
                    {'band': 'GAIA3.Grp',
                     'instrument': 'Gaia',
                     'telescope': 'Gaia',
                     'effective_wavelength': 7620.,
                     'width': 2924.}
                    ]

    db.PhotometryFilters.insert().execute(gaia_filters)
    db.PhotometryFilters.insert().execute(gaia_dr3filters)
    db.save_reference_table('Publications', 'data')
    db.save_reference_table('Instruments', 'data')
    db.save_reference_table('Telescopes', 'data')
    db.save_reference_table('PhotometryFilters', 'data')


def add_gaia_pms(gaia_data, ref):
    unmasked_pms = np.logical_not(gaia_data['pmra'].mask).nonzero()
    pms = gaia_data[unmasked_pms]['db_names', 'pmra', 'pmra_error', 'pmdec', 'pmdec_error']
    refs = [ref] * len(pms)

    ingest_proper_motions(db, pms['db_names'],
                          pms['pmra'], pms['pmra_error'],
                          pms['pmdec'], pms['pmdec_error'],
                          refs, verbose=VERBOSE)

    return


def add_gaia_parallaxes(gaia_data, ref):
    unmasked_pi = np.logical_not(gaia_data['parallax'].mask).nonzero()
    gaia_parallaxes = gaia_data[unmasked_pi]['db_names', 'parallax', 'parallax_error']
    refs = [ref] * len(gaia_parallaxes)

    ingest_parallaxes(db, gaia_parallaxes['db_names'], gaia_parallaxes['parallax'],
                      gaia_parallaxes['parallax_error'], refs, verbose=VERBOSE)


def add_gaia_photometry(gaia_data,ref):
    unmasked_gphot = np.logical_not(gaia_data['phot_g_mean_mag'].mask).nonzero()
    gaia_g_phot = gaia_data[unmasked_gphot]['db_names', 'phot_g_mean_flux', 'phot_g_mean_flux_error',
                                            'phot_g_mean_mag']

    unmased_rpphot = np.logical_not(gaia_data['phot_rp_mean_mag'].mask).nonzero()
    gaia_rp_phot = gaia_data[unmased_rpphot]['db_names', 'phot_rp_mean_flux', 'phot_rp_mean_flux_error',
                                             'phot_rp_mean_mag']

    # e_Gmag=abs(-2.5/ln(10)*e_FG/FG) from Vizier Note 37 on Gaia DR2 (I/345/gaia2)
    gaia_g_phot['g_unc'] = np.abs(
        -2.5 / np.log(10) * gaia_g_phot['phot_g_mean_flux_error'] / gaia_g_phot['phot_g_mean_flux'])
    gaia_rp_phot['rp_unc'] = np.abs(
        -2.5 / np.log(10) * gaia_rp_phot['phot_rp_mean_flux_error'] / gaia_rp_phot['phot_rp_mean_flux'])

    if ref == 'GaiaDR2':
        g_band_name = 'GAIA2.G'
        rp_band_name = 'GAIA2.Grp'
    elif ref == 'GaiaEDR3':
        g_band_name = 'GAIA3.G'
        rp_band_name = 'GAIA3.Grp'
    else:
        raise Exception

    ingest_photometry(db, gaia_g_phot['db_names'], g_band_name, gaia_g_phot['phot_g_mean_mag'], gaia_g_phot['g_unc'],
                      ref, ucds='em.opt', telescope='Gaia', instrument='Gaia', verbose=VERBOSE)

    ingest_photometry(db, gaia_rp_phot['db_names'], rp_band_name, gaia_rp_phot['phot_rp_mean_mag'],
                      gaia_rp_phot['rp_unc'], ref, ucds='em.opt.R', telescope='Gaia', instrument='Gaia',
                      verbose=VERBOSE)

    return


# LOAD THE DATABASE
db = load_simpledb('SIMPLE.db', RECREATE_DB=RECREATE_DB)

# get all sources that have Gaia DR2 designations
# 2nd query which is all sources that are not in that list
gaia_sources = db.query(db.Names.c.source).filter(db.Names.c.other_name.ilike("Gaia DR2%")).table()
if gaia_sources:
    gaia_sources_list = gaia_sources['source'].tolist()
    no_gaia_source_id = db.query(db.Sources.c.source).filter(db.Sources.c.source.notin_(gaia_sources_list)).\
        table()['source'].tolist()
else:
    no_gaia_source_id = db.query(db.Sources.c.source).table()['source'].tolist()


# Use SIMBAD to find Gaia DR2 and DR3 designations for sources without a Gaia designation
# dr2_designations = find_in_simbad(no_gaia_source_id, 'Gaia DR2', source_id_index=2, verbose=VERBOSE)
dr2_desig_file_string = 'scripts/ingests/Gaia/gaia_dr2_designations_'+DATE_SUFFIX+'.xml'
# dr2_designations.write(dr2_desig_file_string, format='votable', overwrite=True)
dr2_designations = Table.read(dr2_desig_file_string, format='votable')

# Use once some DR3 designations in SIMBAD
# dr3_designations = find_in_simbad(no_gaia_source_id, 'Gaia DR3', source_id_index=2, verbose=VERBOSE )
# dr3_file_string = 'scripts/ingests/Gaia/gaia_dr3_designations',DATE_SUFFIX,'.xml'
# if len(dr3_designations) > 1:
#     print('Gaia DR3 hits in SIMBAD!')
#     dr3_designations.write(dr3_file_string, format='votable', overwrite=True)
# else:
#     print('No Gaia DR3 hits in SIMBAD')

# QUERY Gaia DR2
# gaia_dr2_data = query_gaiadr2(dr2_desig_file_string)
dr2_data_file_string = 'scripts/ingests/Gaia/gaia_dr2_data_'+DATE_SUFFIX+'.xml'
# gaia_dr2_data.write(dr2_data_file_string, format='votable')
gaia_dr2_data = Table.read(dr2_data_file_string, format='votable')

# USE DR2 DESIGNATIONS TO GET DR3 DESIGNATIONS
# gaiadr3_names = query_gaiadr3_names_from_dr2(dr2_desig_file_string)
dr3_desig_file_string = 'scripts/ingests/Gaia/gaia_dr3_designations_' + DATE_SUFFIX +'.xml'
# gaiadr3_names.write(dr3_desig_file_string, format='votable', overwrite=True)
gaiadr3_names = Table.read(dr3_desig_file_string, format='votable')

# QUERY Gaia EDR3
# gaia_edr3_data = query_gaiaedr3(gaiadr3_names)
edr3_data_file_string = 'scripts/ingests/Gaia/gaia_edr3_data_'+DATE_SUFFIX+'.xml'
# gaia_edr3_data.write(edr3_data_file_string, format='votable')
gaia_edr3_data = Table.read(edr3_data_file_string, format='votable')

# ADD Gaia TELESCOPE, INSTRUMENT AND Gaia FILTERS
# update_ref_tables()

# ADD Gaia DESIGNATIONS TO NAMES TABLE
add_names(db, sources=gaia_dr2_data['db_names'], other_names=gaia_dr2_data['designation'], verbose=VERBOSE)
add_names(db, sources=gaia_edr3_data['db_names'], other_names=gaia_edr3_data['designation'], verbose=VERBOSE)

# ADD Gaia PROPER MOTIONS
add_gaia_pms(gaia_dr2_data, 'GaiaDR2')
add_gaia_pms(gaia_edr3_data, 'GaiaEDR3')

# ADD Gaia PARALLAXES
add_gaia_parallaxes(gaia_dr2_data, 'GaiaDR2')
add_gaia_parallaxes(gaia_edr3_data, 'GaiaEDR3')

# ADD Gaia PHOTOMETRY
add_gaia_photometry(gaia_dr2_data, 'GaiaDR2')
add_gaia_photometry(gaia_edr3_data, 'GaiaEDR3')

# QUERY THE DATABASE for numbers to add to the data tests

# Expected numbers for DR2:
#   Names added to database:  1266
#   Proper motions added to database:  1076
#   Parallaxes added to database:  1076
#   G: Photometry measurements added to database:  1266
#   Rp: Photometry measurements added to database:  1106
# Expected numbers for EDR3:
#   Names added to database:  1265
#   Proper motions added to database:  1133
#   Parallaxes added to database:  1133
#   G: Photometry measurements added to database:  1256
#   Rp: Photometry measurements added to database:  1261

pm_count = db.query(ProperMotions.reference, func.count(ProperMotions.reference)).group_by(ProperMotions.reference).\
    order_by(func.count(ProperMotions.reference).desc()).limit(20).all()
print("Proper motion count: ", pm_count)
# Proper motion count:  [('GaiaEDR3', 1133), ('GaiaDR2', 1076), ('Best20a', 348), ('Gagn15a', 325), ('Fahe09', 216),
# ('Kirk19', 182), ('Best15', 120), ('Burn13', 97), ('Dahn17', 79), ('Jame08', 73), ('vanL07', 68), ('Smar18', 68),
# ('Liu_16', 50), ('Zhan10', 45), ('Schm10b', 44), ('Card15', 40), ('Wein16', 37), ('Case08', 35), ('Luhm14c', 31),
# ('Fahe12', 30)]

pi_count = db.query(Parallaxes.reference, func.count(Parallaxes.reference)).group_by(Parallaxes.reference).\
    order_by(func.count(Parallaxes.reference).desc()).limit(10).all()
print("Parallax count: ", pi_count)
# Parallax count:  [('GaiaEDR3', 1133), ('GaiaDR2', 1076), ('Kirk19', 22), ('Mart18', 15), ('Missing', 7),
# ('Vrba04', 3), ('Fahe12', 3), ('Dupu12a', 3), ('Tinn14', 2), ('Maro13', 2)]

phot_count = db.query(Photometry.band, func.count(Photometry.band)).group_by(Photometry.band).all()
print('Photometry: ', phot_count)
# Photometry:  [('GAIA2.G', 1266), ('GAIA2.Grp', 1106), ('GAIA3.G', 1256), ('GAIA3.Grp', 1261), ('IRAC.I1', 22),
# ('IRAC.I2', 22), ('WISE.W1', 349), ('WISE.W2', 349), ('WISE.W3', 347), ('WISE.W4', 340)]


# WRITE THE JSON FILES
if SAVE_DB:
    db.save_database(directory='data/')
