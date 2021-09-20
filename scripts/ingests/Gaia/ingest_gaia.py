from scripts.ingests.utils import *
from astroquery.gaia import Gaia
from astroquery.simbad import Simbad
from astropy.table import Table, setdiff
from astropy import table
import numpy as np
from sqlalchemy import func
import numpy as np


SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = False  # recreates the .db file from the data files
VERBOSE = True
DATE_SUFFIX = 'Sep2021'

verboseprint = print if VERBOSE else lambda *a, **k: None

db = load_simpledb(RECREATE_DB=RECREATE_DB)

# get all sources that have Gaia DR2 designations
# 2nd query which is all sources that are not in that list
gaia_sources = db.query(db.Names.c.source).filter(db.Names.c.other_name.ilike("Gaia DR2%")).table()
if gaia_sources:
    gaia_sources_list = gaia_sources['source'].tolist()
    no_gaia_source_id = db.query(db.Sources.c.source).filter(db.Sources.c.source.notin_(gaia_sources_list)).\
        table()['source'].tolist()
else:
    no_gaia_source_id = db.query(db.Sources.c.source).table()['source'].tolist()



def find_in_simbad(no_gaia_source_id, desig_prefix, source_id_index = None, verbose = False):
    """
    Function to extract source designations from SIMBAD

    Parameters
    ----------
    no_gaia_source_id
    desig_prefix
    source_id_index
    verbose

    Returns
    -------
    Astropy table

    """

    verboseprint = print if VERBOSE else lambda *a, **k: None

    sources = no_gaia_source_id
    n_sources = len(sources)

    Simbad.reset_votable_fields()
    Simbad.add_votable_fields('typed_id')  # keep search term in result table
    Simbad.add_votable_fields('ids')  # add all SIMBAD identifiers as an output column
    print("simbad query started")
    result_table = Simbad.query_objects(sources)
    print("simbad query ended")

    ind = result_table['SCRIPT_NUMBER_ID'] > 0  # find indexes which contain results

    simbad_ids = result_table['TYPED_ID', 'IDS'][ind]  # .topandas()

    db_names = []
    simbad_designations = []
    if source_id_index is not None:
        source_ids = []

    for row in simbad_ids:
        db_name = row['TYPED_ID']
        ids = row['IDS'].split('|')
        designation = [i for i in ids if desig_prefix in i]

        if designation:
            verboseprint(db_name, designation[0])
            db_names.append(db_name)
            simbad_designations.append(designation[0])
            if source_id_index is not None:
                source_id = designation[0].split()[source_id_index]
                source_ids.append(int(source_id)) #convert to int since long in Gaia

    n_matches = len(db_names)
    print('Found', n_matches, desig_prefix, ' sources for', n_sources, ' sources')

    result_table = Table([db_names, simbad_designations, source_ids],
                      names=('db_names', 'designation', 'source_id'))

    return result_table


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


# GET GAIA DR2 DATA
# Re-run Gaia query
# gaia_dr2_data = query_gaiadr2(dr2_desig_file_string)
dr2_data_file_string = 'scripts/ingests/Gaia/gaia_dr2_data_'+DATE_SUFFIX+'.xml'
# gaia_dr2_data.write(dr2_data_file_string, format='votable')
# read results from saved table
gaia_dr2_data = Table.read(dr2_data_file_string, format='votable')


# GET Gaia DR3 designations
def query_gaiadr3_names_from_dr2(input_table):
    gaiadr3_query_string = "SELECT * FROM gaiaedr3.dr2_neighbourhood " \
                        "INNER JOIN tap_upload.upload_table ON " \
                        "gaiaedr3.dr2_neighbourhood.dr2_source_id = tap_upload.upload_table.source_id"

    job_gaiadr3_query = Gaia.launch_job(gaiadr3_query_string, upload_resource=input_table,
                                     upload_table_name="upload_table", verbose=VERBOSE)

    gaiadr3_names = job_gaiadr3_query.get_results()

    print("length of Dr3 names table: ", len(gaiadr3_names))

    # Find Dupes

    # find dr2 sources with only one dr3 entry
    gaiadr3_unique = table.unique(gaiadr3_names, keys='dr2_source_id', keep='none')
    print("Number of unique sources: ", len(gaiadr3_unique))

    # dr2 sources with multiple dr3 matches
    dr3_dupes = setdiff(gaiadr3_names, gaiadr3_unique, keys='dr2_source_id')
    dr3_dupes_grouped = dr3_dupes.group_by('dr2_source_id')

    for group in dr3_dupes_grouped.groups:
        # print(group['dr2_source_id', 'dr3_source_id', 'magnitude_difference', 'angular_distance'])
        if not np.ma.is_masked(min(abs(group['magnitude_difference']))):
            min_mag_index = abs(group['magnitude_difference']).tolist().index(min(abs(group['magnitude_difference'])))
        min_angdist_index = group['angular_distance'].tolist().index(min(group['angular_distance']))

        if min_angdist_index == min_mag_index:
            print('best one found \n', group['dr2_source_id', 'dr3_source_id'][min_angdist_index], '\n')
            gaiadr3_unique.add_row(group[min_mag_index])
        elif np.ma.is_masked(min(abs(group['magnitude_difference']))):
            print('best one found just with angular distance \n', group['dr2_source_id', 'dr3_source_id'][min_angdist_index], '\n')
            gaiadr3_unique.add_row(group[min_mag_index])
        else:
            print('no choice for \n', group['dr2_source_id', 'dr3_source_id', 'magnitude_difference', 'angular_distance'], '\n')

    print("Number of unique sources after dupe fix:", len(gaiadr3_unique))

    return gaiadr3_unique


# gaiadr3_names = query_gaiadr3_names_from_dr2(dr2_desig_file_string)
dr3_desig_file_string = 'scripts/ingests/Gaia/gaia_dr3_designations_' + DATE_SUFFIX +'.xml'
# gaiadr3_names.write(dr3_desig_file_string, format='votable', overwrite=True)
gaiadr3_names = Table.read(dr3_desig_file_string, format='votable')


# TODO: query eDR3

# add Gaia telescope, instrument, and Gaia filters
def update_ref_tables():
    add_publication(db, doi='10.1051/0004-6361/201629272', name='Gaia')
    db.Publications.delete().where(db.Publications.c.name == 'GaiaDR2').execute()
    db.Publications.update().where(db.Publications.c.name == 'Gaia18').values(name='GaiaDR2').execute()

    gaia_instrument = [{'name': 'Gaia', 'reference': 'Gaia'}]
    gaia_telescope = gaia_instrument
    db.Instruments.insert().execute(gaia_instrument)
    db.Telescopes.insert().execute(gaia_telescope)

    gaia_filters = [{'band': 'GAIA2.Gbp',
                     'instrument': 'Gaia',
                     'telescope': 'Gaia',
                     'effective_wavelength': 5050.,
                     'width': 2347.},
                    {'band': 'GAIA2.G',
                     'instrument': 'Gaia',
                     'telescope': 'Gaia',
                     'effective_wavelength': 6230.,
                     'width': 4183.},
                    {'band': 'GAIA2.Grp',
                     'instrument': 'Gaia',
                     'telescope': 'Gaia',
                     'effective_wavelength': 7730.,
                     'width': 2757.}
                    ]

    db.PhotometryFilters.insert().execute(gaia_filters)
    db.save_reference_table('Publications', 'data')
    db.save_reference_table('Instruments', 'data')
    db.save_reference_table('Telescopes', 'data')
    db.save_reference_table('PhotometryFilters', 'data')


# TODO: add DR3 refs
# update_ref_tables()

# add Gaia designations to Names table
# add_names(db, sources=gaia_data['db_names'], other_names=gaia_data['gaia_designation'], verbose=VERBOSE)
# TODO: add DR3 designations

# add Gaia proper motions
def add_gaia_pms():
    gaia_pms_df = gaia_data['db_names', 'pmra', 'pmra_error', 'pmdec', 'pmdec_error'].to_pandas()
    # drop empty rows using Pandas
    gaia_pms_df = gaia_pms_df[gaia_pms_df['pmra'].notna()]
    gaia_pms_df.reset_index(inplace=True, drop=True)
    refs = ['GaiaDR2'] * len(gaia_pms_df)
    ingest_proper_motions(db, gaia_pms_df['db_names'],
                          gaia_pms_df['pmra'], gaia_pms_df['pmra_error'],
                          gaia_pms_df['pmdec'], gaia_pms_df['pmdec_error'],
                          refs)


# add_gaia_pms()


# add Gaia parallaxes
def add_gaia_parallaxes():
    # drop empty rows using Astropy Tables
    unmasked_pi = np.logical_not(gaia_data['parallax'].mask).nonzero()
    gaia_parallaxes = gaia_data[unmasked_pi]['db_names', 'parallax', 'parallax_error']
    refs = ['GaiaDR2'] * len(gaia_parallaxes)
    ingest_parallaxes(db, gaia_parallaxes['db_names'], gaia_parallaxes['parallax'],
                      gaia_parallaxes['parallax_error'], refs, verbose=VERBOSE)


# add_gaia_parallaxes()


def add_gaia_photometry():
    # drop empty rows
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

    ingest_photometry(db, gaia_rp_phot['db_names'], 'GAIA2.Grp', gaia_rp_phot['phot_rp_mean_mag'],
                      gaia_rp_phot['rp_unc'], 'GaiaDR2', ucds='em.opt.R', telescope='Gaia', instrument='Gaia',
                      verbose=VERBOSE)

    ingest_photometry(db, gaia_g_phot['db_names'], 'GAIA2.G', gaia_g_phot['phot_g_mean_mag'], gaia_g_phot['g_unc'],
                      'GaiaDR2', ucds='em.opt', telescope='Gaia', instrument='Gaia', verbose=VERBOSE)

    return


# add_gaia_photometry()

# query the database for number to add to the data tests
# Expected numbers:
#   Names added to database:  1266
#   Proper motions added to database:  1076
#   Parallaxes added to database:  1076
#   Grp: Photometry measurements added to database:  1106
#   G: Photometry measurements added to database:  1266

phot_count = db.query(Photometry.band, func.count(Photometry.band)).group_by(Photometry.band).all()
print('Photometry: ', phot_count)

pi_count = db.query(Parallaxes.reference, func.count(Parallaxes.reference)).group_by(Parallaxes.reference).\
    order_by(func.count(Parallaxes.reference).desc()).limit(10).all()
print("Parallax count: ", pi_count)

pm_count = db.query(ProperMotions.reference, func.count(ProperMotions.reference)).group_by(ProperMotions.reference).\
    order_by(func.count(ProperMotions.reference).desc()).limit(20).all()
print("Proper motion count: ", pm_count)

# Write the JSON files
if SAVE_DB:
    db.save_database(directory='data/')
