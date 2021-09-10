import os
from astrodbkit2.astrodb import create_database
from astrodbkit2.astrodb import Database
from scripts.ingests.utils import *
from simple.schema import *
from pathlib import Path
from astroquery.gaia import Gaia
from sqlalchemy import and_, or_
from astropy.table import Table, Column, unique
from astropy.io.votable import from_table, writeto
from astroquery.simbad import Simbad
from astroquery.vizier import Vizier
import astropy.units as u


SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = False  # recreates the .db file from the data files
VERBOSE = True

verboseprint = print if VERBOSE else lambda *a, **k: None


def load_db():
    # Utility function to load the database

    db_file = 'SIMPLE.db'
    db_file_path = Path(db_file)
    db_connection_string = 'sqlite:///SIMPLE.db'

    if RECREATE_DB and db_file_path.exists():
        os.remove(db_file)  # removes the current .db file if one already exists

    if not db_file_path.exists():
        create_database(db_connection_string)  # creates empty database based on the simple schema
        db = Database(db_connection_string)  # connects to the empty database
        db.load_database('data/')  # loads the data from the data files into the database
    else:
        db = Database(db_connection_string)  # if database already exists, connects to .db file

    return db


db = load_db()

# get all sources that have Gaia DR2 designations
# 2nd query which is all sources that are not in that list
gaia_sources = db.query(db.Names.c.source).filter(db.Names.c.other_name.ilike("Gaia DR2%")).table()
if gaia_sources:
    gaia_sources_list = gaia_sources['source'].tolist()
    no_gaia_source_id = db.query(db.Sources.c.source).filter(db.Sources.c.source.notin_(gaia_sources_list)).\
        table()['source'].tolist()
else:
    no_gaia_source_id = db.query(db.Sources.c.source).table()['source'].tolist()


# Use SIMBAD to find Gaia designations for sources without a Gaia designation
def find_gaia_in_simbad(no_gaia_source_id):
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
    simbad_gaia_designation = []

    for row in simbad_ids:
        db_name = row['TYPED_ID']
        ids = row['IDS'].split('|')
        gaia_designation = [i for i in ids if "Gaia DR2" in i]

        if gaia_designation:
            verboseprint(db_name, gaia_designation[0])
            db_names.append(db_name)
            simbad_gaia_designation.append(gaia_designation[0])

    n_matches = len(db_names)
    print('Found', n_matches, 'Gaia sources for', n_sources, ' sources')

    # TODO: make table of db_names and simbad_gaia_designation and return it. ZIP?
    # print(simbad_gaia_designation)
    print(n_matches, len(simbad_gaia_designation))

    table = Table([db_names, simbad_gaia_designation], names=('db_names', 'gaia_designation'))

    table.write('scripts/ingests/Gaia/gaia_designations.xml', format='votable', overwrite=True)

    return table


# gaia_designations = find_gaia_in_simbad(no_gaia_source_id)
# Don't need to re-run since designations are in scripts/ingests/Gaia/gaia_designations.xml

def query_gaia():
    gaia_query_string = "SELECT *,upload_table.db_names FROM gaiadr2.gaia_source " \
                             "INNER JOIN tap_upload.upload_table ON " \
                        "gaiadr2.gaia_source.designation = tap_upload.upload_table.gaia_designation  "
    job_gaia_query = Gaia.launch_job(gaia_query_string, upload_resource='scripts/ingests/Gaia/gaia_designations.xml',
                                     upload_table_name="upload_table", verbose=True)

    gaia_data = job_gaia_query.get_results()

    return gaia_data

# Re-run Gaia query
# gaia_data = query_gaia()


# read results from saved table
gaia_data = Table.read('scripts/ingests/Gaia/gaia_data.xml', format='votable')


# add Gaia telescope, instrument, and Gaia filters
def update_ref_tables(db):
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


# add Gaia designations to Names table
# add_names(db, sources=gaia_data['db_names'], other_names=gaia_data['gaia_designation'], verbose=True)


# add Gaia proper motions
def add_gaia_pms(db):
    gaia_pms_df = gaia_data['db_names', 'pmra', 'pmra_error', 'pmdec', 'pmdec_error'].to_pandas()
    gaia_pms_df = gaia_pms_df[gaia_pms_df['pmra'].notna()]
    gaia_pms_df.reset_index(inplace=True, drop=True)
    refs = ['GaiaDR2'] * len(gaia_pms_df)
    ingest_proper_motions(db, gaia_pms_df['db_names'],
                          gaia_pms_df['pmra'], gaia_pms_df['pmra_error'],
                          gaia_pms_df['pmdec'], gaia_pms_df['pmdec_error'],
                          refs)


# add_gaia_pms(db)


# add Gaia parallaxes
def add_gaia_parallaxes(db):
    unmasked = np.logical_not(gaia_data['parallax'].mask).nonzero()
    gaia_parallaxes = gaia_data[unmasked]['db_names', 'parallax', 'parallax_error']
    refs = ['GaiaDR2'] * len(gaia_parallaxes)
    ingest_parallaxes(db, gaia_parallaxes['db_names'], gaia_parallaxes['parallax'],
                      gaia_parallaxes['parallax_error'], refs, verbose=True)


# add_gaia_parallaxes(db)

gaia_g_phot = gaia_data['gaia_designation','db_names', 'phot_g_mean_mag']
gaia_bp_phot = gaia_data['db_names', 'phot_bp_mean_mag']
gaia_rp_phot = gaia_data['db_names', 'phot_rp_mean_mag']

def query_vizier(db):
    v = Vizier(columns=["DR2Name","e_Gmag",'e_BPmag','e_RPmag',"+_r"], catalog="I/345/gaia2")
    radius = 20*u.arcsec
    for source in gaia_g_phot[0:10]:
        print(source['gaia_designation'], source['db_names'])
        result = v.query_object(source['gaia_designation'], radius=radius)
        if len(result) == 0:
            print("no hits within", radius)
        elif len(result[0]) == 1:
            print(result[0])
            # TODO append results to new list
        elif len(result[0]) > 1:
            print("too many hits",len(result[0]))
            # TODO find matching Gaia designation


query_vizier(db)

# TODO: ingest Gaia photometry
def add_gaia_photometry(db):

    ingest_photometry(db, gaia_bp_phot['db_names'], gaia_bp_phot['phot_bp_mean_mag'], gaia_bp_phot['UNKNOWN'],
                      reference = 'GaiaDR2', ucds = 'em.opt.V', telescope='Gaia', instrument='Gaia', verbose=True)

    ingest_photometry(db, gaia_rp_phot['db_names'], gaia_rp_phot['phot_rp_mean_mag'], gaia_rp_phot['UNKNOWN'],
                      reference='GaiaDR2', ucds='em.opt.R', telescope='Gaia', instrument='Gaia', verbose=True)

    ingest_photometry(db, gaia_g_phot['db_names'], gaia_g_phot['phot_g_mean_mag'], gaia_g_phot['UNKNOWN'],
                      reference='GaiaDR2', ucds='em.opt', telescope='Gaia', instrument='Gaia', verbose=True)

    return