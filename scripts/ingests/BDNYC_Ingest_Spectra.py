from scripts.ingests.utils import *
import pandas as pd
import numpy.ma as ma

SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files
VERBOSE = False

logger.setLevel(logging.INFO)

db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)

# Delete test spectrum
db.Spectra.delete().where(db.Spectra.c.source == '2MASS J00192626+4614078').execute()

# Read in CSV file as Astropy table
data = Table.read('scripts/ingests/BDNYC_spectra4.csv')


# Inserting various Libraries for Modes,Telescopes and Instruments
def insert_new_modes():
    telescope_mgl2 = [{'name': 'Magellan II Clay'}]
    instrument_mgl2 = [{'name': 'MagE'}]
    echelle2_mode = [{'name': 'Echelle',
                      'instrument': 'MagE',
                      'telescope': 'Magellan II Clay'}]
    # Inserting CTIO 1.5m
    telescope_ct = [{'name': 'CTIO 1.5m'}]
    instruments_ct = [{'name': 'OSIRIS'}]
    sxd_mode_ct = [{'name': 'SXD',
                    'instrument': 'OSIRIS',
                    'telescope': 'CTIO 1.5m'}]
    # Inserting LL Mode
    ll_mode = [{'name': 'LL',
                'instrument': 'IRS',
                'telescope': 'Spitzer'}]
    # Inserting Magellan I Baade
    telescope_mgl = [{'name': 'Magellan I Baade'}]
    instruments_mgl = [{'name': 'FIRE'}]
    echelle_mode = [{'name': 'Echelle',
                     'instrument': 'FIRE',
                     'telescope': 'Magellan I Baade'}]
    # Inserting IRS instrument and SL mode
    instrument_sl = [{'name': 'IRS'}]
    sxd_mode_sl = [{'name': 'SL',
                    'instrument': 'IRS',
                    'telescope': 'Spitzer'}]
    # Inserting Gemini South in various tables
    telescope_gmos = [{'name': 'Gemini South'}]
    instruments_gmos = [{'name': 'GMOS-S'}]
    prism_mode_gmos = [{'name': 'Prism',
                        'instrument': 'GMOS-S',
                        'telescope': 'Gemini South'}]
    # Inserting Gemini North in various tables
    telescope_gnirs = [{'name': 'Gemini North'}]
    instruments_gnirs = [{'name': 'GNIRS'}]
    sxd_mode_gnirs = [{'name': 'SXD',
                       'instrument': 'GNIRS',
                       'telescope': 'Gemini North'}]
    instrument_gmos_n = [{'name': 'GMOS-N'}]
    prism_mode_gmos_n = [{'name': 'Prism',
                          'instrument': 'GMOS-N',
                          'telescope': 'Gemini North'}]
    # Inserting Keck I telescope and LRIS instrument
    telescopekc = [{'name': 'Keck I'}]
    instrumentlris = [{'name': 'LRIS'}]
    # Inserting CTIO 4m telescope and R-C Spec Instrument
    telescope_ct4 = [{'name': 'CTIO 4m'}]
    instrument_rc = [{'name': 'R-C Spec'}]
    # Inserting SXD mode
    sxd_mode = [{'name': 'SXD',
                 'instrument': 'SpeX',
                 'telescope': 'IRTF'}]
    # Inserting KPNO 2.1 telescope
    telescope_kp = [{'name': 'KPNO 2.1m'}]
    instrument_g = [{'name': 'GoldCam'}]
    # Inserting KPNO 4m
    telescope_kp4 = [{'name': 'KPNO 4m'}]
    # Inserting LDSS3 instrument
    instrument_ld = [{'name': 'LDSS3'}]
    # Inserting ARC 3.5m telescope
    telescope_arc = [{'name': 'ARC 3.5m'}]
    # Inserting DIS instrument
    instrument_dis = [{'name': 'DIS'}]
    # Inserting ESO telescope and SINFONI instrument
    telescope_eso = [{'name': 'ESO VLT U2'}]
    instrument_sin = [{'name': 'SINFONI'}]

    db.Telescopes.insert().execute(telescope_mgl2)
    db.Telescopes.insert().execute(telescope_ct)
    db.Telescopes.insert().execute(telescope_mgl)
    db.Telescopes.insert().execute(telescope_gmos)
    db.Telescopes.insert().execute(telescope_gnirs)
    db.Telescopes.insert().execute(telescopekc)
    db.Telescopes.insert().execute(telescope_ct4)
    db.Telescopes.insert().execute(telescope_kp)
    db.Telescopes.insert().execute(telescope_kp4)
    db.Telescopes.insert().execute(telescope_arc)
    db.Telescopes.insert().execute(telescope_eso)
    db.Instruments.insert().execute(instrument_mgl2)
    db.Instruments.insert().execute(instruments_gmos)
    db.Instruments.insert().execute(instrument_sl)
    db.Instruments.insert().execute(instruments_mgl)
    db.Instruments.insert().execute(instruments_ct)
    db.Instruments.insert().execute(instrument_gmos_n)
    db.Instruments.insert().execute(instruments_gnirs)
    db.Instruments.insert().execute(instrumentlris)
    db.Instruments.insert().execute(instrument_rc)
    db.Instruments.insert().execute(instrument_g)
    db.Instruments.insert().execute(instrument_ld)
    db.Instruments.insert().execute(instrument_dis)
    db.Instruments.insert().execute(instrument_sin)
    db.Modes.insert().execute(echelle2_mode)
    db.Modes.insert().execute(sxd_mode_ct)
    db.Modes.insert().execute(ll_mode)
    db.Modes.insert().execute(echelle_mode)
    db.Modes.insert().execute(sxd_mode_sl)
    db.Modes.insert().execute(prism_mode_gmos)
    db.Modes.insert().execute(prism_mode_gmos_n)
    db.Modes.insert().execute(sxd_mode_gnirs)
    db.Modes.insert().execute(sxd_mode)

    return


if RECREATE_DB:
    insert_new_modes()

source_names = data['designation']

missing_indices, existing_indices, alt_names_table = sort_sources(db, source_names)

add_names(db, names_table=alt_names_table)

# ADD MISSING SOURCES
to_add = data[missing_indices]
ingest_sources(db, to_add['designation'], to_add['ra'], to_add['dec'], to_add['publication_shortname'],
               comments=to_add['comments'])

missing_indices2, existing_indices2, alt_names_table2 = sort_sources(db, source_names)
existing_data = data[existing_indices2]

# ADD THE SPECTRA TO THE DATABASE
# TODO: convert to function in utils

n_spectra = len(existing_data)
n_skipped = 0
n_dupes = 0
n_added = 0
n_blank = 0

msg = f'Trying to add {n_spectra} spectra'
logger.info(msg)

for row in existing_data:
    # TODO: check that spectrum can be read by astrodbkit

    # Get source name as it appears in the database
    db_name = find_source_in_db(db, row['designation'])

    # Find what spectra already exists in database for this source
    source_spec_data = db.query(db.Spectra).filter(db.Spectra.c.source == db_name).table()

    # SKIP if observation date is blank
    # TODO: try to populate obs date from meta data
    if ma.is_masked(row['obs_date']) or row['obs_date'] == '':
        obs_date = None
        missing_obs_msg = f"Skipping spectrum with missing observation date: {row['designation']} \n"
        missing_row_spe = f"{row['designation', 'name_1', 'mode', 'obs_date', 'publication_shortname']} \n"
        logger.info(missing_obs_msg)
        logger.debug(missing_row_spe)
        n_blank += 1
        continue
    else:
        try:
            obs_date = pd.to_datetime(row["obs_date"])
        except:
            logger.warning(f"Skipping {row['designation']} Cant convert obs date to Date Time object: {obs_date}")
            n_skipped += 1
            continue

    if row['regime'] == 'OPT':
        regime = 'optical'
    elif row['regime'] == 'NIR' or row["regime"] == 'nir':
        regime = 'nir'
    elif row['regime'] == 'MIR':
        regime = 'mir'  # should be 'em.IR.MIR'
    else:
        regime = row["regime"]
        logger.warning(f'Regime unknown: {regime}')

    publication_shortname = row["publication_shortname_1"]
    if publication_shortname == 'Alle07':
        publication_shortname = 'Alle07a'

    row_data = [{'source': db_name,
                 'spectrum': row['spectrum'],
                 'local_spectrum': None if ma.is_masked(row["local_spectrum"]) else row["local_spectrum"],
                 'regime': regime,
                 'telescope': row["name"],
                 'instrument': None if ma.is_masked(row["name_1"]) else row["name_1"],
                 'mode': None if ma.is_masked(row["mode"]) else row["mode"],
                 'observation_date': obs_date,
                 'wavelength_units': None if ma.is_masked(row["wavelength_units"]) else row["wavelength_units"],
                 'flux_units': None if ma.is_masked(row["flux_units"]) else row["flux_units"],
                 'wavelength_order': None if ma.is_masked(row["wavelength_order"]) else row["wavelength_order"],
                 'comments': None if ma.is_masked(row["comments_1"]) else row["comments_1"],
                 'reference': publication_shortname}]
    logger.debug(row_data)

    try:
        db.Spectra.insert().execute(row_data)
        n_added += 1
    except sqlalchemy.exc.IntegrityError:
        # TODO: add elif to check if reference is in Publications Table
        if len(source_spec_data) > 0:  # Spectra data already exists
            # check for duplicate measurement
            ref_dupe_ind = source_spec_data['reference'] == publication_shortname
            date_dupe_ind = source_spec_data['observation_date'] == obs_date
            instrument_dupe_ind = source_spec_data['instrument'] == row['name_1']
            mode_dupe_ind = source_spec_data['mode'] == row['mode']
            file_dupe_ind = source_spec_data['spectrum'] == row['spectrum']
            if sum(ref_dupe_ind) and sum(date_dupe_ind) and sum(instrument_dupe_ind) and sum(mode_dupe_ind):
                msg = f"Skipping suspected duplicate measurement \n {row['designation','name_1', 'mode', 'spectrum']} \n"
                msg2 = f"{source_spec_data[ref_dupe_ind]['source', 'instrument', 'mode', 'observation_date', 'reference']}"
                msg3 = f"{row['designation', 'name_1', 'mode', 'obs_date', 'publication_shortname']} \n"
                logger.warning(msg)
                logger.debug(msg2 + msg3)
                n_dupes += 1
                continue  # Skip duplicate measurement
            else:
                msg = f'Spectrum could not be added to the database (other data exist): \n ' \
                      f"{row['designation', 'name_1', 'mode', 'obs_date', 'publication_shortname','spectrum']} \n"
                msg2 = f"Existing Data: \n " \
                       f"{source_spec_data[ref_dupe_ind]['source', 'instrument', 'mode', 'observation_date', 'reference','spectrum']}"
                msg3 = f"Data not able to add: \n {row_data} \n "
                logger.warning(msg + msg2)
                logger.debug(msg3)
                n_skipped += 1
                continue
        else:
            msg = f'Spectrum could not be added to the database: \n {row_data} \n '
            logger.error(msg)
            raise SimpleError(msg)

logger.info(f"Spectra added: {n_added}")
logger.info(f"Spectra with blank obs_date: {n_blank}")
logger.info(f"Suspected duplicates skipped: {n_dupes}")
logger.info(f"Spectra skipped for unknown reason: {n_skipped}")

if n_added + n_dupes + n_blank + n_skipped != n_spectra:
    msg = "Numbers don't add up: "
    logger.error(msg)
    raise SimpleError(msg)

# TODO: add to tests
from sqlalchemy import func
spec_count = db.query(Spectra.regime, func.count(Spectra.regime)).group_by(Spectra.regime).all()
# [(<Regime.mir: 'em.IR.MIR'>, 91), (<Regime.nir: 'em.IR.NIR'>, 457), (<Regime.optical: 'em.opt'>, 720)]


spec_ref_count = db.query(Spectra.reference, func.count(Spectra.reference)).\
    group_by(Spectra.reference).order_by(func.count(Spectra.reference).desc()).limit(20).all()
# [('Reid08b', 280), ('Cruz03', 191), ('Cruz18', 186), ('Cruz07', 158), ('Bard14', 57), ('Burg10a', 46),
# ('Cush06b', 30), ('Rayn09', 17), ('Kirk10', 15), ('Burg08d', 15), ('Burg04b', 15), ('Fili15', 14),
# ('PID51', 13), ('Kirk00', 11), ('PID3136', 10), ('CruzUnpub', 10), ('Burg06b', 10), ('Kirk08', 9),
# ('Cruz09', 9), ('Bonn14b', 8)]

telescope_spec_count = db.query(Spectra.telescope, func.count(Spectra.telescope)).\
    group_by(Spectra.telescope).order_by(func.count(Spectra.telescope).desc()).limit(20).all()

logger.info(f'Spectra in the database: \n {spec_count} \n {spec_ref_count} \n {telescope_spec_count}' )
