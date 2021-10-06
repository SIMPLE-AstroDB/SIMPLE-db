from scripts.ingests.utils import *
import pandas as pd
import numpy.ma as ma

SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = False  # recreates the .db file from the data files
VERBOSE = False

logger.setLevel(logging.INFO)

db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)

# Read in CSV file with Pandas
df = pd.read_csv('scripts/ingests/BDNYC_spectra2.csv')
data = Table.from_pandas(df)
#Inserting CTIO 1.5m
telescope_ct = [{'name': 'CTIO 1.5m'}]
instruments_ct = [{'name': 'OSIRIS'}]
sxd_mode_ct = [{'name': 'SXD',
                'instrument': 'OSIRIS',
                'telescope': 'CTIO 1.5m'}]
#db.Telescopes.insert().execute(telescope_ct)
#db.Instruments.insert().execute(instruments_ct)
#db.Modes.insert().execute(sxd_mode_ct)

#Inserting LL Mode
ll_mode = [{'name': 'LL',
            'instrument': 'IRS',
            'telescope': 'Spitzer'}]
#db.Modes.insert().execute(ll_mode)

#Inserting Magellan I Baade
telescope_mgl = [{'name': 'Magellan I Baade'}]
instruments_mgl = [{'name': 'FIRE'}]
echelle_mode = [{'name': 'Echelle',
                 'instrument': 'FIRE',
                 'telescope': 'Magellan I Baade'}]
#db.Telescopes.insert().execute(telescope_mgl)
#db.Instruments.insert().execute(instruments_mgl)
#db.Modes.insert().execute(echelle_mode)

# Inserting IRS instrument and SL mode
instrument_sl = [{'name': 'IRS'}]
sxd_mode_sl = [{'name': 'SL',
                'instrument': 'IRS',
                'telescope': 'Spitzer'}]
# db.Instruments.insert().execute(instrument_sl)
# db.Modes.insert().execute(sxd_mode_sl)

# Inserting Gemini South in various tables
telescope_gmos = [{'name': 'Gemini South'}]
instruments_gmos = [{'name': 'GMOS-S'}]
prism_mode_gmos = [{'name': 'Prism',
                    'instrument': 'GMOS-S',
                    'telescope': 'Gemini South'}]
#db.Telescopes.insert().execute(telescope_gmos)
#db.Instruments.insert().execute(instruments_gmos)
#db.Modes.insert().execute(prism_mode_gmos)

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
#db.Instruments.insert().execute(instrument_gmos_n)
#db.Modes.insert().execute(prism_mode_gmos_n)

# Adding missing modes
sxd_mode = [{'name': 'SXD',
             'instrument': 'SpeX',
             'telescope': 'IRTF'}]
# db.Instruments.insert().execute(instruments_gnirs)
# db.Telescopes.insert().execute(telescope_gnirs)
# db.Modes.insert().execute(sxd_mode_gnirs)
# db.Modes.insert().execute(sxd_mode)

source_names = data['designation']

# Run once and then comment out:
missing_indices, existing_indices, alt_names_table = sort_sources(db, source_names)

print(len(alt_names_table))
# alt_names_string = 'BDNYC_ingest_spectra_alt_names.vot'
# alt_names_table.write(alt_names_string, format='votable')
# alt_names_table = Table.read(alt_names_string, format='votable')

add_names(db, names_table=alt_names_table)

# TODO: add missing sources
# to_add = data[missing]
# missing_string = 'BDNYC_ingest_spectra_missing.vot'
# to_add.write(missing_string, format='votable')

# For now, just ingest the spectra for sources which are already in the database
existing_string = 'BDNYC_ingest_spectra_existing.vot'
# Run once to write file and then comment out 46-47 and uncomment 48
# existing_data = data[existing]
# existing_data.write(existing_string, format='votable')
existing_data = Table.read(existing_string, format='votable')

for row in existing_data:
    db_name = find_source_in_db(db, row['designation'])

    source_spec_data = db.query(db.Spectra).filter(db.Spectra.c.source == db_name).table()

    if ma.is_masked(row['obs_date']) or row['obs_date'] == '':
        obs_date = None
        continue
    else:
        obs_date = pd.to_datetime(row["obs_date"])

    publication_shortname = row["publication_shortname"]
    if publication_shortname == 'Alle07':
        publication_shortname = 'Alle07a'

    if len(source_spec_data) > 0:  # Spectra data already exists
        # check for duplicate measurement
        dupe_ind = source_spec_data['reference'] == publication_shortname
        if sum(dupe_ind):
            msg = f"Skipping suspected duplicate measurement \n"
            msg2 = f"{source_spec_data[dupe_ind]['source', 'instrument', 'mode', 'observation_date', 'reference']}"
            msg3 = f"{row['designation', 'name.1', 'mode', 'obs_date', 'publication_shortname']} \n"
            logger.info(msg+msg2+msg3)
            continue  # Skip duplicate measurement

    Id = row['id']
    designation = db_name
    spectrum = row['spectrum']
    wavelength_units = row["wavelength_units"]
    flux_units = row["flux_units"]
    wavelength_order = row["wavelength_order"]
    regime = row["regime"]
    comments = row["comments"]
    local_spectrum = row["local_spectrum"]
    telescope_name = row["name"]
    instrument_name = row["name.1"]
    mode = row["mode"]
    row_data = [{'source': designation,
                 'spectrum': spectrum,
                 'local_spectrum': local_spectrum,
                 'regime': regime,
                 'telescope': telescope_name,
                 'instrument': instrument_name,
                 'mode': mode,
                 'observation_date': obs_date,
                 'wavelength_units': wavelength_units,
                 'flux_units': flux_units,
                 'wavelength_order': wavelength_order,
                 'comments': comments,
                 'reference': publication_shortname}]
    logger.debug(row_data)
    db.Spectra.insert().execute(row_data)
