from scripts.ingests.utils import *
import pandas as pd
import numpy.ma as ma

SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files
VERBOSE = False

logger.setLevel(logging.INFO)

db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)

# Read in CSV file with Pandas
df = pd.read_csv('scripts/ingests/BDNYC_spectra3.csv')
data = Table.from_pandas(df)

# Inserting various Libraries for Modes,Telescopes and Instruments
def insert_new_modes():
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
    # Adding missing modes
    sxd_mode = [{'name': 'SXD',
                 'instrument': 'SpeX',
                 'telescope': 'IRTF'}]
    db.Telescopes.insert().execute(telescope_ct)
    db.Telescopes.insert().execute(telescope_mgl)
    db.Telescopes.insert().execute(telescope_gmos)
    db.Telescopes.insert().execute(telescope_gnirs)
    db.Instruments.insert().execute(instruments_gmos)
    db.Instruments.insert().execute(instrument_sl)
    db.Instruments.insert().execute(instruments_mgl)
    db.Instruments.insert().execute(instruments_ct)
    db.Instruments.insert().execute(instrument_gmos_n)
    db.Instruments.insert().execute(instruments_gnirs)
    db.Modes.insert().execute(sxd_mode_ct)
    db.Modes.insert().execute(ll_mode)
    db.Modes.insert().execute(echelle_mode)
    db.Modes.insert().execute(sxd_mode_sl)
    db.Modes.insert().execute(prism_mode_gmos)
    db.Modes.insert().execute(prism_mode_gmos_n)
    db.Modes.insert().execute(sxd_mode_gnirs)
    db.Modes.insert().execute(sxd_mode)

    return


insert_new_modes()

source_names = data['designation']

# Run once and then comment out:
missing_indices, existing_indices, alt_names_table = sort_sources(db, source_names)

# alt_names_string = 'BDNYC_ingest_spectra_alt_names.vot'
# alt_names_table.write(alt_names_string, format='votable')
# alt_names_table = Table.read(alt_names_string, format='votable')

add_names(db, names_table=alt_names_table)

# TODO: add missing sources
# run this at the same time as the adding info one
# [15-, 49-, 89-, 90-, 103-, 146-, 147-, 153-, 160-, 171-, 182-, 206-, 244-, 251-, 274-, 277-, 284-, 387-, 434-, 451-, 580-]

to_add = data[missing_indices]
# missing_string = 'BDNYC_ingest_spectra_missing.vot'
# to_add.write(missing_string, format='votable')
# to_add = Table.read(missing_string, format='votable')

# ingest sources function
ingest_sources(db, to_add['designation'], to_add['ra'], to_add['dec'], to_add['publication_shortname'], comments=to_add['comments'])

# ingest_sources(db, ['2MASS J01330461-1355271'], [23.269208], [-13.924194], ['Kirk10'])
# ingest_sources(db, ['2MASS J08555320-0259207'], [133.97129], [-2.9893056], ['Cruz18'], comments='giant')
# ingest_sources(db, ['2MASS J03180906+4925189'], [49.53775], [49.421917], ['Missing'])
# ingest_sources(db, ['2MASS J03194133+5030451'], [49.922083], [50.5125], ['Missing'])
# ingest_sources(db, ['2MASS J14352035-4255403'], [218.834791667], [-42.9278611111], ['Missing'], comments='giant')
# ingest_sources(db, ['2MASS J05574102-1333264'], [89.420954], [-13.557334], ['Cruz18'], comments='not M')
# ingest_sources(db, ['2MASS J05574229-1333156'], [89.426209], [-13.554361], ['Cruz18'], comments='not M/galaxy')
# ingest_sources(db, ['2MASS J08183325-0036282'], [124.63833], [-0.60777778], ['Cruz07'])
# # ingest_sources(db, ['2MASS J09411157+4254031'], [145.29792], [42.900833], ['Cruz07'])
# ingest_sources(db, ['2MASS J15010693-0531388'], [225.27817], [-5.5277778], ['Missing'], comments='Carbon Star ')
# ingest_sources(db, ['2MASS J20151370-1252571'], [303.80692], [-12.881389], ['Cruz18'], comments='galaxy')
# ingest_sources(db, ['2MASS J23202927+4123415'], [350.12167], [41.394722], ['Cruz18'])
# ingest_sources(db, ['2MASS J12490458-3454080'], [192.26979], [-34.902194], ['Cruz18'], comments='galaxy')
# ingest_sources(db, ['2MASS J15063706+2759544'], [226.65421], [27.998833], ['Cruz18'], comments='galaxy')
# ingest_sources(db, ['2MASS J03435353+2431115'], [55.973051], [24.519865], ['Missing'])
# ingest_sources(db, ['Cl* Melotte   22    NPL      40'], [57.204167], [24.340278], ['Missing'])
# ingest_sources(db, ['2MASS J09411157+4254031'], [145.29792], [42.900833], ['Cruz07'])
# Line 118 seems to have a missing reference
# ingest_sources(db, ['2MASS J14044948-3159330'], [211.2058755], [-31.992516])
# ingest_sources(db, ['2MASS J09171104-1650010'], [139.296], [-16.833611], ['Kirk'])
# ingest_sources(db, ['2MASS J15010693-0531388'], [225.27817], [-5.5277778], ['Missing'], comments='Carbon Star')
# Line 122 seems to have a missing reference
# ingest_sources(db, ['LP  738-14 B'], [207.0103434], [-13.7367212])


# For now, just ingest the spectra for sources which are already in the database
existing_string = 'BDNYC_ingest_spectra_existing.vot'
# Run once to write file and then comment out 46-47 and uncomment 48
# existing_data = data[existing_indices]
# existing_data.write(existing_string, format='votable')
existing_data = Table.read(existing_string, format='votable')

n_skipped = 0
n_added = 0

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
            # TODO: use observation date to check for duplicate
            msg = f"Skipping suspected duplicate measurement \n"
            msg2 = f"{source_spec_data[dupe_ind]['source', 'instrument', 'mode', 'observation_date', 'reference']}"
            msg3 = f"{row['designation', 'name.1', 'mode', 'obs_date', 'publication_shortname']} \n"
            logger.warning(msg+msg2+msg3)
            n_skipped += 1
            continue  # Skip duplicate measurement

    # TODO: remove unecessary variable definitions
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
    # TODO: make into try/except
    db.Spectra.insert().execute(row_data)
    n_added += 1

logger.info(f"Spectra added: {n_added}")
logger.info(f"Spectra skipped: {n_skipped}")