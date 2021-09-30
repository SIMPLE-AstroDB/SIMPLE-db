from scripts.ingests.utils import *
import pandas as pd
import numpy.ma as ma

SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = False  # recreates the .db file from the data files
VERBOSE = False

db = load_simpledb('SIMPLE.db', RECREATE_DB=RECREATE_DB)

# Read in CSV file with Pandas
df = pd.read_csv('scripts/ingests/BDNYC_spectra2.csv')
data = Table.from_pandas(df)
# Extracting Columns from CSV
sxd_mode = [{'name': 'SXD',
             'instrument': 'SpeX',
             'telescope': 'IRTF'}]

#db.Modes.insert().execute(sxd_mode)

for row in data:
    db_name = row['designation']
    source_spec_data = db.query(db.Spectra).filter(db.Spectra.c.source == db_name).table()
    if len(source_spec_data) > 0:  # Spectra data already exists
        # check for duplicate measurement
        dupe_ind = source_spec_data['reference'] == row['publication_shortname']
        if sum(dupe_ind):
            print("Duplicate measurement\n", source_spec_data[dupe_ind], '\n')
            continue
    if ma.is_masked(row['obs_date']):
        obs_date = None
        continue
    else:
        obs_date = pd.to_datetime(row["obs_date"])
    publication_shortname = row["publication_shortname"]
    if publication_shortname == 'Alle07':
        publication_shortname = 'Alle07a'
    Id = row['id']
    designation = row['designation']
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
    print(row_data)
    db.Spectra.insert().execute(row_data)
