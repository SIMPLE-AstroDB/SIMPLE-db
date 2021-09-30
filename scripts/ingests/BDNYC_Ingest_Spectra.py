from scripts.ingests.utils import *
import pandas as pd

SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = False  # recreates the .db file from the data files
VERBOSE = False

db = load_simpledb('SIMPLE.db', RECREATE_DB=RECREATE_DB)

# Read in CSV file with Pandas
df = pd.read_csv('scripts/ingests/BDNYC_spectra2.csv')
data = Table.from_pandas(df)
# Extracting Columns from CSV


for row in data[0:10]:
    Id = row['id']
    designation = row['designation']
    spectrum = row['spectrum']
    wavelength_units = row["wavelength_units"]
    flux_units = row["flux_units"]
    wavelength_order = row["wavelength_order"]
    regime = row["regime"]
    publication_shortname = row["publication_shortname"]
    obs_date = pd.to_datetime(row["obs_date"])
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
