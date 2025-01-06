#SIMPLE & Astrodb Packages
from astrodb_utils import ingest_publication, ingest_source, load_astrodb, find_source_in_db
from simple.utils.spectra import * 
from simple.schema import *
from simple.schema import REFERENCE_TABLES

#Additional Packages
import pandas as pd


db = load_astrodb("SIMPLE.sqlite", recreatedb=False, reference_tables=REFERENCE_TABLES)

# Ingest New Sources 
ingest_publication(
    db,
    doi = "10.3847/1538-4357/ad0b12")

newsources = pd.read_csv('unavailable_sources.csv') # Read in new sources
for _, source in newsources.iterrows():
    ingest_source(
        db,
        source = source['name'],
        ra = source['ra_j2000_formula'],
        dec = source['dec_j2000_formula'],
        reference = source['reference'],
    )
    print(f"Source {source['name']} ingested")

## Ingest New Fundamental Parameters
evo_cols = ["name_simbadable", "ra_j2000_formula", "dec_j2000_formula", "teff_evo", "teff_evo_err", "radius_evo", "radius_evo_err", "mass_evo", "mass_evo_err", "logg_evo", "logg_evo_err"]
evo_table = pd.read_csv('FundamentalPropertiesbyEvoModel.csv')

modeled_parameters_evo_ingest_dict = [
    {'source': row['name_simbadable'],
     'ra': row['ra_j2000_formula'], 
     'dec': row['dec_j2000_formula'],
     'Radius': {'value': row['radius_evo'], 'value_error': row['radius_evo_err'], 'parameter': "radius", 'unit': 'R_jup', "model": "evolutionary"},
     'log_g': {'value': row['logg_evo'], 'value_error': row['logg_evo_err'], 'parameter': "log g", 'unit': 'dex', "model": "evolutionary"},
     'T_eff': {'value': row['teff_evo'], 'value_error': row['teff_evo_err'], 'parameter': "T eff", 'unit': 'K', "model": "evolutionary"},
     'Mass': {'value': row['mass_evo'], 'value_error': row['mass_evo_err'], 'parameter': "mass", 'unit': 'M_jup', "model": "evolutionary"},
     'reference': "Sang23"
     } for _, row in evo_table.iterrows()
]


for i, source_dict in enumerate(modeled_parameters_evo_ingest_dict):  
    found_source = find_source_in_db(db, source_dict['source'], ra=source_dict['ra'], dec=source_dict['dec'])
    source_dict['source'] = found_source[0]

value_types = ['Radius', 'log_g', 'T_eff', 'Mass']
with db.engine.connect() as conn:
    for row in modeled_parameters_evo_ingest_dict:
        for value_type in value_types:
            if row[value_type]['value'] is not None:  
                conn.execute(db.ModeledParameters.insert().values({'source': row['source'], **row[value_type]}))

    conn.commit()  

# Save to Database
db.save_database(directory="data/")