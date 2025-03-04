#SIMPLE & Astrodb Packages
# from astrodb_utils.publications import ingest_publication
from astrodb_utils import load_astrodb, logger
from astrodb_utils.publications import ingest_publication
from astrodb_utils.sources import ingest_source, find_source_in_db, ingest_names
# from simple.utils.spectra import * 
from simple.schema import *
from simple.schema import REFERENCE_TABLES

#Additional Packages
import pandas as pd

# logger.setLevel("DEBUG")

db = load_astrodb("SIMPLE.sqlite", recreatedb=True,  reference_tables=REFERENCE_TABLES)

# Ingest New Sources 
# ingest_publication(
#     db,
#     doi = "10.3847/1538-4357/ad0b12")

newsources = pd.read_csv('missing_sources.csv') # Read in new sources
for _, source in newsources.iterrows():
    try :
        ingest_source(
            db,
            source = source['name'],
            ra = source['ra_j2000_formula'],
            dec = source['dec_j2000_formula'],
            reference = source['reference'],
        )
        print(f"Source {source['name']} ingested")
    except: 
        print(f"Source {source['name']} already in database")
        continue

## Ingest New Fundamental Parameters
#evo_cols = ["name", "name_simbadable", "ra_j2000_formula", "dec_j2000_formula", "teff_evo", "teff_evo_err", "radius_evo", "radius_evo_err", "mass_evo", "mass_evo_err", "logg_evo", "logg_evo_err"]
evo_table = pd.read_csv('FundamentalPropertiesbyEvoModel.csv')

modeled_parameters_evo_ingest_dict = [
    {'sourceName': row['name'],
     'sourceAltName': row['name_simbadable'],
     'ra': row['ra_j2000_formula'], 
     'dec': row['dec_j2000_formula'],
     'Radius': {'value': row['radius_evo'], 'value_error': row['radius_evo_err'], 'parameter': "radius", 'unit': 'R_jup', "model": "evolutionary"},
     'log_g': {'value': row['logg_evo'], 'value_error': row['logg_evo_err'], 'parameter': "log g", 'unit': 'dex', "model": "evolutionary"},
     'T_eff': {'value': row['teff_evo'], 'value_error': row['teff_evo_err'], 'parameter': "T eff", 'unit': 'K', "model": "evolutionary"},
     'Mass': {'value': row['mass_evo'], 'value_error': row['mass_evo_err'], 'parameter': "mass", 'unit': 'M_jup', "model": "evolutionary"},
     'reference': "Sang23"
     } for _, row in evo_table.iterrows()
]
#atmo_cols = ["name", "name_simbadable", "ra_j2000_formula", "dec_j2000_formula", "teff_atmo", "radius_atmo", "logg_atmo", "log_lbol_lsun", "log_lbol_lsun_err"]
atmo_table = pd.read_csv('FundamentalPropertiesbyAtmoModel.csv')
modeled_parameters_atmo_ingest_dict = [
    {'source': atmo["name"],
     'ra': atmo["ra_j2000_formula"], 'dec': atmo["dec_j2000_formula"],
     'Radius': {'value': atmo["radius_atmo"], 'parameter': "radius", 'unit': 'R_jup', "model": "atmospheric"},
     'log_g': {'value': atmo["logg_atmo"], 'parameter': "log g", 'unit': 'dex', "model": "atmospheric"},
     'T_eff': {'value': atmo["teff_atmo"], 'parameter': "T eff", 'unit': 'K', "model": "atmospheric"},
     'log_L_bol': {'value': atmo["log_lbol_lsun"], 'value_error': atmo["log_lbol_lsun_err"], 'parameter': "L bol", 'unit': 'dex', "model": "atmospheric"},
     'reference': "Sang23"  #confirm this
     } for _, atmo in atmo_table.iterrows() 
    ]

def find_source_name(db, source_name, source_alt_name, ra, dec, reference):
    found_source = find_source_in_db(db, source_name, ra=ra, dec=dec, ra_col_name = "ra", dec_col_name = "dec")
    if len(found_source) > 0:
        source_name = found_source[0]
    
    else:
        print(f"Source {source_name} not found in database. Trying with Simbad name")
        found_source = find_source_in_db(db, source_alt_name, ra=ra, dec=dec, ra_col_name = "ra", dec_col_name = "dec")
        if len(found_source) > 0:
            print(f"Found with simbad name {source_alt_name}. Ingesting alternative name.")
            ingest_names(db, found_source[0], source_name)
            source_name = found_source[0]
        else:
            print(f"Source {source_name} not found. Skipping for now.")
            source_name = None


    print(f'Found correct source name, saving as {source_name}')
    return source_name

for source in modeled_parameters_evo_ingest_dict:
    found_name = find_source_name(db, source['sourceName'], source['sourceAltName'], source['ra'], source['dec'], source['reference'])
    source['source'] = found_name
    if source['source'] is None:
        with open('missing_sources.txt', 'a') as f:
            f.write(f"{source['sourceName']}\n")

value_types_evo = ['Radius', 'log_g', 'T_eff', 'Mass']
value_types_atmo = ['Radius', 'log_g', 'T_eff', 'log_L_bol']

with db.engine.connect() as conn:
    #Adding Evo Model Parameters
    for row in modeled_parameters_evo_ingest_dict:
        if row["source"] is not None:
            source_exists = conn.execute(db.Sources.select().where(db.Sources.c.source == row["source"])).fetchone()
            if source_exists:
                for value_type in value_types_evo:
                    if row[value_type]['value'] is not None:
                        conn.execute(db.ModeledParameters.insert().values({'source': row['source'], **row[value_type], 'reference': "Sang23"}))

    #Adding Atmo Model Parameters
    for row in modeled_parameters_atmo_ingest_dict:
        if row["source"] is not None:
            source_exists = conn.execute(db.Sources.select().where(db.Sources.c.source == row["source"])).fetchone()
            if source_exists:
                for value_type in value_types_atmo:
                    if row[value_type]['value'] is not None:
                        conn.execute(db.ModeledParameters.insert().values({'source': row['source'], **row[value_type], 'reference': "Sang23"}))

    conn.commit()  

# Save to Database
db.save_database()
