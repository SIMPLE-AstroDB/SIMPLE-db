# # Ingest Fundamental Paramters from Sanghi23 

# #SIMPLE & Astrodb Packages ---
# import logging
# from astrodb_utils import load_astrodb, AstroDBError
# from astrodb_utils.sources import find_source_in_db, ingest_names
# from simple import REFERENCE_TABLES
# #Additional Packages --
# import os
# import pandas as pd

# # Set up logging
# logger = logging.getLogger("astrodb_utils.beiler24") 
# logger.setLevel(logging.INFO) 
# logger.info(f"script using {logger.name}")
# logger.info(f"Logger level: {logging.getLevelName(logger.getEffectiveLevel()) }")

# # Load Database
# recreate_db = True
# save_db = True

# SCHEMA_PATH = "simple/schema.yaml"   
# db = load_astrodb(
#     "SIMPLE.sqlite",
#     recreatedb=recreate_db,  
#     reference_tables=REFERENCE_TABLES, 
#     felis_schema=SCHEMA_PATH)


# # Ingest Fundamental Parameters
# #evo_cols = ["name", "name_simbadable", "ra_j2000_formula", "dec_j2000_formula", "teff_evo", "teff_evo_err", "radius_evo", "radius_evo_err", "mass_evo", "mass_evo_err", "logg_evo", "logg_evo_err"]
# evo_table = pd.read_csv('FundamentalPropertiesbyEvoModel.csv')
# modeled_parameters_evo_ingest_dict = [
#     {'sourceName': row['name'],
#     'sourceAltName': row['name_simbadable'],
#     'ra': row['ra_j2000_formula'], 
#     'dec': row['dec_j2000_formula'],
#     'Radius': {'value': row['radius_evo'], 'value_error': row['radius_evo_err'], 'parameter': "radius", 'unit': 'R_jup', "model": "evolutionary"},
#     'log_g': {'value': row['logg_evo'], 'value_error': row['logg_evo_err'], 'parameter': "log g", 'unit': 'dex', "model": "evolutionary"},
#     'T_eff': {'value': row['teff_evo'], 'value_error': row['teff_evo_err'], 'parameter': "T eff", 'unit': 'K', "model": "evolutionary"},
#     'Mass': {'value': row['mass_evo'], 'value_error': row['mass_evo_err'], 'parameter': "mass", 'unit': 'M_jup', "model": "evolutionary"},
#     'reference': "Sang23"
#     } for _, row in evo_table.iterrows()
# ]

# #atmo_cols = ["name", "name_simbadable", "ra_j2000_formula", "dec_j2000_formula", "teff_atmo", "radius_atmo", "logg_atmo", "log_lbol_lsun", "log_lbol_lsun_err"]
# atmo_table = pd.read_csv('FundamentalPropertiesbyAtmoModel.csv')
# modeled_parameters_atmo_ingest_dict = [
#     {'source': atmo["name"],
#      'ra': atmo["ra_j2000_formula"], 'dec': atmo["dec_j2000_formula"],
#      'Radius': {'value': atmo["radius_atmo"], 'parameter': "radius", 'unit': 'R_jup', "model": "atmospheric"},
#      'log_g': {'value': atmo["logg_atmo"], 'parameter': "log g", 'unit': 'dex', "model": "atmospheric"},
#      'T_eff': {'value': atmo["teff_atmo"], 'parameter': "T eff", 'unit': 'K', "model": "atmospheric"},
#      'log_L_bol': {'value': atmo["log_lbol_lsun"], 'value_error': atmo["log_lbol_lsun_err"], 'parameter': "L bol", 'unit': 'dex', "model": "atmospheric"},
#      'reference': "Sang23"  #confirm this
#      } for _, atmo in atmo_table.iterrows() 
#     ]

# def find_source_name(db, source_name, source_alt_name, ra, dec, reference):
#     found_source = find_source_in_db(db, source_name, ra=ra, dec=dec, ra_col_name = "ra", dec_col_name = "dec")
#     if len(found_source) > 0:
#         source_name = found_source[0]
    
#     else:
#         print(f"Source {source_name} not found in database. Trying with Simbad name")
#         found_source = find_source_in_db(db, source_alt_name, ra=ra, dec=dec, ra_col_name = "ra", dec_col_name = "dec")
#         if len(found_source) > 0:
#             print(f"Found with simbad name {source_alt_name}. Ingesting alternative name.")
#             ingest_names(db, found_source[0], source_name)
#             source_name = found_source[0]
#         else:
#             print(f"Source {source_name} not found. Skipping for now.")
#             source_name = None


#     print(f'Found correct source name, saving as {source_name}')
#     return source_name



# for source in modeled_parameters_evo_ingest_dict:
#     found_name = find_source_name(db, source['sourceName'], source['sourceAltName'], source['ra'], source['dec'], source['reference'])
#     source['source'] = found_name
#     if source['source'] is None:
#         print(f"Source {source['sourceName']} not found in database. Skipping for now.")


# # Ingest Parameters ----
# def ingest_parameters(mode, source):
#     """
#     Ingest parameters into the database.
#     """
#     # Check if source exists in the database
#     source_exists = db.Sources.select().where(db.Sources.c.source == source['source']).fetchone()
#     if not source_exists:
#         print(f"Source {source['source']} not found in the database. Skipping.")
#         return

#     # Ingest parameters
#     value_types_evo = ['Radius', 'log_g', 'T_eff', 'Mass']
#     value_types_atmo = ['Radius', 'log_g', 'T_eff', 'log_L_bol']


#     for value_type in value_types_evo:
#         if source[value_type]['value'] is not None:
#             db.ModeledParameters.insert().values({'source': source['source'], **source[value_type], 'reference': "Sang23"})
#             print(f"Ingested {value_type} for {source['source']}")


#     with db.engine.connect() as conn:
#         #Adding Evo Model Parameters
#         for row in source:
#             if row["source"] is not None:
#                 source_exists = conn.execute(db.Sources.select().where(db.Sources.c.source == row["source"])).fetchone()
                
#                 if source_exists:
                    
#                     if mode == "evolutionary":
#                         for value_type in value_types_evo:
#                             if row[value_type]['value'] is not None:
#                                 conn.execute(db.ModeledParameters.insert().values({'source': row['source'], **row[value_type], 'reference': "Sang23"}))
#                     elif mode == "atmospheric":
#                         for value_type in value_types_atmo:
#                             if row[value_type]['value'] is not None:
#                                 conn.execute(db.ModeledParameters.insert().values({'source': row['source'], **row[value_type], 'reference': "Sang23"}))
#         conn.commit()  

# #Call Functions ----
# ingest_parameters(mode = "evolutionary", 
#                   source=modeled_parameters_evo_dict)

# ingest_parameters(mode = "atmospheric", 
#                   source=modeled_parameters_atmo_dict)

# # Save to Database, Writes the JSON Files
# if save_db: 
#     db.save_database(directory="data/")


# Ingest Fundamental Parameters from Sanghi23 

# SIMPLE & Astrodb Packages
import logging
from astrodb_utils import load_astrodb, AstroDBError
from astrodb_utils.sources import find_source_in_db, ingest_names
from simple import REFERENCE_TABLES

# Additional Packages
import os
import pandas as pd
import numpy as np

# Set up logging
logger = logging.getLogger("astrodb_utils.sanghi23")  # Updated logger name for clarity
logger.setLevel(logging.INFO)
logger.info(f"Script using {logger.name}")
logger.info(f"Logger level: {logging.getLevelName(logger.getEffectiveLevel())}")

# Load Database
recreate_db = True
save_db = True

SCHEMA_PATH = "simple/schema.yaml"   
db = load_astrodb(
    "SIMPLE.sqlite",
    recreatedb=recreate_db,  
    reference_tables=REFERENCE_TABLES, 
    felis_schema=SCHEMA_PATH
)

# Reference for all parameters
ref = "Sang23"

# Load data files
evo_table = pd.read_csv('FundamentalPropertiesbyEvoModel.csv')
atmo_table = pd.read_csv('FundamentalPropertiesbyAtmoModel.csv')

def find_source_name(db, source_name, ra, dec):
    logger.info(f"Looking for source {source_name} in database")

    # Try to find the source by primary name
    found_source = find_source_in_db(db, source_name, ra=ra, dec=dec, ra_col_name="ra", dec_col_name="dec")
    if len(found_source) > 0:
        logger.info(f"Found source {source_name} in database")
        return found_source[0]
    
    logger.warning(f"Source {source_name} not found in database.")
    return None

def create_dict(row, model_type):
    if model_type == "evolutionary":
        return {
            'sourceName': row['name'],
            'sourceAltName': row['name_simbadable'],
            'ra': row['ra_j2000_formula'],
            'dec': row['dec_j2000_formula'],
            'parameters': {
                'Radius': {
                    'value': row['radius_evo'], 
                    'value_error': row['radius_evo_err'], 
                    'parameter': "radius", 
                    'unit': 'R_jup', 
                    'model': "evolutionary"
                },
                'log_g': {
                    'value': row['logg_evo'], 
                    'value_error': row['logg_evo_err'], 
                    'parameter': "log g", 
                    'unit': 'dex', 
                    'model': "evolutionary"
                },
                'T_eff': {
                    'value': row['teff_evo'], 
                    'value_error': row['teff_evo_err'], 
                    'parameter': "T eff", 
                    'unit': 'K', 
                    'model': "evolutionary"
                },
                'Mass': {
                    'value': row['mass_evo'], 
                    'value_error': row['mass_evo_err'], 
                    'parameter': "mass", 
                    'unit': 'M_jup', 
                    'model': "evolutionary"
                }
            }
        }
    
    elif model_type == "atmospheric":
        return {
            'sourceName': row['name'],
            'sourceAltName': row.get('name_simbadable', row['name']),  # Fallback if not present
            'ra': row['ra_j2000_formula'],
            'dec': row['dec_j2000_formula'],
            'parameters': {
                'Radius': {
                    'value': row['radius_atmo'],
                    'value_error': None, 
                    'parameter': "radius", 
                    'unit': 'R_jup', 
                    'model': "atmospheric"
                },
                'log_g': {
                    'value': row['logg_atmo'],
                    'value_error': None, 
                    'parameter': "log g", 
                    'unit': 'dex', 
                    'model': "atmospheric"
                },
                'T_eff': {
                    'value': row['teff_atmo'],
                    'value_error': None, 
                    'parameter': "T eff", 
                    'unit': 'K', 
                    'model': "atmospheric"
                },
                'log_L_bol': {
                    'value': row['log_lbol_lsun'], 
                    'value_error': row['log_lbol_lsun_err'], 
                    'parameter': "L bol", 
                    'unit': 'dex', 
                    'model': "atmospheric"
                }
            }
        }
    else:
        logger.error(f"Unknown model type: {model_type}")
        return None

def ingest_parameters(db, param_list, reference):
    with db.engine.connect() as conn:
        for data in param_list:
            # Find the database source name
            source_name = find_source_name(
                db, 
                data['sourceName'], 
                data['ra'], 
                data['dec']
            )
            if source_name is None:
                logger.warning(f"Source {data['sourceName']} not found. Skipping.")
                continue
            
            # Insert each parameter
            for param_type, param_data in data['parameters'].items():
                # Skip if value is None or NaN
                if param_data['value'] is None:
                    logger.info(f"Skipping {param_type} for {source_name}: NaN value found")
                    continue
                    
                # Prepare values for insertion
                insert_data = {
                    'source': source_name,
                    'parameter': param_data['parameter'],
                    'value': param_data['value'],
                    'value_error': param_data['value_error'],
                    'unit': param_data['unit'],
                    'model': param_data['model'],
                    'reference': reference
                }
                
                # Insert into ModeledParameters table
                try:
                    conn.execute(db.ModeledParameters.insert().values(insert_data))
                    logger.info(f"Inserted {param_type} for {source_name}")
                except Exception as e:
                    logger.error(f"Error inserting {param_type} for {source_name}: {e}")
        
        # Commit all changes at once
        conn.commit()
        logger.info("All parameters committed to database")


# Prepare data for ingestion
evo_data = [create_dict(row, "evolutionary") for _, row in evo_table.iterrows()]
atmo_data = [create_dict(row, "atmospheric") for _, row in atmo_table.iterrows()]

# Ingest the data
logger.info("Ingesting evolutionary model parameters")
ingest_parameters(db, evo_data, ref)

logger.info("Ingesting atmospheric model parameters")
ingest_parameters(db, atmo_data, ref)

# Save to Database, Writes the JSON Files
if save_db:
    logger.info("Saving database")
    db.save_database(directory="data/")