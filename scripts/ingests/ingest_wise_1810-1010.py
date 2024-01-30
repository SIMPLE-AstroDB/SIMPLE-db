# Script ingest first data into companion tables

from scripts.ingests.ingest_utils import *
from scripts.ingests.utils import *
from astropy.table import Table
import astropy.units as u
from astropy.coordinates import Angle
   

SAVE_DB = False  # True: save the data files(json) in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files
# LOAD THE DATABASE
db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)


# Ingest wise_1810-1010 and its reference
#doi- 10.3847/1538-4357/ab9a40  and  10.1051/0004-6361/202243516
#bibcode of coordinates reference- 2020ApJ...898...77S  and  2022A&A...663A..84L
def add_publication(db):

    ingest_publication(db, doi = "10.3847/1538-4357/ab9a40")

    ingest_publication(db, doi = "10.1051/0004-6361/202243516")

def add_sources(db):

    ra_1810= Angle("18 10 06.18", u.hour).degree  
    dec_1010=Angle("-10 10 00.5", u.degree).degree

    ingest_sources(db, ["CWISEP J181006.00-101001.1"], references=["Schn20"], 
               ras= [ra_1810], 
               decs=[dec_1010], 
               search_db=False)


    #  Ingest other name for Wise 1810-1010 (one used in SIMBAD)
    #  code from deprecated utils does not work
    ingest_names(db, 'CWISEP J181006.00-101001.1', 'Wise 1810-1010')

def add_parallaxes(db):
    ingest_parallaxes(db, 
                  sources = ["CWISEP J181006.00-101001.1"], 
                  plxs = [112.5], 
                  plx_errs = [8.1], 
                  plx_refs = "Lodi22", 
                  comments=None)

def add_proper_motions(db):
    ingest_proper_motions(db, sources = ["CWISEP J181006.00-101001.1"], 
                          pm_ras = [-1027], 
                          pm_ra_errs = [3.5],
                          pm_decs = [-246.4], 
                          pm_dec_errs = [3.6], 
                          pm_references = "Schn20")


#Ingest Functions for Modeled Parameters (Gravity, Metallicity, Radius, Mass)
#Creating list of dictionaries for each value formatted for modeled parameters
def add_modeled_parameters_dict(db):
    ingest_modeled_parameters = [{ 
                                    'Gravity':
                                       {'value': 5.0,
                                        'value_error': 0.25,
                                        'parameter': "log g", 
                                        'unit': 'dex',
                                        'reference': "Lodi22"}, 

                                   'Metallicity':
                                       {'value': -1.5,
                                        'value_error': 0.5,
                                        'parameter': "metallicity", 
                                        'unit': 'dex',
                                        'reference': "Lodi22"},

                                   'Radius':
                                       {'value': 0.067,
                                        'value_error': 0.032, #Highest value error was picked between +0.032 & -0.020 listed
                                        'parameter': "radius", 
                                        'unit': 'R_jup',
                                        'reference': "Lodi22"},

                                   'Mass':
                                       {'value': 17,
                                        'value_error': null, #Highest value error was picked between +56 & -12 listed
                                        'parameter': "mass", 
                                        'unit': 'M_jup',
                                        'comments': "17[+56, -12]",
                                        'reference': "Lodi22"},

                                    'Effective temperature':
                                       {'value': 800,
                                        'value_error': 100,
                                        'parameter': "T eff", 
                                        'unit': 'K',
                                        'reference': "Lodi22"}   
                                 }]   
                                    
    source = "CWISEP J181006.00-101001.1"
    #value_types = ['Gravity', 'Metallicity', 'Radius', 'Mass', 'Effective temperature']
    value_types = ['Effective temperature']
    with db.engine.connect() as conn:
        for row in ingest_modeled_parameters:

            for value_type in value_types:
                if row[value_type]['value'] is not None: # Checking that there's a value
                    conn.execute(db.ModeledParameters.insert().values({'source': source, 'reference': 'Lodi22', **row[value_type]}))

                conn.commit() 

#Call functions/ Comment out when not needed
#add_publication(db)
#add_sources(db)
#add_parallaxes(db)
#add_proper_motions(db)
add_modeled_parameters_dict(db)
db.inventory('CWISEP J181006.00-101001.1', pretty_print=True)

# WRITE THE JSON FILES
if SAVE_DB:
    db.save_database(directory='data/') 