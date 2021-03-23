from astrodbkit2.astrodb import create_database
from astrodbkit2.astrodb import Database
from astropy.table import Table
import numpy as np
from astropy.coordinates import SkyCoord
import astropy.units as u
from astroquery.simbad import Simbad
import warnings
warnings.filterwarnings("ignore", module='astroquery.simbad')

connection_string = 'sqlite:///../../SIMPLE.db'  # SQLite
create_database(connection_string)
db = Database(connection_string)
db.load_database('../../data')

# load table of sources to ingest into an astropy table
ingest_file = ("ATLAS_table.vot")
ingest_table = Table.read(ingest_file)

# identify the relevant columns in the ingest table
ingest_names = ingest_table['Name']
ingest_RA = ingest_table['_RAJ2000']
ingest_dec = ingest_table['_DEJ2000']

#Make sure all source names are Simbad resolvable:
def check_names_simbad(ingest_table, verbose = False):
    verboseprint = print if verbose else lambda *a, **k: None

    resolved_name = []
    n_sources = len(ingest_table)
    n_name_matches = 0
    n_selections = 0
    n_nearby = 0
    n_notfound = 0

    for ingest_source in range(n_sources):
        #Query Simbad for identifiers matching the ingest source name
        identifer_result_table = Simbad.query_object(ingest_names[ingest_source], verbose=False)

        # Successfully resolved one matching identifier in Simbad
        if identifer_result_table is not None and len(identifer_result_table) == 1:
            # Add the Simbad resolved identifier ot the resolved_name list and deals with unicode
            if isinstance(identifer_result_table['MAIN_ID'][0],str):
                resolved_name.append(identifer_result_table['MAIN_ID'][0])
            else:
                resolved_name.append(identifer_result_table['MAIN_ID'][0].decode())
            verboseprint(resolved_name[ingest_source], "Found name match in Simbad")
            n_name_matches = n_name_matches + 1

        # If no identifier match found, search within 2 arcseconds of coords for a Simbad obingest_sourceect
        else:
            coord_result_table = Simbad.query_region(SkyCoord(ingest_RA[ingest_source], ingest_dec[ingest_source], unit=(u.deg, u.deg), frame='icrs'), radius='2s', verbose=verbose)

            # If more than one match found within 2 arcseconds, query user for selection and append to resolved_name
            if len(coord_result_table) > 1:
                for i, name in enumerate(coord_result_table['MAIN_ID']):
                    print(f'{i}: {name}')
                selection = int(input('Choose \n'))
                if isinstance(coord_result_table['MAIN_ID'][selection],str):
                    resolved_name.append(coord_result_table['MAIN_ID'][selection])
                else:
                    resolved_name.append(coord_result_table['MAIN_ID'][selection].decode())
                verboseprint(resolved_name[ingest_source], "you selected")
                n_selections = n_selections + 1

            # If there is only one match found, accept it and append to the resolved_name list
            elif len(coord_result_table) == 1:
                if isinstance(coord_result_table['MAIN_ID'][0],str):
                    resolved_name.append(coord_result_table['MAIN_ID'][0])
                else:
                    resolved_name.append(coord_result_table['MAIN_ID'][0].decode())
                verboseprint(resolved_name[ingest_source], "only result nearby in Simbad")
                n_nearby = n_nearby + 1

            # If no match is found in Simbad, use the name in the ingest table
            else:
                resolved_name.append(ingest_names[ingest_source])
                verboseprint("coord search failed")
                n_notfound = n_notfound + 1

    # Report how many find via which methods
    print("Names Found:", n_name_matches)
    print("Names Selected", n_selections)
    print("Names Found", n_nearby)
    print("Not found", n_notfound)

    n_found = n_notfound + n_name_matches + n_selections + n_nearby
    print('problem' if n_found != n_sources else (n_sources, 'names') )

    return resolved_name

check_names_simbad(ingest_table, verbose = True)