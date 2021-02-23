# Script to ingest Cloud Atlas Manjavacas+2019
# Using Clémence Fontanive's previous code as guide

#------------------------------------------------------------------------------------------------

from astrodbkit2.astrodb import create_database
from astrodbkit2.astrodb import Database
#from simple.schema import *
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
#db.load_database('../../data')


# load table of sources to ingest



input_file = ("ATLAS_table.vot")
ATLAS=Table.read(input_file)

# Inital attempt, fixing object name flag issue with simbad

'''
#simbad search fix
# PSO J318.5338-22.8603
Ind = ATLAS['Name'] == 'PSO J318.5-22'
ATLAS['Name'][Ind]='PSO J318.5338-22.8603'

Ind = ATLAS['Name'] == 'Luh 16AB'
ATLAS['Name'][Ind]='Luhman 16'

Ind = ATLAS['Name'] == '2MASS J2339101+135230'
ATLAS['Name'][Ind]='2MASSI J2339101+135230'

Ind = ATLAS['Name'] == '2MASS J1110100+0116130'
ATLAS['Name'][Ind]='2MASS J11101001+0116130'

Ind = ATLAS['Name'] == '2MASS J2228288-4310262'
ATLAS['Name'][Ind]='2MASS J22282889-4310262'

Ind = ATLAS['Name'] == '2MASS J0817300-6155158'
ATLAS['Name'][Ind]='2MASS J08173001-6155158'

Ind = ATLAS['Name'] == 'S Ori 70'
ATLAS['Name'][Ind]='[BZR99] S Ori 70'


Ind = ATLAS['Name'] == 'S Ori 70'
ATLAS['Name'][Ind]='[BZR99] S Ori 70'


Ind = ATLAS['Name'] == 'S Ori 70'
ATLAS['Name'][Ind]='[BZR99] S Ori 70'


Ind = ATLAS['Name'] == 'S Ori 70'
ATLAS['Name'][Ind]='[BZR99] S Ori 70'


Ind = ATLAS['Name'] == 'S Ori 70'
ATLAS['Name'][Ind]='[BZR99] S Ori 70'

'''

#attempt 2, fixing simbad flag issue:

resolved_name = []

for j in range(len(ATLAS)):
	identifer_result_table = Simbad.query_object(ATLAS['Name'][j], verbose=False)
	if identifer_result_table is not None and len(identifer_result_table) == 1:
		# Successfully identified
		if isinstance(identifer_result_table['MAIN_ID'][0],str):
			resolved_name.append(identifer_result_table['MAIN_ID'][0])
		else:
			resolved_name.append(identifer_result_table['MAIN_ID'][0].decode())
	else:
		coord_result_table = Simbad.query_region(SkyCoord(ATLAS['_RAJ2000'][j],
			ATLAS['_DEJ2000'][j], unit=(u.deg, u.deg), frame='icrs'), radius='2s', verbose=False)

		if len(coord_result_table) > 1:
			for i, name in enumerate(coord_result_table['MAIN_ID']):
				print(f'{i}: {name}')
			selection = int(input('Choose \n'))
			#print(selection)
			#print(coord_result_table[selection])
			if isinstance(coord_result_table['MAIN_ID'][selection],str):
				resolved_name.append(coord_result_table['MAIN_ID'][selection])
			else:
				resolved_name.append(coord_result_table['MAIN_ID'][selection].decode())

		elif len(coord_result_table) == 1:
			#print(coord_result_table[0])
			if isinstance(coord_result_table['MAIN_ID'][0],str):
				resolved_name.append(coord_result_table['MAIN_ID'][0])
			else:
				resolved_name.append(coord_result_table['MAIN_ID'][0].decode())

		else:
			print("coord search failed")
			resolved_name.append(ATLAS['Name'][j])



# find sources already in database
existing_sources = []
missing_sources = []
db_names = []
for i,name in enumerate(resolved_name):
	if len(db.search_object(name,resolve_simbad=True, verbose=False)) != 0:
		existing_sources.append(i)
		db_names.append(db.search_object(name,resolve_simbad=True)[0].source)

	else:
		missing_sources.append(i)
		db_names.append(resolved_name[i])
