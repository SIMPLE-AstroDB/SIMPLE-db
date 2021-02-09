# Script to ingest Cloud Atlas Manjavacas+2019
# Using Clémence Fontanive's previous code as guide

#------------------------------------------------------------------------------------------------

from astrodbkit2.astrodb import create_database
from astrodbkit2.astrodb import Database
#from simple.schema import *
from astropy.table import Table
connection_string = 'sqlite:///../../SIMPLE.db'  # SQLite
create_database(connection_string)
db = Database(connection_string)
db.load_database('../../data')
import numpy as np
# load table of sources to ingest

input_file = ("ATLAS_table.vot")
ATLAS=Table.read(input_file)

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




# find sources already in database
existing_sources = []
missing_sources = []
db_names = []
for i,name in enumerate(ATLAS['Name']):
	if len(db.search_object(name,resolve_simbad=False)) != 0:
		existing_sources.append(i)
		db_names.append(db.search_object(name,resolve_simbad=False)[0].source)
	else:
		missing_sources.append(i)
		db_names.append(ATLAS['Name'][i])


