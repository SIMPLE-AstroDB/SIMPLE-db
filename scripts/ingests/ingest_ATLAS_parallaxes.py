import sys
from sqlalchemy.sql.elements import Null
sys.path.append('.')
from astrodbkit2.astrodb import create_database
from astrodbkit2.astrodb import Database
from simple.schema import *
from utils import *
from astropy.table import Table
from pathlib import Path
import os

DRY_RUN = True #modifies .db file but not the data files
RECREATE_DB = True #recreates the .db file from the data files
VERBOSE = False

verboseprint = print if VERBOSE else lambda *a, **k: None

db_file = 'SIMPLE.db'
db_file_path = Path(db_file)
db_connection_string = 'sqlite:///SIMPLE.db'  # SQLite browser

if RECREATE_DB and db_file_path.exists():
    os.remove(db_file) #removes the current .db file if one already exists

if not db_file_path.exists():
    create_database(db_connection_string) #creates empty database based on the simple schema
    db = Database(db_connection_string) #connects to the empty database
    db.load_database('data/') #loads the data from the data files into the database
else:
    db = Database(db_connection_string) #if database already exists, connects to .db file

#Adding missing publications to publication table
add_publication(db, name='Sahl16', bibcode='J/MNRAS/455/357', dryrun=False)
add_publication(db, name='Liu16', bibcode='J/ApJ/833/96', dryrun=False)
add_publication(db, name='Wang18', bibcode='2018PASP..130f4402W', dryrun=False)
add_publication(db, name='Bedi17', bibcode='2017MNRAS.470.1140B', dryrun=False)
add_publication(db, name='Delo17', bibcode='2017A&A...608A..79D', dryrun=False)
add_publication(db, name='Luhm16', bibcode='2016AJ....152...78L', dryrun=False)


# load table of sources to ingest
input_file = ("scripts/ingests/ATLAS_table.vot")
ATLAS=Table.read(input_file, format='votable')

#filtering out objects w/out parallax data
ind=ATLAS['Plx'].nonzero()


sources = ATLAS['Name'][ind]
plx = ATLAS['Plx'][ind]
plx_unc = ATLAS['e_Plx'][ind]
plx_ref = ATLAS['r_Plx'][ind]

#Mapping the ref numbers to the actual references
name_ref = []
for ref in plx_ref:
    if ref ==13:
        name_ref.append('Andr11')
    elif ref ==14:
        name_ref.append('Fahe13')
    elif ref ==15:
        name_ref.append('Sahl16') 
    elif ref ==16:
        name_ref.append('Vrba04')
    elif ref==17:
        name_ref.append('Gizi15') 
    elif ref==18:
        name_ref.append('Liu16') 
    elif ref==19:
        name_ref.append('Fahe12')
    elif ref ==20:
        name_ref.append('Wang18') 
    elif ref ==21:
        name_ref.append('Liu_13a')
    elif ref ==22:
        name_ref.append('Dupu12a') 
    elif ref ==23:
        name_ref.append('Bedi17') 
    elif ref ==24:
        name_ref.append('Dahn02')
    elif ref ==25:
        name_ref.append('Maro13')
    elif ref ==26:
        name_ref.append('Wein12')
    elif ref ==27:
        name_ref.append('Tinn14')
    elif ref ==28:
        name_ref.append('Tinn03')
    elif ref ==29:
        name_ref.append('Delo17') 
    elif ref ==30:
        name_ref.append('Mars13')
    elif ref ==31:
        name_ref.append('Luhm16') 
    elif ref ==32:
        name_ref.append('Kirk11')
    elif ref ==33:
        name_ref.append('Legg17')
    elif ref ==34:
        name_ref.append('Mart18')
    else:
        name_ref.append('Missing')



ingest_parallaxes(db, sources, plx, plx_unc, name_ref, verbose=True)


if not DRY_RUN:
    db.save_db('data') #edits the JSON files if we're not doing a dry run
