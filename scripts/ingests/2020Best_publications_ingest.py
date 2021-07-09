from operator import add
from re import search
import sys
from sqlalchemy.sql.elements import Null
sys.path.append('.')
from scripts.ingests.utils import *
from astrodbkit2.astrodb import create_database
from astrodbkit2.astrodb import Database
from simple.schema import *
from astropy.table import Table
from pathlib import Path
import os

save_db = False #modifies .db file but not the data files
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


# add missing references
#Searching for all publications in table and adding missing ones to pub table in .db file
Pubs = Table.read('scripts/ingests/UltracoolSheet-References.csv', data_start=1, data_end=50)
bibcodes = Pubs['ADSkey_ref']
for i,ref in enumerate(Pubs['code_ref']):
	if search_publication(db, bibcode=bibcodes[i]) == False:
		add_publication(db, name=ref, bibcode=bibcodes[i], save_db=save_db)
	elif search_publication(db, bibcode=bibcodes[i], verbose=True) == True:
		if search_publication(db, name=ref) == False:
			print('Mismatched Source:',ref)
	elif search_publication(db, name=ref) == True:
		pass
		#TODO Make sure that found publication is correct one


#add_publication(db, bibcode='2015MNRAS.450.2486C')
#add_publication(db, name='Luhm14c', bibcode='2014ApJ...787..126L')
#add_publication(db, bibcode='2019AJ....158..182G')
#add_publication(db, name='Schn16a', bibcode='2016ApJ...817..112S')
#add_publication(db, bibcode='2010A&A...517A..53M')
#add_publication(db, bibcode='2017AJ....154..112K')
#add_publication(db, name='Pinf14a', bibcode='2014MNRAS.437.1009P')
#add_publication(db, bibcode='2015ApJ...802...37B')
#add_publication(db, name='Mesh15b', bibcode='2015MNRAS.453.2378M')
#add_publication(db, name='Best20a', bibcode='2020AJ....159..257B')
#add_publication(db, bibcode='2016ApJ...821..120A')
#add_publication(db, name='Deac14b', bibcode='2014ApJ...792..119D')
#add_publication(db, name='Tinn98b', bibcode='1998A&A...338.1066T')
#add_publication(db, name='Caba07a', bibcode='2007A&A...462L..61C')
#add_publication(db, name='DayJ08', bibcode='2008MNRAS.388..838D')
#add_publication(db, bibcode='2016A&A...587A..51S')
#add_publication(db, bibcode='2017AJ....153..196S')
#add_publication(db, name='Lodi12b', bibcode='2012A&A...542A.105L')
#add_publication(db, bibcode='1981MNRAS.196p..15R')
#add_publication(db, bibcode='2013A&A...553L...5D')
#add_publication(db, bibcode='1997MNRAS.284..507T')

