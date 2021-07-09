from scripts.ingests.utils import *
from astrodbkit2.astrodb import create_database
from astrodbkit2.astrodb import Database
from astropy.table import Table
from pathlib import Path
from simple.schema import *
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


add_Arti_bibcode = db.Publications.update().where(db.Publications.c.name == 'Arti15').\
	values(bibcode='2015ApJ...806..254A')
db.engine.execute(add_Arti_bibcode)
db.Publications.delete().where(db.Publications.c.name == 'Barm10').execute()
db.Publications.delete().where(db.Publications.c.name == 'Bouy06').execute()
update_Deac11 = db.Publications.update().where(db.Publications.c.name == 'Deac11').\
	values(name='Deac11b')
db.engine.execute(update_Deac11)
update_Delo17 = db.Publications.update().where(db.Publications.c.name == 'Delo17').\
	values(name='Delo17b')
db.engine.execute(update_Delo17)
update_Folk07 = db.Publications.update().where(db.Publications.c.name == 'Folk07').\
	values(name='Folk07PhD')
db.engine.execute(update_Folk07)
update_Gate09 = db.Publications.update().where(db.Publications.c.name == 'Gate08').\
	values(name='Gate09')
db.engine.execute(update_Gate09)
add_Gauz15_bibcode = db.Publications.update().where(db.Publications.c.name == 'Gauz15').\
	values(bibcode='2015ApJ...804...96G')
db.engine.execute(add_Gauz15_bibcode)
add_Hink15_bibcode = db.Publications.update().where(db.Publications.c.name == 'Hink15').\
	values(bibcode='2015ApJ...805L..10H')
db.engine.execute(add_Hink15_bibcode)
add_Irwi91_bibcode = db.Publications.update().where(db.Publications.c.name == 'Irwi91').\
	values(bibcode='1991MNRAS.252P..61I')
db.engine.execute(add_Irwi91_bibcode)
update_Kirk97 = db.Publications.update().where(db.Publications.c.name == 'Kirk97b').\
	values(name='Kirk97a')
db.engine.execute(update_Kirk97)

# add missing references
#Searching for all publications in table and adding missing ones to pub table in .db file
Pubs = Table.read('scripts/ingests/UltracoolSheet-References.csv', data_start=351, data_end=450)
best_bibcodes = Pubs['ADSkey_ref']
best_names = Pubs['code_ref']
for i, best_name in enumerate(best_names):
	# print("searching:",i,best_names[i],best_bibcodes[i])
	bibcode_search = search_publication(db, bibcode=best_bibcodes[i])
	if bibcode_search[0] == False and bibcode_search[1] == 0: # no matches
		# print(i," Adding:", best_name, best_bibcodes[i])
		add_publication(db, name=best_name, bibcode=best_bibcodes[i], save_db=save_db,verbose=False)
	elif bibcode_search[0] == False and bibcode_search[1] > 0: # multiple matches]
		print("!! multiple matches: ", i, best_name, best_bibcodes[i])
	elif bibcode_search[0] == True: #bibcode found
		# print(i," Bibcode Match found for:", best_name, best_bibcodes[i])
		if search_publication(db, bibcode=best_bibcodes[i], name=best_name)[0] == False:
			print(i,'!! Bibcode match but mismatched ref name: !! ', i, best_name)
			if search_publication(db, bibcode=best_bibcodes[i],name=best_name[:-1])[0] == True:
				print(i,'!! Match found for ',best_name[:-1])
	elif search_publication(db, name=best_name) == True: #bibcode didn't match, but name did
		print("Name match: ", i, best_name)

#Alle07a = Alle07
#Alle13a = Alle13
# Bill06a = Bill06
# Bocc03b = Bocc03
#  Bouv08a =  Bouv08
#Bouy04a = Bouy04
#Bouy08b = Bouy08

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

