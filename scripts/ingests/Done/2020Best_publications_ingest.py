import sys
sys.path.append('.')
from utils import *
from astrodbkit2.astrodb import create_database
from astrodbkit2.astrodb import Database
from astropy.table import Table
from pathlib import Path
from simple.schema import *
import os

save_db = True #modifies .db file but not the data files
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
update_Law06 = db.Publications.update().where(db.Publications.c.name == 'Law_06').\
	values(name='Law_06b')
db.engine.execute(update_Law06)
db.Publications.delete().where(db.Publications.c.name == 'Legg17').execute()
add_publication(db,bibcode='2017ApJ...842..118L', verbose=False)
update_Liu05 = db.Publications.update().where(db.Publications.c.name == 'Liu_05').\
	values(bibcode='2005ApJ...634..616L')
db.engine.execute(update_Liu05)
update_Liu13 = db.Publications.update().where(db.Publications.c.name == 'Liu_13a').\
	values(name='Liu_13b')
db.engine.execute(update_Liu13)
update_NLTT = db.Publications.update().where(db.Publications.c.name == 'Luyt79b').\
	values(bibcode='1979nlcs.book.....L')
db.engine.execute(update_NLTT)
update_Mars08 = db.Publications.update().where(db.Publications.c.name == 'Mars08').\
	values(name='Mars08b')
db.engine.execute(update_Mars08)
update_Mart18 = db.Publications.update().where(db.Publications.c.name == 'Mart18').\
	values(bibcode='2018ApJ...867..109M')
db.engine.execute(update_Mart18)
update_Niel12 = db.Publications.update().where(db.Publications.c.name == 'Niel12').\
	values(bibcode='2012ApJ...750...53N')
db.engine.execute(update_Niel12)
update_Pinf12 = db.Publications.update().where(db.Publications.c.name == 'Pinf12').\
	values(bibcode='2012MNRAS.422.1922P')
db.engine.execute(update_Pinf12)
update_Radi08 = db.Publications.update().where(db.Publications.c.name == 'Radi08').\
	values(bibcode='2008ApJ...689..471R')
db.engine.execute(update_Radi08)
update_Reid03a = db.Publications.update().where(db.Publications.c.name == 'Reid03a').\
	values(name='Reid03c')
db.engine.execute(update_Reid03a)
update_Reid03b = db.Publications.update().where(db.Publications.c.name == 'Reid03b').\
	values(name='Reid03a')
db.engine.execute(update_Reid03b)
update_Sahl16 = db.Publications.update().where(db.Publications.c.name == 'Sahl16').\
	values(bibcode='2016MNRAS.455..357S')
db.engine.execute(update_Sahl16)
update_Schn15 = db.Publications.update().where(db.Publications.c.name == 'Schn15').\
	values(bibcode='2015ApJ...804...92S')
db.engine.execute(update_Schn15)
update_Ston16 = db.Publications.update().where(db.Publications.c.name == 'Ston16').\
	values(bibcode='2016ApJ...818L..12S')
db.engine.execute(update_Ston16)
update_Zhan17 = db.Publications.update().where(db.Publications.c.name == 'Zhan17a').\
	values(bibcode='2017MNRAS.464.3040Z')
db.engine.execute(update_Zhan17)
add_publication(db,name='Card12', bibcode='2012PhDT.........?C',ignore_ads=True,\
	description='Observational properties of brown dwarfs: The low-mass end of the mass function')
add_publication(db, name='Tria20', bibcode='2020NatAs...4..650T', verbose=False)			  
#New database modifications that may or may not work
update_Alle07 = db.Publications.update().where(db.Publications.c.name == 'Alle07').\
	values(name='Alle07a')
db.engine.execute(update_Alle07)
update_Bill06 = db.Publications.update().where(db.Publications.c.name == 'Bill06').\
	values(name='Bill06a')
db.engine.execute(update_Bill06)
update_Bocc03 = db.Publications.update().where(db.Publications.c.name == 'Bocc03').\
	values(name='Bocc03b')
db.engine.execute(update_Bocc03)
update_Bouv08 = db.Publications.update().where(db.Publications.c.name == 'Bouv08').\
	values(name='Bouv08a')
db.engine.execute(update_Bouv08)
update_Bouy04 = db.Publications.update().where(db.Publications.c.name == 'Bouy04').\
	values(name='Bouy04a')
db.engine.execute(update_Bouy04)
update_Bouy08 = db.Publications.update().where(db.Publications.c.name == 'Bouy08').\
	values(name='Bouy08b')
db.engine.execute(update_Bouy08)
update_Kirk94b = db.Publications.update().where(db.Publications.c.name == 'Kirk94b').\
	values(name='Kirk94')
db.engine.execute(update_Kirk94b)
db.Publications.delete().where(db.Publications.c.name == 'Luhm09').execute()
update_Riaz08b = db.Publications.update().where(db.Publications.c.name == 'Riaz08b').\
	values(name='Riaz08')
db.engine.execute(update_Riaz08b)
update_Wils03b = db.Publications.update().where(db.Publications.c.name == 'Wils03b').\
	values(name='Wils03')
db.engine.execute(update_Wils03b)


print('confirmed')


# add missing references
#Searching for all publications in table and adding missing ones to pub table in .db file
data_start=1
#data_end=data_start+500
Pubs = Table.read('scripts/ingests/UltracoolSheet-References.csv', data_start=data_start)
best_bibcodes = Pubs['ADSkey_ref']
best_names = Pubs['code_ref']
for i, best_name in enumerate(best_names):
	#print(i+data_start)
	#print("searching:",i,best_names[i],best_bibcodes[i])
	bibcode_search = search_publication(db, bibcode=best_bibcodes[i], verbose = False)
	name_search = search_publication(db, name=best_name, verbose= False)
	if bibcode_search[0] == False and bibcode_search[1] == 0: # no bibcode matches
		if name_search[0] == False: #no name matches either
			print(i," Adding:", best_name, best_bibcodes[i])
			add_publication(db, name=best_name, bibcode=best_bibcodes[i], save_db=save_db,verbose=False)
		elif name_search[0] == True:
			print(i, " Name match: ", best_name)
		else:
			raise 
	elif bibcode_search[0] == False and bibcode_search[1] > 0: # multiple matches
		print("!! multiple matches: ", i, best_name, best_bibcodes[i])
	elif bibcode_search[0] == True: #bibcode found
		print(i," Bibcode Match found for:", best_name, best_bibcodes[i])
		if search_publication(db, bibcode=best_bibcodes[i], name=best_name, verbose=False)[0] == False:
			print(i,'!! Bibcode match but mismatched ref name: !! ', i, best_name)
			if search_publication(db, bibcode=best_bibcodes[i],name=best_name[:-1], verbose=False)[0] == True:
				print(i,'!! Match found for ',best_name[:-1])
	else:
		raise


#Geli11 is in database as Geli11
#Schm10b is in database as Schm10b and Schm10


