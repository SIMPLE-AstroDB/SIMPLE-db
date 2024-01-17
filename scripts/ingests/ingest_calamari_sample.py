from scripts.ingests.ingest_utils import *
from scripts.ingests.utils import find_source_in_db, load_simpledb
from astropy.io import ascii


SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files
# LOAD THE DATABASE
db = load_simpledb("SIMPLE.db", recreatedb=RECREATE_DB)

#Calamari url: https://docs.google.com/spreadsheets/d/1i_Iblqp4WoKJ8lbyCT_u_u7AOiyse3OHfzdQln-fi9g/edit#gid=0

SHEET_ID = '1i_Iblqp4WoKJ8lbyCT_u_u7AOiyse3OHfzdQln-fi9g'
SHEET_NAME = 'calamari'
full = 'all'

url = f'https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={full}'

calamari_table = ascii.read(
    url,
    format="csv",
    data_start=1,
    header_start=0,
    guess=False,
    fast_reader=False, 
    delimiter=",",
)

#print result table
print(calamari_table.info)



for row in calamari_table:
    # read in row
    Dec = row['Dec']
    RA = row['RA']
    name = row['All Names']

    # check the target in the row is in the DB
    result = find_source_in_db(db, name, ra=RA, dec=Dec)

    found_list= ['calamari_table'] #not sure what to assign these to
    not_found_list= ['calamari_table']

    # add the target to its assigned list
    if len(result) > 0: # if it is in the database
        found_list.append(name)
    else: # not in the db
        not_found_list.append(name)


print(found_list)
print(not_found_list)

# WRITE THE JSON FILES
if SAVE_DB:
    db.save_database(directory="data/")

