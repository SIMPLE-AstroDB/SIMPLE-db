from scripts.ingests.ingest_utils import *
from scripts.ingests.utils import find_source_in_db, load_simpledb
from astropy.io import ascii


SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files
# LOAD THE DATABASE
db = load_simpledb("SIMPLE.db", recreatedb=RECREATE_DB)

#Calamari url: https://docs.google.com/spreadsheets/d/1i_Iblqp4WoKJ8lbyCT_u_u7AOiyse3OHfzdQln-fi9g/edit#gid=0

SHEET_ID = '1i_Iblqp4WoKJ8lbyCT_u_u7AOiyse3OHfzdQln-fi9g'
#SHEET_NAME = 'calamari'
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

found_list= [] 
not_found_list= []


for row in calamari_table:
    # read in row
    Dec = row['Dec']
    RA = row['RA']
    name = row['All Names']

    # check the target in the row is in the DB
    result = find_source_in_db(db, name, ra=RA, dec=Dec)
    print(name, result)
    
    # add the target to its assigned list
    if len(result) > 0: # if it is in the database
        found_list.append(name)
    else: # not in the db
        not_found_list.append(name)


print(found_list)
print(not_found_list)


#Ingest missing sources (8 sources) also not found in SIMBAD
#Need to ingest names, sources, publications, companion relationships
sources = [
    ["J19073307+3015304"],
    ["J18005854+1505198"],
    ["J11102921-2925186"],
    ["ULAS J13300249+0914321"],
    ["WISE J124332.17+600126.6"],
    ["ULAS J014016.91+015054.7"],
    ["HD 4747 B"],
    ["HD 33632 Ab"]
]
#specific rows to ingest
specific_rows = [6, 8, 10, 12, 32, 33, 39, 41]

#idea is to loop through specific rows to ingest ra, dec and ref
with calamari_table:
    for row_index_calamari_table, row in enumerate(specific_rows):
        if row_index_calamari_table in specific_rows:
            #ingest data from specific row
            print(row)

#different idea
def add_sources(db):
    #line 43
    ingest_source(db, "2MASS J19073307+3015304", reference="Gaia DR3")

    #line 42        
    ingest_source(db, "2MASS J18005854+1505198", reference="Gaia DR3, Marocco+2020",
            ra= "270.2437", 
            dec="15.08843", 
            search_db=False)
    #line 25        
    ingest_source(db, "2MASS J11102921-2925186", reference="Gaia DR3, Marocco+2020", 
            ra= ["167.621714"], 
            dec=["-29.4221669"], 
            search_db=False)
    #line 31
    ingest_source(db, "ULAS J13300249+0914321", reference="Gaia DR3, Marocco+2017", 
            ra= ["202.5102524"], 
            dec=["9.2422718"], 
            search_db=False)
    #line 27
    ingest_source(db, "WISE J124332.17+600126.6", reference="Faherty+2022", 
            ra= ["190.88386"], 
            dec=["60.023957"], 
            search_db=False)
    #line 7
    ingest_source(db, "ULAS J014016.91+015054.7", reference="Burningham+2018, Faherty+2022", 
            ra= ["25.071311"], 
            dec=["1.8484382"], 
            search_db=False)
    #line 6
    ingest_source(db, "HD 4747B", reference="Crepp+2016", 
            ra= "12.361505", 
            dec= "-23.212463", 
            search_db=False)
    #line 14
    ingest_source(db, "HD 33632Ab", reference="Currie+2020", 
            ra= "78.3208", 
            dec="37.2808", 
            search_db=False)


# WRITE THE JSON FILES
if SAVE_DB:
    db.save_database(directory="data/")

