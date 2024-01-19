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

    ingest_sources(db, ["J19073307+3015304"], references="", 
            ras= ["286.8885"], 
            decs=["30.25893"], 
            search_db=False)
    ingest_sources(db, ["J18005854+1505198"], references="", 
            ras= [], 
            decs=[], 
            search_db=False)
    ingest_sources(db, ["J11102921-2925186"], references="", 
            ras= [], 
            decs=[], 
            search_db=False)
    ingest_sources(db, ["ULAS J13300249+0914321"], references="", 
            ras= [], 
            decs=[], 
            search_db=False)
    ingest_sources(db, ["WISE J124332.17+600126.6"], references="", 
            ras= [], 
            decs=[], 
            search_db=False)
    ingest_sources(db, ["ULAS J014016.91+015054.7"], references="", 
            ras= [], 
            decs=[], 
            search_db=False)
    ingest_sources(db, ["HD 4747 B"], references="", 
            ras= [], 
            decs=[], 
            search_db=False)
    
    ingest_sources(db, ["HD 33632 Ab"], references="", 
            ras= [], 
            decs=[], 
            search_db=False)


# WRITE THE JSON FILES
if SAVE_DB:
    db.save_database(directory="data/")

