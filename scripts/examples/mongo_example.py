# Example script on how to use MongoDB with SIMPLE
# Requires a running (local) instance of MongoDB; see https://docs.mongodb.com/manual/installation/
# Requires pymongo: conda install pymongo

import os
import json
import pymongo


def load_reference(file, path, table):
    # Handle loading a reference table by looping over each entry

    with open(os.path.join(path, file)) as f:
        data = json.load(f)

    for json_data in data:
        name = json_data['name']
        count = db[table].count_documents({'name': name})

        if count > 0:
            # Replace existing
            cursor = db[table].find({'name': name})
            for doc in cursor:
                result = db[table].replace_one(filter={'_id': doc['_id']}, replacement=json_data)
        else:
            # Insert new
            result = db[table].insert_one(json_data)


client = pymongo.MongoClient()  # default connection (ie, local)

# Connect to database
db = client['SIMPLE']

# Set up main collection
Sources = db.Sources
# Sources.drop()  # drop collection, if needed
# For other collections I will use the db['collection'] scheme

reference_tables = ['Publications', 'Telescopes', 'Instruments']
path = 'data'

# Loop over the JSON files and add them to the database
for file in os.listdir(path):
    if not file.endswith('.json'):
        continue
    if file.split('.')[0] in reference_tables:
        load_reference(file, path, table=file.split('.')[0])
        continue

    # Start loading a Source
    with open(os.path.join(path, file)) as f:
        data = json.load(f)

    # Match by source name and if existing replace, otherwise insert a new record
    source = data['Sources'][0]['source']
    print(f'Inserting {source}')
    count = Sources.count_documents({'Sources.source': source})

    if count > 0:
        # Replace existing
        cursor = Sources.find({'Sources.source': source})
        for doc in cursor:
            result = Sources.replace_one(filter={'_id': doc['_id']}, replacement=data)
            # upsert=True can be used here to insert new entries if they don't exist,
            # but the collection must exist first for it to work properly
    else:
        # Insert new
        result = Sources.insert_one(data)

# Some queries
cursor = db['Sources'].find({'Sources.source': 'TWA 27'})
for doc in cursor:
    print(doc)
    print(doc['Sources'][0]['source'])

# Note that MongoDB adds a unique _id key for each record
# Can use projection to remove some of the output, like _id
cursor = db['Sources'].find({'Photometry.band': 'WISE_W1'}, projection={'_id': 0})
for doc in cursor.limit(1):
    print(json.dumps(doc, indent=4))  # pretty print
# Note that this returns the full Sources information for anything that had WISE_W1 photometry

# Query for any photometry, but only return the Photometry subset of the information
cursor = Sources.find({'Photometry': {'$exists': True}}, projection={'_id': 0, 'Photometry': 1})
for doc in cursor.limit(1):
    print(json.dumps(doc, indent=4))  # pretty print

# Indexing all text fields for better text queries
if 'text_fields' not in Sources.index_information():
    Sources.drop_index('text_fields')
Sources.create_index([('$**', pymongo.TEXT)], name='text_fields', background=True)

# Searching for any source that has TWA in the name
cursor = Sources.find({'$text': {'$search': 'TWA'}}, {'_id': 0, 'Sources.source': 1, 'Names.other_name': 1})
for doc in cursor:
    print(doc)

# Query other collections
cursor = db['Publications'].find({'name': 'Cruz18'}, {'_id': 0})
for doc in cursor:
    print(json.dumps(doc, indent=4))  # pretty print

# The reverse (loading data from MongoDB to a data directory) should not be too complicated,
# but we have to remember to remove the extra _id column as that is not serializable in JSON
# and not needed for our purposes
