# Script to add photometry from the BDNYC database into SIMPLE

from astrodbkit2.astrodb import Database, and_
from sqlalchemy import types  # for BDNYC column overrides

verbose = True

# --------------------------------------------------------------------------------------
# Establish connection to databases

# Note that special parameters have to be passed to allow the BDNYC schema work properly
connection_string = 'sqlite:///../BDNYCdevdb/bdnycdev.db'
bdnyc = Database(connection_string,
                 reference_tables=['changelog', 'data_requests', 'publications', 'ignore', 'modes',
                                   'systems', 'telescopes', 'versions', 'instruments'],
                 primary_table='sources',
                 primary_table_key='id',
                 foreign_key='source_id',
                 column_type_overrides={'spectra.spectrum': types.TEXT(),
                                        'spectra.local_spectrum': types.TEXT()})

# SIMPLE
connection_string = 'sqlite:///SIMPLE.db'
db = Database(connection_string)

# --------------------------------------------------------------------------------------
# Reload from directory, if needed
db.load_database('data', verbose=False)

# --------------------------------------------------------------------------------------
# For each source in SIMPLE, search in BDNYC and grab specified photometry

# Will be only grabbing WISE data for now
telescope = 'WISE'
band_list = ['WISE_W1', 'WISE_W2', 'WISE_W3', 'WISE_W4']

# Don't include sources that already have photometry in these bands
temp = db.query(db.Photometry.c.source).filter(db.Photometry.c.band.in_(band_list)).distinct().all()
sources_with_photometry = [s[0] for s in temp]

sources = db.query(db.Sources).\
    filter(db.Sources.c.source.notin_(sources_with_photometry)).\
    pandas()

# Get the BDNYC source_id values for our SIMPLE sources
source_dict = {}
for i, row in sources.iterrows():
    bd_source = bdnyc.search_object(row['source'], output_table='sources',
                                    table_names={'sources': ['designation', 'names']},
                                    format='pandas')
    if len(bd_source) != 1:
        print(f"ERROR matching {row['source']}")
    else:
        source_dict[row['source']] = int(bd_source['id'].values[0])

# Grab only photometry in the band list that has version flags and publications
for source, bdnyc_id in source_dict.items():
    print(f'{source} : {bdnyc_id}')
    bd_data = bdnyc.query(bdnyc.photometry).\
        filter(and_(bdnyc.photometry.c.source_id == bdnyc_id,
                    bdnyc.photometry.c.publication_shortname.isnot(None),
                    bdnyc.photometry.c.version <= 2,
                    bdnyc.photometry.c.band.in_(band_list))).\
        pandas()

    if len(bd_data) == 0:
        continue

    # Insert into the database
    new_data = []
    for i, row in bd_data.iterrows():
        old_data = db.query(db.Photometry).filter(db.Photometry.c.source == source).pandas()
        if len(old_data) > 0:
            if (row['band'], row['publication_shortname']) in zip(old_data['band'].tolist(),
                                                                  old_data['reference'].tolist()):
                if verbose:
                    print(f"{source}: {row['band']} already in database for reference {row['publication_shortname']}")
                new_data = None
                continue

        datum = {'source': source,
                'band': row['band'],
                'magnitude': row['magnitude'],
                'magnitude_error': row['magnitude_unc'],
                'telescope': 'WISE',
                'reference': row['publication_shortname'],
                'epoch': row['epoch'],
                'comments': row['comments']}
        new_data.append(datum)
    if new_data is not None:
        print(f"{source} : Ingesting new data: {new_data}")
        db.Photometry.insert().execute(new_data)


# --------------------------------------------------------------------------------------
# Output changes to directory
db.save_database('data')
