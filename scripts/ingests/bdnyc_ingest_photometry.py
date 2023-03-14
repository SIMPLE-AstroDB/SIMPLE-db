# Script to add photometry from the BDNYC database into SIMPLE

from astrodbkit2.astrodb import Database, and_
from sqlalchemy import types  # for BDNYC column overrides
from datetime import datetime
from astropy.table import Table

verbose = True


# Helper functions
def fix_band(band):
    # Fix the band name to what we want in SIMPLE
    band = band.replace('_', '.')
    return band


def fix_epoch(epoch):
    # Fix epoch; it's expected to be a decimal year

    def dt_to_dec(dt):
        """Convert a datetime to decimal year.
        See: https://stackoverflow.com/questions/29851357/python-datetime-to-decimal-year-one-day-off-where-is-the-bug
        """
        year_start = datetime(dt.year, 1, 1)
        year_end = year_start.replace(year=dt.year + 1)
        return dt.year + ((dt - year_start).total_seconds() /  # seconds so far
                          float((year_end - year_start).total_seconds()))  # seconds in year

    decimal_year = None
    if epoch is not None:
        try:
            d = datetime.fromisoformat(epoch)
            decimal_year = dt_to_dec(d)
        except:
            pass

    return decimal_year


def get_telescope(band):
    # Fetch the correct telescope name
    tel = None
    if 'WISE' in band:
        tel = 'WISE'
    elif 'SDSS' in band:
        tel = 'SDSS'
    elif 'IRAC' in band or 'MIPS' in band:
        tel = 'Spitzer'
    elif 'Gaia' in band:
        tel = 'Gaia'
    elif 'HST' in band:
        tel = 'HST'
    elif 'GALEX' in band:
        tel = 'GALEX'
    elif '2MASS' in band:
        tel = '2MASS'

    return tel


def fetch_reference(db, bdnyc, pub_name):
    # Fetch the correct reference to use
    # This is good practice, but for BDNYC-SIMPLE it may be the same
    bibcode = None
    ref = None
    t = bdnyc.query(bdnyc.publications). \
        filter(bdnyc.publications.c.shortname == pub_name). \
        table()
    if len(t) == 1:
        bibcode = t['bibcode'][0]
    if bibcode is not None:
        t = db.query(db.Publications).filter(db.Publications.c.bibcode == bibcode).table()
        if len(t) == 1:
            ref = t['publication'][0]

    return ref

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

# Considering all bands rather than specifying only a few

# These bands have issues and will be skipped:
band_skip = ["MKO_M'"]  # has duplicate values in BDNYC with no epoch to distinguish

# # Which BDNYC bands to consider
# band_list = ['WISE_W1', 'WISE_W2', 'WISE_W3', 'WISE_W4']
#
# # Don't include sources that already have photometry in these bands
# temp = db.query(db.Photometry.c.source).filter(db.Photometry.c.band.in_(band_list)).distinct().all()
# sources_with_photometry = [s[0] for s in temp]
#
# sources = db.query(db.Sources).\
#     filter(db.Sources.c.source.notin_(sources_with_photometry)).\
#     pandas()

sources = db.query(db.Sources).pandas()

# Check and add any missing telescopes
tel_list = ['HST', 'WISE', 'GALEX', 'CFHT', 'Keck II', 'Gemini South', 'CTIO 1.5m',
            'Pan-STARRS 1', 'Spitzer', 'UKIRT', '2MASS', 'SDSS']
tel_list_db = db.query(db.Telescopes.c.name).table()['name'].tolist()
new_tel = []
for tel in tel_list:
    if tel not in tel_list_db:
        datum = {'name': tel}
        new_tel.append(datum)
if len(new_tel) > 0:
    print(f'Inserting: {new_tel}')
    with db.engine.connect() as conn:
        conn.execute(db.Telescopes.insert().values(new_tel))
        conn.commit()

# Get the BDNYC source_id values for our SIMPLE sources
source_dict = {}
for i, row in sources.iterrows():
    bd_source = bdnyc.search_object(row['source'], output_table='sources',
                                    table_names={'sources': ['designation', 'names']},
                                    fmt='pandas')
    if len(bd_source) != 1:
        # print(f"ERROR matching {row['source']}")
        continue
    else:
        source_dict[row['source']] = int(bd_source['id'].values[0])
print(f'{len(source_dict)} sources matched between databases')

# Grab only photometry in the band list that has version flags and publications
for source, bdnyc_id in source_dict.items():
    print(f'{source} : {bdnyc_id}')
    bd_data = bdnyc.query(bdnyc.photometry).\
        filter(and_(bdnyc.photometry.c.source_id == bdnyc_id,
                    bdnyc.photometry.c.publication_shortname.isnot(None),
                    bdnyc.photometry.c.version <= 2,
                    bdnyc.photometry.c.band.notin_(band_skip)
                    # bdnyc.photometry.c.band.in_(band_list)
                    )).\
        pandas()

    if len(bd_data) == 0:
        continue

    # Insert into the database
    new_data = []
    for i, row in bd_data.iterrows():
        old_data = db.query(db.Photometry).filter(db.Photometry.c.source == source).pandas()
        if len(old_data) > 0:
            if (fix_band(row['band']), row['publication_shortname']) in zip(old_data['band'].tolist(),
                                                                            old_data['reference'].tolist()):
                if verbose:
                    print(f"{source}: {row['band']} already in database for reference {row['publication_shortname']}")
                continue

        # Fetch the correct telescope to use
        tel = None
        t = bdnyc.query(bdnyc.telescopes).\
            filter(bdnyc.telescopes.c.id == row['telescope_id']).\
            table()
        if len(t) == 1:
            tel = t['name'][0]
        else:
            tel = get_telescope(row['band'])

        # Fetch correct reference
        ref = fetch_reference(db, bdnyc, row['publication_shortname'])
        if ref is None:
            # Missing reference- manually insert
            t = bdnyc.query(bdnyc.publications). \
                filter(bdnyc.publications.c.shortname == row['publication_shortname']). \
                table()
            try:
                new_pub = [{'publication': t['shortname'][0],
                            'bibcode': t['bibcode'][0],
                            'doi': t['DOI'][0],
                            'description': t['description'][0]}]
                with db.engine.connect() as conn:
                    conn.execute(db.Publications.insert().values(new_pub))
                    conn.commit()
                ref = t['shortname'][0]
            except Exception as e:
                # If the publication can't be added- skip this photometry data point
                print(f'Unable to add new publication: {e}')
                continue

        # Fix some values
        band = fix_band(row['band'])
        epoch = fix_epoch(row['epoch'])

        datum = {'source': source,
                'band': band,
                'magnitude': row['magnitude'],
                'magnitude_error': row['magnitude_unc'],
                'telescope': tel,
                'reference': ref,
                'epoch': epoch,
                'comments': row['comments']}
        new_data.append(datum)

    if new_data is not None and len(new_data) > 0:
        print(f"{source} : Ingesting new data: {new_data}")
        try:
            with db.engine.connect() as conn:
                conn.execute(db.Photometry.insert().values(new_data))
                conn.commit()
        except Exception as e:
            # Print out some diagnositics (the new data to be inserted and the original) to spot the issue
            print(Table(new_data))
            print(Table(bdnyc.inventory(bdnyc_id)['photometry']))
            raise e


# --------------------------------------------------------------------------------------
# Output changes to directory
db.save_database('data')
