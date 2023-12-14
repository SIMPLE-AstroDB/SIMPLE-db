from scripts.ingests.ingest_utils import *
from scripts.ingests.utils import *
from astropy.table import Table
import astropy.units as u
from astropy.coordinates import Angle

SAVE_DB = True  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files
# LOAD THE DATABASE
db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)


# live google sheet
link = "https://docs.google.com/spreadsheets/d/1JFa8F4Ngzp3qAW8NOBurkz4bMKo9zXYeF6N1vMtqDZs/edit#gid=0"

columns = ['source', 'ra', 'dec', 'epoch', 'equinox', 'shortname', 'reference', 'other_ref', 'comments']
byw_table = Table.read(link, format='ascii', data_start=2, data_end=90, header_start=1, names=columns, guess=False,
                           fast_reader=False, delimiter=',')

data_columns = ['CWISE J000021.45-481314.9', '0.0893808', '-48.2208077', '2015.4041', 'ICRS', 'CWISE 0000-4813', 'Rothermich', 'None', 'None']  # columns with wanted data values

print(byw_table)

# replacing empty values ('cdots') with None
for column in data_columns:
    byw_table[column][np.where(byw_table[column] == 'cdots')] = None

def ingest_source(db, source, reference=None, ra=None, dec=None, epoch=None, equinox=None, 
                  raise_error=True, search_db=True, other_reference=None, comment=None):
    # TODO: ADD DOCSTRING
    
    if ra is None and dec is None:
        coords_provided = False
    else:
        coords_provided = True
        ra = ra
        dec = dec
        epoch = epoch
        equinox = equinox
   
    logger.debug(f"coords_provided:{coords_provided}")

    # Find out if source is already in database or not
    if coords_provided and search_db:
        name_matches = find_source_in_db(db, source, ra=ra, dec=dec)
    elif search_db:
        name_matches = find_source_in_db(db, source)
    elif not search_db:
        name_matches = []
    else:
        name_matches = None

    logger.debug(f"name_matches: {name_matches}")

    # Source is already in database
    if len(name_matches) == 1 and search_db:  
        msg = f"Not ingesting {source}. Already in database as {name_matches[0]}. \n "
        logger.info(msg)

        # Figure out if source name provided is an alternate name
        db_source_matches = db.search_object(source, output_table='Sources', fuzzy_search=False)
        
        # Try to add alternate source name to Names table
        if len(db_source_matches) == 0:
            alt_names_data = [{'source': name_matches[0], 'other_name': source}]
            try:
                with db.engine.connect() as conn:
                    conn.execute(db.Names.insert().values(alt_names_data))
                    conn.commit()
                logger.debug(f"   Name added to database: {alt_names_data}\n")
            except sqlalchemy.exc.IntegrityError as e:
                msg = f"   Could not add {alt_names_data} to database"
                logger.warning(msg)
                if raise_error:
                    raise SimpleError(msg + '\n' + str(e))
                else:
                    return
        return  # Source is already in database, nothing new to ingest
        
    # Multiple source matches in the database so unable to ingest source
    elif len(name_matches) > 1 and search_db:  
        msg1 = f"   Not ingesting {source}."
        msg  = f"   More than one match for {source}\n {name_matches}\n"
        logger.warning(msg1 + msg)
        if raise_error:
            raise SimpleError(msg)
        else:
            return
    
     # No match in the database, INGEST!
    elif len(name_matches) == 0 or not search_db: 

         # Make sure reference is provided and in References table
        if reference is None or ma.is_masked(reference):
            msg = f"Not ingesting {source}. Discovery reference is blank. \n"
            logger.warning(msg)
            if raise_error:
                raise SimpleError(msg)
            else:
                return

        ref_check = find_publication(db, name=reference)
        logger.debug(f'ref_check: {ref_check}')

        if ref_check is False:
            msg = f"Skipping: {source}. Discovery reference {reference} is not in Publications table. \n" \
                    f"(Add it with ingest_publication function.)"
            logger.warning(msg)
            if raise_error:
                raise SimpleError(msg + msg2)
            else:
                return

        # Try to get coordinates from SIMBAD if they were not provided
        if not coords_provided:
            # Try to get coordinates from SIMBAD
            simbad_result_table = Simbad.query_object(source)
           
            if simbad_result_table is None:
                msg = f"Not ingesting {source}. Coordinates are needed and could not be retrieved from SIMBAD. \n"
                logger.warning(msg)
                if raise_error:
                    raise SimpleError(msg)
                else:
                    return
            # One SIMBAD match! Using those coordinates for source.
            elif len(simbad_result_table) == 1:
                simbad_coords = simbad_result_table['RA'][0] + ' ' + simbad_result_table['DEC'][0]
                simbad_skycoord = SkyCoord(simbad_coords, unit=(u.hourangle, u.deg))
                ra      = simbad_skycoord.to_string(style='decimal').split()[0]
                dec     = simbad_skycoord.to_string(style='decimal').split()[1]
                epoch   = '2000'  # Default coordinates from SIMBAD are epoch 2000.
                equinox = 'J2000'  # Default frame from SIMBAD is IRCS and J2000.
                msg     = f"Coordinates retrieved from SIMBAD {ra}, {dec}"
                logger.debug(msg)
            else: 
                msg = f"Not ingesting {source}. Coordinates are needed and could not be retrieved from SIMBAD. \n"
                logger.warning(msg)
                if raise_error:
                    raise SimpleError(msg)
                else:
                    return

       

    # Just in case other conditionals not met    
    else:
        msg = f"Unexpected condition encountered ingesting {source}"
        logger.error(msg)
        raise SimpleError(msg)

    logger.debug(f"   Ingesting {source}.")

    # Construct data to be added
    source_data = [{'source': source,
                    'ra': ra,
                    'dec': dec,
                    'reference': reference,
                    'epoch': epoch,
                    'equinox': equinox,
                    'other_references': other_reference,
                    'comments': comment }]
    names_data = [{'source': source,
                    'other_name': source}]

    # Try to add the source to the database
    try:
        with db.engine.connect() as conn:
            conn.execute(db.Sources.insert().values(source_data))
            conn.commit()
        msg = f"Added {str(source_data)}"
        logger.info(f"Added {source}")
        logger.debug(msg)
    except sqlalchemy.exc.IntegrityError:
        msg  = f"Not ingesting {source}. Not sure why. \n"
        msg2 = f"   {str(source_data)} "
        logger.warning(msg)
        logger.debug(msg2)
        if raise_error:
            raise SimpleError(msg + msg2)
        else:
            return

    # Try to add the source name to the Names table
    try:
        with db.engine.connect() as conn:
            conn.execute(db.Names.insert().values(names_data))
            conn.commit()
        logger.debug(f"    Name added to database: {names_data}\n")
    except sqlalchemy.exc.IntegrityError:
        msg = f"   Could not add {names_data} to database"
        logger.warning(msg)
        if raise_error:
            raise SimpleError(msg)
        else:
            return
    
    return


# WRITE THE JSON FILES
if SAVE_DB:
    db.save_database(directory='data/') 