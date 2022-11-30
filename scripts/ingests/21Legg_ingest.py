import logging
from astropy.io import ascii
from scripts.ingests.utils import *
from scripts.ingests.ingest_utils import *
from astropy.utils.exceptions import AstropyWarning
import numpy.ma as ma


warnings.simplefilter('ignore', category=AstropyWarning)

SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files
db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)

logger.setLevel(logging.INFO)


def convert_radec_to_decimal(source):
    # convert ra and dec to decimal string
    ra_seconds = source['RA'][4:]
    if float(ra_seconds) > 60.0:
        ra_seconds = f"{source['RA'][4:6]}.{source['RA'][6:]}"
    ra_sexg = f"{source['RA'][0:2]}h{source['RA'][2:4]}m{ra_seconds}s"
    dec_seconds = source['Decl.'][5:]
    if float(dec_seconds) > 60.0:
        dec_seconds = f"{source['Decl.'][5:7]}.{source['Decl.'][7:]}"
    dec_sexg = f"{source['Decl.'][0:3]}d{source['Decl.'][3:5]}m{dec_seconds}s"
    coord = SkyCoord(ra_sexg, dec_sexg)
    ra = coord.ra.degree
    dec = coord.dec.degree

    return ra, dec


def add_publications(db):
    ingest_publication(db, publication='Meis20.74', bibcode='2020ApJ...889...74M')  # Meisner_2020a
    ingest_publication(db, publication='Meis20.123', bibcode='2020ApJ...899..123M')  # Meisner_2020b
    ingest_publication(db, bibcode='2020ApJ...895..145B')
    ingest_publication(db, bibcode='2021ApJS..253....7K')
    ingest_publication(db, bibcode='2019ApJ...881...17M')
    ingest_publication(db, bibcode='2021ApJ...918...11L')
    ingest_publication(db, bibcode='2020ApJ...889..176F')
    ingest_publication(db, publication='Pinf14.priv',
                       description='P. Pinfield and M. Gromadzki, private communication 2014')

    db.Publications.update().where(db.Publications.c.publication == 'Burn10').values(
        publication='Burn10.1952').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Burn10b').values(
        publication='Burn10.1885').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Burn11').values(
        publication='Burn11.3590').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Burn11b').values(
        publication='Burn11.90').execute()

    db.Publications.update().where(db.Publications.c.publication == 'Burg00a').values(
        publication='Burg00.57').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Burg00b').values(
        publication='Burg00.473').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Burg00c').values(
        publication='Burg00.1100').execute()

    db.Publications.update().where(db.Publications.c.publication == 'Burg02a').values(
        publication='Burg02.421').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Burg02b').values(
        publication='Burg02.2744').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Burg02c').values(
        publication='Burg02.151').execute()

    db.Publications.update().where(db.Publications.c.publication == 'Burg03d').values(
        publication='Burg03.510').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Burg03b').values(
        publication='Burg03.512').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Burg03c').values(
        publication='Burg03.1186').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Burg03e').values(
        publication='Burg03.2487').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Burg03a').values(
        publication='Burg03.850').execute()

    db.Publications.update().where(db.Publications.c.publication == 'Burg04c').values(
        publication='Burg04.73').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Burg04a').values(
        publication='Burg04.827').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Burg04b').values(
        publication='Burg04.2856').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Burg04d').values(
        publication='Burg04.191').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Burg06c').values(
        publication='Burg06.1095').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Burg06e').values(
        publication='Burg06.585').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Burg06b').values(
        publication='Burg06.1067').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Burg06a').values(
        publication='Burg06.1007').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Burg06d').values(
        publication='Burg06.1485').execute()

    db.Publications.update().where(db.Publications.c.publication == 'Cush11').values(
        publication='Cush11.50').execute()

    db.Publications.update().where(db.Publications.c.publication == 'Delo08a').values(
        publication='Delo08.961').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Delo08b').values(
        publication='Delo08.469').execute()

    db.Publications.update().where(db.Publications.c.publication == 'Dupu15a').values(
        publication='Dupu15.102').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Dupu15b').values(
        publication='Dupu15.56').execute()

    db.Publications.update().where(db.Publications.c.publication == 'Geli11').values(
        publication='Geli11.57').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Geli11b').values(
        publication='Geli11.871').execute()

    db.Publications.update().where(db.Publications.c.publication == 'Lodi09b').values(
        publication='Lodi09.1631').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Lodi09d').values(
        publication='Lodi09.258').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Lodi07a').values(
        publication='Lodi07.1423').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Lodi07b').values(
        publication='Lodi07.372').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Lodi12a').values(
        publication='Lodi12.1495').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Lodi12b').values(
        publication='Lodi12.105').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Lodi12d').values(
        publication='Lodi12.53').execute()
    db.Publications.delete().where(db.Publications.c.publication == 'Lodi12').execute()

    db.Publications.update().where(db.Publications.c.publication == 'Loop07a').values(
        publication='Loop07.1162').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Loop07b').values(
        publication='Loop07.97').execute()

    db.Publications.update().where(db.Publications.c.publication == 'Liu_11a').values(
        publication='Liu_11.32').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Liu_11b').values(
        publication='Liu_11.108').execute()

    db.Publications.update().where(db.Publications.c.publication == 'Luhm12').values(
        publication='Luhm12.135').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Luhm12d').values(
        publication='Luhm12.152').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Luhm14b').values(
        publication='Luhm14.18').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Luhm14d').values(
        publication='Luhm14.16').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Luhm14a').values(
        publication='Luhm14.4').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Luhm14c').values(
        publication='Luhm14.126').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Luhm14e').values(
        publication='Luhm14.6').execute()

    db.Publications.update().where(db.Publications.c.publication == 'Mace13').values(
        publication='Mace13.6').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Mace13b').values(
        publication='Mace13.36').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Pinf14').values(
        publication='Pinf14.1931').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Pinf14a').values(
        publication='Pinf14.1009').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Scho10a').values(
        publication='Scho10.92').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Scho10b').values(
        publication='Scho10.8').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Tinn05a').values(
        publication='Tinn05.1171').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Tinn05b').values(
        publication='Tinn05.2326').execute()

    db.Publications.update().where(db.Publications.c.publication == 'Warr07').values(
        publication='Warr07.1400').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Warr07a').values(
        publication='Warr07.213').execute()


def convert_ref_name(reference):
    ref_dict = {'Burgasser_2000': 'Burg00.57',
                'Burgasser_2002': 'Burg02.421',
                'Burgasser_2003': 'Burg03.2487',
                'Burgasser_2006': 'Burg06.1067',
                'Burgasser_2004': 'Burg04.2856',
                'Burgasser_2010b': 'Burg02.421',  # Error in Leggett 2021
                'Burningham_2010a': 'Burn10.1885',
                'Burningham_2010b': 'Burn10.1885',
                'Burningham_2011': 'Burn11.90',
                'Cushing_2011': 'Cush11.50',
                'Delorme_2008': 'Delo08.961',
                'Dupuy_2015': 'Dupu15.102',
                'Gelino_2011': 'Geli11.57',
                'Kirkpatrick_2012': 'Kirk12',
                'Kirkpatrick_2013': 'Kirk13',
                'Liu_2011': 'Liu_11.108',
                'Lodieu_2007': 'Lodi07.1423',
                'Lodieu_2009': 'Lodi09.258',
                'Lodieu_2012': 'Lodi12.53',
                'Looper_2007': 'Loop07.1162',
                'this_work': 'Legg21',
                'Lucas_2010': 'Luca10',
                'Luhman_2012': 'Luhm12.135',
                'Luhman_2014': 'Luhm14.18',
                'Mace_2013a': 'Mace13.6',
                'Mace_2013b': 'Mace13.36',
                'Meisner_2020a': 'Meis20.74',
                'Meisner_2020b': 'Meis20.123',
                'Pinfield_2014a': 'Pinf14.1009',
                'Pinfield_2014b': 'Pinf14.1931',
                'Pinfield_Gromadzki_2014': 'Pinf14.priv',
                'Scholz_2010a': 'Scho10.8',
                'Scholz_2010b': 'Scho10.92',
                'Tinney_2005': 'Tinn05.2326',
                'Warren_2007': 'Warr07.1400'
                }

    # convert reference names
    first_ref = reference.split()[0]
    # print('Leggett Ref: ', first_ref)
    for leggett_ref, simple_ref in ref_dict.items():
        if first_ref == leggett_ref:
            first_ref = first_ref.replace(leggett_ref, simple_ref)
            # print('Replaced: ', first_ref)

    simple_search = find_publication(db, first_ref)
    if simple_search[0]:
        simple_ref = simple_search[1]
    else:
        msg = f'More than SIMPLE ref found for {first_ref}'
        raise SimpleError(msg)
    # print('Simple Ref: ', simple_ref)
    return simple_ref


if RECREATE_DB:
    add_publications(db)

legg21_table = ascii.read('scripts/ingests/apjac0cfet10_mrt.txt', format='mrt')

primary_list = ['WISE J014656.66+423410.0A']

binary_list = [
    "WISE J014656.66+423410.0B",
    "WISEPA J045853.89+643452.9B",
    "WISEPC J121756.91+162640.2B",
    "2MASS J122554.32-273946.6B",
    "CFBDS J145829.00+101343.0B",
    "2MASSI J155302.20+153236.0B",
    "WISEPA J171104.60+350036.8B"
]

source_strings = []
discovery_refs = []
ras = []
decs = []
other_references = []
spt_source_strings = []
sp_types = []
sp_type_refs = []

for row, source in enumerate(legg21_table):
    if not ma.is_masked(source['CBin']):
        source_string = f"{source['Survey']} J{source['RA']}{source['Decl.']}{source['CBin']}"
    else:
        source_string = f"{source['Survey']} J{source['RA']}{source['Decl.']}"
    source_strings.append(source_string)

    ra, dec = convert_radec_to_decimal(source)
    ras.append(ra)
    decs.append(dec)
    print(row, source_string, ra, dec)

    #  Deal with discovery ref and references
    discovery_ref = convert_ref_name(source['DisRef'])
    discovery_refs.append(discovery_ref)
    # Take care of secord discovery ref, if provided
    try:
        legg21_second_ref = source['DisRef'].split()[1]
        print('2nd: ', legg21_second_ref)
        second_ref = convert_ref_name(legg21_second_ref)
        other_references.append(second_ref)
        # print(f"other ref: {ref}")
    except IndexError:
        second_ref = ''
        other_references.append(None)

    #Add A components as Other Name
    if source_string in primary_list:
        source = find_source_in_db(db, source_string, ra, dec)
        names_data = [{'source': source,
                       'other_name': source_string}]
        db.Names.insert().execute(names_data)
        print(f'Added: {source_string}')

    #Add the B components directly.
    if source_string in binary_list:
        source_data = [{'source': source_string,
                        'ra': ra,
                        'dec': dec,
                        'reference': discovery_ref}
                        ]
        names_data = [{'source': source_string,
                       'other_name': source_string}]
        db.Sources.insert().execute(source_data)
        db.Names.insert().execute(names_data)
        print(f'Added: {source_string}')

    # SPECTRAL TYPES
    spt_source_strings.append(source_string)

    # convert Legg21 SpType to string
    legg21_sptype_code_base = source['SpType'].split('.')[0]
    half_int = source['SpType'].split('.')[1][0]
    if half_int == '5':
        half_int_string = '.5'
    elif half_int == '0':
        half_int_string = ''
    else:
        msg = f"Unexpected half integer {half_int} for {source_string}"
        raise SimpleError(msg)

    try:
        suffix = source['SpType'].split()[1]
    except IndexError:
        suffix = None

    if float(legg21_sptype_code_base) <= 9:
        sp_type_string = f"T{legg21_sptype_code_base}{half_int_string}"
    elif float(legg21_sptype_code_base) >= 10:
        sp_type_string = f"Y{legg21_sptype_code_base[1]}{half_int_string}"
    else:
        msg = "Unexpected spectral type code"
        raise SimpleError
    if suffix:
        sp_type_string = f"{sp_type_string} {suffix}"

    sp_types.append(sp_type_string)

    sp_type_ref = convert_ref_name(source['r_SpType'])
    sp_type_refs.append(sp_type_ref)
    # If 2nd Spectral Type Ref, add them both
    try:
        legg21_second_spt_ref = source['r_SpType'].split()[1]
        print('2nd Spt: ', legg21_second_spt_ref)
        second_spt_ref = convert_ref_name(legg21_second_spt_ref)
        spt_source_strings.append(source_string)
        sp_types.append(sp_type_string)
        sp_type_refs.append(second_spt_ref)
    except IndexError:
        second_spt_ref = ''

    print(source_string, discovery_ref, sp_type_ref,
          'SECOND Dis: ', second_ref, 'SECOND SpT: ', second_spt_ref)



    # OTHER NAMES
    if not ma.is_masked(source['OName']):
        other_names = source['OName']
        print(f'Other Names: {other_names}')

print(f'Sources to ingest: {len(source_strings)}')
print(f'SpTypes to ingest: {len(spt_source_strings)}')


if RECREATE_DB:
    ingest_sources(db, source_strings, ras=ras, decs=decs,
               references=discovery_refs, other_references=other_references)

    ingest_spectral_types(db, spt_source_strings, sp_types, sp_type_refs, regimes='nir')


# ingest NIR photometry

# ingest Spitzer photometry

# ingest WISE photometry

# ingest Other Names