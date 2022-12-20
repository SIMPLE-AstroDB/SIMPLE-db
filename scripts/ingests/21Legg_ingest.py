from astropy.io import ascii
from scripts.ingests.ingest_utils import *
from astropy.utils.exceptions import AstropyWarning
import numpy.ma as ma
from sqlalchemy import and_

warnings.simplefilter('ignore', category=AstropyWarning)

SAVE_DB = True  # save the data files in addition to modifying the .db file
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

    update_ref_dict = {
        "Burn10": "Burn10.1952",
        "Burn10b": "Burn10.1885",
        "Burn11": "Burn11.3590",
        "Burn11b": "Burn11.90",
        "Burg00a": "Burg00.57",
        "Burg00b": "Burg00.473",
        "Burg00c": "Burg00.1100",
        "Burg02a": "Burg02.421",
        "Burg02b": "Burg02.2744",
        "Burg02c": "Burg02.151",
        "Burg03d": "Burg03.510",
        "Burg03b": "Burg03.512",
        "Burg03c": "Burg03.1186",
        "Burg03e": "Burg03.2487",
        "Burg03a": "Burg03.850",
        "Burg04c": "Burg04.73",
        "Burg04a": "Burg04.827",
        "Burg04b": "Burg04.2856",
        "Burg04d": "Burg04.191",
        "Burg06c": "Burg06.1095",
        "Burg06e": "Burg06.585",
        "Burg06b": "Burg06.1067",
        "Burg06a": "Burg06.1007",
        "Burg06d": "Burg06.1485",
        "Cush11": "Cush11.50",
        "Delo08a": "Delo08.961",
        "Delo08b": "Delo08.469",
        "Dupu15a": "Dupu15.102",
        "Dupu15b": "Dupu15.56",
        "Geli11": "Geli11.57",
        "Geli11b": "Geli11.871",
        "Lodi09b": "Lodi09.1631",
        "Lodi09d": "Lodi09.258",
        "Lodi07a": "Lodi07.1423",
        "Lodi07b": "Lodi07.372",
        "Lodi12a": "Lodi12.1495",
        "Lodi12b": "Lodi12.105",
        "Lodi12d": "Lodi12.53",
        "Loop07a": "Loop07.1162",
        "Loop07b": "Loop07.97",
        "Liu_11a": "Liu_11.32",
        "Liu_11b": "Liu_11.108",
        "Luhm12": "Luhm12.135",
        "Luhm12d": "Luhm12.152",
        "Luhm14b": "Luhm14.18",
        "Luhm14d": "Luhm14.16",
        "Luhm14a": "Luhm14.4",
        "Luhm14c": "Luhm14.126",
        "Luhm14e": "Luhm14.6",
        "Mace13": "Mace13.6",
        "Mace13b": "Mace13.36",
        "Pinf14": "Pinf14.1931",
        "Pinf14a": "Pinf14.1009",
        "Scho10a": "Scho10.92",
        "Scho10b": "Scho10.8",
        "Tinn05a": "Tinn05.1171",
        "Tinn05b": "Tinn05.2326",
        "Warr07": "Warr07.1400",
        "Warr07a": "Warr07.213",
    }

    for old_ref, new_ref in update_ref_dict.items():
        db.Publications.update().where(db.Publications.c.publication == old_ref).values(
            publication=new_ref).execute()

    db.Publications.delete().where(db.Publications.c.publication == 'Lodi12').execute()


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

primary_list = ['WISE J014656.66+423410.0A',
                'WISEPA J045853.89+643452.9A',
                "WISEPC J121756.91+162640.2A",
                "2MASS J122554.32-273946.6A",
                "CFBDSIR J145829.00+101343.0A",
                '2MASSI J155302.20+153236.0A',
                "WISEPA J171104.60+350036.8A"
                ]

binary_list = [
    "WISE J014656.66+423410.0B",
    "WISEPA J045853.89+643452.9B",
    "WISEPC J121756.91+162640.2B",
    "2MASS J122554.32-273946.6B",
    "CFBDS J145829.00+101343.0B",
    "2MASSI J155302.20+153236.0B",
    "WISEPA J171104.60+350036.8B"
]

source_strings, discovery_refs, ras, decs, other_references = [], [], [], [], []
# spectral type variables
spt_source_strings, sp_types, sp_type_refs = [], [], []
# spitzer phot variables
iraci1_source_strings, iraci2_source_strings, iraci3_source_strings, iraci4_source_strings = [], [], [], []
iraci1_mags, iraci2_mags, iraci3_mags, iraci4_mags = [], [], [], []
iraci1_mags_err, iraci2_mags_err, iraci3_mags_err, iraci4_mags_err = [], [], [], []
iraci1_refs, iraci2_refs, iraci3_refs, iraci4_refs = [], [], [], []

for row_number, source in enumerate(legg21_table):
    if not ma.is_masked(source['CBin']):
        source_string = f"{source['Survey']} J{source['RA']}{source['Decl.']}{source['CBin']}"
    else:
        source_string = f"{source['Survey']} J{source['RA']}{source['Decl.']}"
    source_strings.append(source_string)

    ra, dec = convert_radec_to_decimal(source)
    ras.append(ra)
    decs.append(dec)
    print(row_number, source_string, ra, dec)

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

    # Add A components as Other Name
    if source_string in primary_list:
        source_matches = find_source_in_db(db, source_string, ra, dec)
        if len(source_matches) == 1:
            source_match = source_matches[0]
        else:
            shortest = 100
            source_short = ''
            for source_match in source_matches:
                if len(source_match) < shortest:
                    source_short = source_match
                    shortest = len(source_short)
            source_match = source_short
        names_data = [{'source': source_match,
                       'other_name': source_string}]
        db.Names.insert().execute(names_data)
        print(f'Added to Names: {source_match, source_string}')

    # Add the B components directly.
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
        print(f'Added Source: {source_string}')

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
    print(f"kelle's way: {half_int_string}")

    # Will's way
    w_half_int_string = ''
    try:
        half_int = int(half_int)
        if half_int:
            w_half_int_string += f'.{half_int}'
    except ValueError:
        msg = f"Unexpected half integer {half_int} for {source_string}"
        raise SimpleError(msg)
    print(f'Wills way: {w_half_int_string}')

    if w_half_int_string != half_int_string:
        msg = f'{w_half_int_string} and {half_int_string} are not the same'
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

    # IRAC photometry
    if not ma.is_masked(source['3.6mag']):
        iraci1_source_strings.append(source_string)
        iraci1_mags.append(source['3.6mag'])
        iraci1_mags_err.append(source['e_3.6mag'])
        iraci1_ref = convert_ref_name(source['SpitRef'])
        iraci1_refs.append(iraci1_ref)

    if not ma.is_masked(source['4.5mag']):
        iraci2_source_strings.append(source_string)
        iraci2_mags.append(source['4.5mag'])
        iraci2_mags_err.append(source['e_4.5mag'])
        iraci2_ref = convert_ref_name(source['SpitRef'])
        iraci2_refs.append(iraci2_ref)

    if not ma.is_masked(source['5.8mag']):
        iraci3_source_strings.append(source_string)
        iraci3_mags.append(source['5.8mag'])
        iraci3_mags_err.append(source['e_5.8mag'])
        iraci3_ref = convert_ref_name(source['SpitRef'])
        iraci3_refs.append(iraci3_ref)

    if not ma.is_masked(source['8.0mag']):
        iraci4_source_strings.append(source_string)
        iraci4_mags.append(source['8.0mag'])
        iraci4_mags_err.append(source['e_8.0mag'])
        iraci4_ref = convert_ref_name(source['SpitRef'])
        iraci4_refs.append(iraci4_ref)

print(f'Sources to ingest: {len(source_strings)}')
print(f'SpTypes to ingest: {len(spt_source_strings)}')


if RECREATE_DB:
    ingest_sources(db, source_strings, ras=ras, decs=decs,
               references=discovery_refs, other_references=other_references)

    ingest_spectral_types(db, spt_source_strings, sp_types, sp_type_refs, regimes='nir')

    # ingest Spitzer photometry
    ingest_photometry(db, iraci1_source_strings, 'IRAC.I1', iraci1_mags, iraci1_mags_err,
                      iraci1_refs, ucds='em.IR.3-4um', telescope='Spitzer', instrument='IRAC',
                      raise_error=False)

    ingest_photometry(db, iraci2_source_strings, 'IRAC.I2', iraci2_mags, iraci2_mags_err,
                      iraci2_refs, ucds='em.IR.4-8um', telescope='Spitzer', instrument='IRAC', raise_error=False)

    ingest_photometry(db, iraci3_source_strings, 'IRAC.I3', iraci3_mags, iraci3_mags_err,
                      iraci3_refs, ucds='em.IR.4-8um', telescope='Spitzer', instrument='IRAC', raise_error=False)

    ingest_photometry(db, iraci4_source_strings, 'IRAC.I4', iraci4_mags, iraci4_mags_err,
                      iraci4_refs, ucds='em.IR.8-15um', telescope='Spitzer', instrument='IRAC', raise_error=False)

# not ingesting NIR photometry at the moment. Data is too heterogeneous.

# not going to ingest WISE photometry. Should get direct from IRSA

# remove duplicate spectral types
query = "SELECT source, reference, regime, spectral_type_code, count(*) FROM SpectralTypes where regime like 'nir%' " \
        "Group by source, reference, spectral_type_code " \
        "having count(*)>1 " \
        "ORDER by source"

dupe_types = db.sql_query(query, fmt='astropy')
# make sure it's 103
for row in dupe_types:
    db.SpectralTypes.delete().where(and_(
    db.SpectralTypes.c.source == row['source'],
    db.SpectralTypes.c.reference == row['reference'],
    db.SpectralTypes.c.regime == 'nir_UCD'))\
        .execute()

# WRITE THE JSON FILES
if SAVE_DB:
    db.save_database(directory='data/')