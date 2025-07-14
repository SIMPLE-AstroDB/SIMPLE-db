import os
import csv

path  = "/Users/guanying/SIMPLE_Archive/SIMPLE-db/scripts/spectra_convert/BONES Archive/"
output_csv = os.path.join(os.path.dirname(path), 'BONES_Archive.csv')

"""
This convert function is to convert the data into csv, easier to make header into FITS and ingest spectra into SIMPLE
"""

# Process source with NIR spectra
file_converted = 0
metadata_list = []

for filename in os.listdir(path):
    """
    Things need to be ingested into SIMPLE:
    Publication: Mace13, The Exemplar T8 Subdwarf Companion of Wolf 1130, bibcode: 2013ApJ...777...36M, doi: 10.1088/0004-637X/777/1/36
    """
    metadata = {}
    filename = os.path.basename(filename)
    metadata['FILENAME'] = filename

    if filename.startswith("LRIS_1333"):
        metadata.update({
            'OBJECT': "2MASS J13334822+2735115",
            'RA_TARG': "203.4510080",
            'DEC_TARG': "27.5858030",
            'DATE-OBS': '2014-06-16',
            'AUTHOR': 'Kirkpatrick et al.',
            'Reference': 'Kirk16',
            'TELESCOPE': 'Keck I',
            'INSTRUME': 'LRIS',
        })
        file_converted += 1

    elif filename.startswith("mosfire_NIR_Wolf1130B.txt"):
        metadata.update({
            'OBJECT': "WISE J200520.38+542433.9",
            'RA_TARG': "301.3407439",
            'DEC_TARG': "54.4120205",
            'DATE-OBS': '2012-10-12',
            'AUTHOR': 'Mace et al.',
            'Reference': 'Mace13',
            'TELESCOPE': 'Keck I',
            'INSTRUME': 'Mosfire',
        })
        file_converted += 1

    elif filename.startswith("NIR_J1810-1009.txt"):
        metadata.update({
            'OBJECT': "WISEA J181006.18-101000.5",
            'RA_TARG': "272.5257500",
            'DEC_TARG': "-10.1668100",
            'DATE-OBS': '2019-08-13',
            'AUTHOR': 'Schneider et al.',
            'Reference': 'Schn20',
            'TELESCOPE': 'Keck II',
            'INSTRUME': 'NIRES',
        })
        file_converted += 1

    elif filename.startswith("NIR_J1316+0755_Burn2014.txt"):
        metadata.update({
            'OBJECT': "ULAS J131610.28+075553.0",
            'RA_TARG': "199.0428300",
            'DEC_TARG': "7.9313900",
            'DATE-OBS': '2011-04-19',
            'AUTHOR': 'Burningham et al.',
            'Reference': 'Burn14',
            'TELESCOPE': 'Gemini North',
            'INSTRUME': 'GNIRS'
        })
        file_converted += 1

    elif filename.startswith("nirspec_0014m0838.txt"):
        metadata.update({
            'OBJECT': "WISEA J001450.17-083823.4",
            'RA_TARG': "3.7046392",
            'DEC_TARG': "-8.6390397",
            'DATE-OBS': '2014-12-03',
            'AUTHOR': 'Kirkpatrick et al.',
            'Reference': 'Krik16',
            'TELESCOPE': 'Keck II',
            'INSTRUME': 'NIRSPEC'
        })
        file_converted += 1
    
    elif filename.startswith("nires_NIR_J1553+6934_20200707.txt"):
        metadata.update({
            'OBJECT': "WISE J155349.98+693355.2",
            'RA_TARG': "238.4582860",
            'DEC_TARG': "69.5653400",
            'DATE-OBS': '2020-07-07',
            'AUTHOR': 'Meisner et al.',
            'Reference': 'Meis21',
            'TELESCOPE': 'Keck II',
            'INSTRUME': 'NIRES'
        })
        file_converted += 1

    elif filename.startswith("nires_NIR_GJ576B_20220611.txt"):
        metadata.update({
            'OBJECT': "ULAS J150457.65+053800.8",
            'RA_TARG': "226.2402340",
            'DEC_TARG': "5.6335630",
            'DATE-OBS': '2022-06-11',
            'AUTHOR': 'Zhang et al.',
            'Reference': 'Zhan18.2054',
            'TELESCOPE': 'Keck II',
            'INSTRUME': 'NIRES'
        }) 
        file_converted += 1
    
    elif filename.startswith("nires_NIR_J0532+8246_20220119.csv"):
        metadata.update({
            'OBJECT': "2MASS J05325346+8246465",
            'RA_TARG': "83.2268105",
            'DEC_TARG': "82.7792114",
            'DATE-OBS': '2022-01-19',
            'AUTHOR': 'Burgasser et al.',
            'Reference': '', # Burgasser in prep
            'TELESCOPE': 'Keck II',
            'INSTRUME': 'NIRES'
        })
        file_converted += 1
    
    elif filename.startswith("NIR_J0850-0221.csv"):
        metadata.update({
            'OBJECT': "2MASS J08503941-0221528",
            'RA_TARG': "132.6641425",
            'DEC_TARG': "-2.3646949",
            'DATE-OBS': '2016-02-24',
            'AUTHOR': 'Greci et al.',
            'Reference': 'Grec19',
            'TELESCOPE': 'IRTF',
            'INSTRUME': 'SpeX'
        })
        file_converted += 1

    elif filename.startswith("NIR_J0948-2903.csv"):
        metadata.update({
            'OBJECT': "2MASS J09481253-2903268",
            'RA_TARG': "147.0521206",
            'DEC_TARG': "-29.0575300",
            'DATE-OBS': '2016-02-24',
            'AUTHOR': 'Greci et al.',
            'Reference': 'Grec19',
            'TELESCOPE': 'IRTF',
            'INSTRUME': 'SpeX'
        })
        file_converted += 1

    elif filename.startswith("NIR_J1820+2021.csv"):
        metadata.update({
            'OBJECT': "2MASS J18201045+2021263",
            'RA_TARG': "275.0435869",
            'DEC_TARG': "20.3573279",
            'DATE-OBS': '2016-10-24',
            'AUTHOR': 'Greci et al.',
            'Reference': 'Grec19',
            'TELESCOPE': 'IRTF',
            'INSTRUME': 'SpeX'
        })
        file_converted += 1
    
    elif filename.startswith("NIR_J1552+0951.csv"):
        metadata.update({
            'OBJECT': "2MASS J15522538+0951585",
            'RA_TARG': "238.1057838",
            'DEC_TARG': "9.8662941",
            'DATE-OBS': '2016-06-20',
            'AUTHOR': 'Greci et al.',
            'Reference': 'Grec19',
            'TELESCOPE': 'IRTF',
            'INSTRUME': 'SpeX'
        })
        file_converted += 1

    elif filename.startswith("NIR_J1219+0810.csv"):
        metadata.update({
            'OBJECT': "2MASS J12191495+0810307",
            'RA_TARG': "184.8123102",
            'DEC_TARG': "8.1751871",
            'DATE-OBS': '2016-02-24',
            'AUTHOR': 'Greci et al.',
            'Reference': 'Grec19',
            'TELESCOPE': 'IRTF',
            'INSTRUME': 'SpeX'
        })
        file_converted += 1

    elif filename.startswith("NIR_J1440-2225.csv"):
        metadata.update({
            'OBJECT': "2MASS J14405684-2225149",
            'RA_TARG': "220.2367562",
            'DEC_TARG': "-22.4209250",
            'DATE-OBS': '2016-06-20',
            'AUTHOR': 'Greci et al.',
            'Reference': 'Grec19',
            'TELESCOPE': 'IRTF',
            'INSTRUME': 'SpeX'
        })
        file_converted += 1

    elif filename.startswith("NIR_J1245+6016.csv"):
        metadata.update({
            'OBJECT': "2MASS J12451711+6016103",
            'RA_TARG': "191.3212584",
            'DEC_TARG': "60.2695206",
            'DATE-OBS': '2016-02-24',
            'AUTHOR': 'Greci et al.',
            'Reference': 'Grec19',
            'TELESCOPE': 'IRTF',
            'INSTRUME': 'SpeX'
        })
        file_converted += 1

    elif filename.startswith("NIR_J0301-2319.csv"):
        metadata.update({
            'OBJECT': "WISEA J030119.39-231921.1",
            'RA_TARG': "45.3298620",
            'DEC_TARG': "-23.3220880",
            'DATE-OBS': '2016-08-03',
            'AUTHOR': 'Greci et al.',
            'Reference': 'Grec19',
            'TELESCOPE': 'IRTF',
            'INSTRUME': 'SpeX'
        })
        file_converted += 1

    elif filename.startswith("NIR_J1439-1100.csv"):
        metadata.update({
            'OBJECT': "2MASS J14394298-1100430",
            'RA_TARG': "219.9290601",
            'DEC_TARG': "-11.0120408",
            'DATE-OBS': '2016-02-24',
            'AUTHOR': 'Greci et al.',
            'Reference': 'Grec19',
            'TELESCOPE': 'IRTF',
            'INSTRUME': 'SpeX'
        })
        file_converted += 1
    
    elif filename.startswith("NIR_J1220+6205.csv"):
        metadata.update({
            'OBJECT': "2MASS J12204296+6205315",
            'RA_TARG': "185.1787143",
            'DEC_TARG': "62.0919875",
            'DATE-OBS': '2016-06-20',
            'AUTHOR': 'Greci et al.',
            'Reference': 'Grec19',
            'TELESCOPE': 'IRTF',
            'INSTRUME': 'SpeX'
        })
        file_converted += 1

    elif filename.startswith("NIR_J1035-0771.csv"):
        metadata.update({
            'OBJECT': "2MASS J10353492-0711479",
            'RA_TARG': "158.8955170",
            'DEC_TARG': "-7.1966470",
            'DATE-OBS': '2016-03-28',
            'AUTHOR': 'Greci et al.',
            'Reference': 'Grec19',
            'TELESCOPE': 'IRTF',
            'INSTRUME': 'SpeX'
        })
        file_converted += 1
    
    elif filename.startswith("NIR_J0004-2604.csv"):
        metadata.update({
            'OBJECT': "WISEA J000430.66-260402.3",
            'RA_TARG': "1.1277310",
            'DEC_TARG': "-26.0665630",
            'DATE-OBS': '2016-08-03',
            'AUTHOR': 'Greci et al.',
            'Reference': 'Grec19',
            'TELESCOPE': 'IRTF',
            'INSTRUME': 'SpeX'
        })
        file_converted += 1

    metadata['BUNIT'] = 'erg / (cm2 s Angstrom)'
    metadata['REGIME']= 'nir'


metadata_list.append(metadata)

path = "/Users/guanying/SIMPLE_Archive/SIMPLE-db/scripts/spectra_convert/BONES Archive/BONES_Archive.csv"
output_csv = os.path.join(os.path.dirname(path), 'BONES_Archive.csv')

if metadata_list:
    with open(output_csv, mode='a', newline='') as csvfile:
        fieldnames = metadata_list[0].keys()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        for metadata in metadata_list:
            writer.writerow(metadata)

    print(f"Metadata appended to: {output_csv}")