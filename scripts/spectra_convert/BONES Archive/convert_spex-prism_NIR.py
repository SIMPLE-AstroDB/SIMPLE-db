import os
import csv

path  = "/Users/guanying/SIMPLE_Archive/SIMPLE-db/scripts/spectra_convert/BONES Archive/BONES SPECTRA"
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
    """
    metadata = {}
    filename = os.path.basename(filename)
    metadata['FILENAME'] = filename
    matched = False

    if filename.startswith("spex-prism_NIR_J0330+3505_Bardalez2014.txt"):
        metadata.update({
            'OBJECT': "LSPM J0330+3504",
            'RA_TARG': "52.5718321",
            'DEC_TARG': "35.0833273",
            'DATE-OBS': '2014-06-16',
            'AUTHOR': 'Bardalez Gagliuffi, D. C.',
            'Reference': 'Bard14',
        })
        file_converted += 1
        matched = True

    elif filename.startswith("spex-prism_NIR_J0330-2348_Bardalez2014.txt"):
        metadata.update({
            'OBJECT': "LEHPM 1-3365",
            'RA_TARG': "52.6651396",
            'DEC_TARG': "-23.8126937",
            'DATE-OBS': '2004-09-09',
            'AUTHOR': 'Bardalez Gagliuffi, D. C.',
            'Reference': 'Bard14',
        })
        file_converted += 1
        matched = True
    
    elif filename.startswith("spex-prism_NIR_J1416+1348B_Burg2010.txt"):
        metadata.update({
            'OBJECT': "ULAS J141623.94+134836.3",
            'RA_TARG': "214.0997590",
            'DEC_TARG': "13.8100890",
            'DATE-OBS': '2010-01-23',
            'AUTHOR': 'Burgasser, A. J.',
            'Reference': 'Burg10.2448',
        })
        file_converted += 1
        matched = True

    elif filename.startswith("spex-prism_NIR_J1541+5425_Burgasser2004.txt"):
        metadata.update({
            'OBJECT': "2MASS J15412408+5425598",
            'RA_TARG': "235.3503962",
            'DEC_TARG': "54.4333447",
            'DATE-OBS': '2003-05-21',
            'AUTHOR': 'Burgasser, A. J.',
            'Reference': 'Burg04.2856',
        })
        file_converted += 1
        matched = True
    
    elif filename.startswith("spex-prism_NIR_J1556+1300_Burgasser2004.txt"):
        metadata.update({
            'OBJECT': "2MASS J15561873+1300527",
            'RA_TARG': "239.0780633",
            'DEC_TARG': "13.0146447",
            'DATE-OBS': '2003-05-23',
            'AUTHOR': 'Burgasser, A. J.',
            'Reference': 'Burg04.2856',
        })
        file_converted += 1
        matched = True

    elif filename.startswith("spex-prism_NIR_J1640+1231_Burgasser2004.txt"):
        metadata.update({
            'OBJECT': "2MASS J16403197+1231068",
            'RA_TARG': "250.1331099",
            'DEC_TARG': "12.5184894",
            'DATE-OBS': '2003-05-21',
            'AUTHOR': 'Burgasser, A. J.',
            'Reference': 'Burg04.2856',
        })
        file_converted += 1
        matched = True

    elif filename.startswith("spex-prism_NIR_J1756+2815_Kirkpatrick2010.txt"):
        metadata.update({
            'OBJECT': "2MASS J17561080+2815238",
            'RA_TARG': "269.0448777",
            'DEC_TARG': "28.2565151",
            'DATE-OBS': '2005-10-20',
            'AUTHOR': 'Kirkpatrick, J. D.',
            'Reference': 'Kirk10',
        })
        file_converted += 1
        matched = True

    elif filename.startswith("spex-prism_NIR_J1826+3014_Bardalez2014.txt"):
        metadata.update({
            'OBJECT': "LSR J1826+3014", # the source cannot be found in Bard's paper, only in Burgasser's paper, info provided by https://cass.ucsd.edu/~ajb/browndwarfs/spexprism/html/all.html
            'RA_TARG': "276.5458466",
            'DEC_TARG': "30.2385765",
            'DATE-OBS': '2003-05-21',
            'AUTHOR': 'Bardalez Gagliuffi, D. C.', 
            'Reference': 'Burg04.2856', 
        })
        file_converted += 1
        matched = True
    
    elif filename.startswith("spex-prism_NIR_J0306-0330_Kirkpatrick2014.txt"):
        metadata.update({
            'OBJECT': "WISEA J030601.66-033059.0",
            'RA_TARG': "46.5059730",
            'DEC_TARG': "-3.5126280",
            'DATE-OBS': '2013-11-22',
            'AUTHOR': 'Kirkpatrick, J. D.',
            'Reference': 'Kirk14',
        })
        file_converted += 1
        matched = True
    
    elif filename.startswith("spex-prism_NIR_J0435+2115_Luhman2014.txt"):
        metadata.update({
            'OBJECT': "WISEA J043535.82+211508.9",
            'RA_TARG': "68.8966272",
            'DEC_TARG': "21.2552596",
            'DATE-OBS': '2013-12-26', 
            'AUTHOR': 'Luhman, K. L.',
            'Reference': 'Luhm14.126',
        })
        file_converted += 1
        matched = True
    
    elif filename.startswith("spex-prism_NIR_J0115+3130_Burgasser2004.txt"):
        metadata.update({
            'OBJECT': "2MASS J01151621+3130061",
            'RA_TARG': "18.8178538",
            'DEC_TARG': "31.5017099",
            'DATE-OBS': '2003-09-19',
            'AUTHOR': 'Burgasser, A. J.',
            'Reference': 'Burg04.2856',
        })
        file_converted += 1
        matched = True

    elif filename.startswith("spex-prism_NIR_J1013-1356_Burgasser2004.txt"):
        metadata.update({
            'OBJECT': "SSSPM J1013-1356",
            'RA_TARG': "153.2806102",
            'DEC_TARG': "-13.93923931",
            'DATE-OBS': '2004-03-12',
            'AUTHOR': 'Burgasser, A. J.',
            'Reference': 'Burg04.73',
        })
        file_converted += 1
        matched = True

    elif filename.startswith("spex-prism_NIR_J1444-2019_Kirkpatrick2016.txt"):
        metadata.update({
            'OBJECT': "SSSPM J1444-2019",
            'RA_TARG': "221.0847404",
            'DEC_TARG': "-20.3237455",
            'DATE-OBS': '2015-07-03',
            'AUTHOR': 'Kirkpatrick, J. D.',
            'Reference': 'Kirk16',
        })
        file_converted +=1
        matched = True

    elif filename.startswith("spex-prism_NIR_J1439+1839_Burgasser2004.txt"):
        metadata.update({
            'OBJECT': "LHS 377",
            'RA_TARG': "219.7512861",
            'DEC_TARG': "18.6607528",
            'DATE-OBS': '2004-03-12',
            'AUTHOR': 'Burgasser, A. J.',
            'Reference': 'Burg04.2856',
        })
        file_converted += 1
        matched = True
    
    elif filename.startswith("spex-prism_NIR_J1640+2922_Burgasser2004.txt"):
        metadata.update({
            'OBJECT': "2MASS J16403561+2922225",
            'RA_TARG': "250.1483975",
            'DEC_TARG': "29.3729562",
            'DATE-OBS': '2003-05-22',
            'AUTHOR': 'Burgasser, A. J.',
            'Reference': 'Burg04.2856',
        })
        file_converted += 1
        matched = True

    elif filename.startswith("spex-prism_NIR_J1158+0435_Kirkpatrick2010.txt"):
        metadata.update({
            'OBJECT': "2MASS J11582077+0435014",
            'RA_TARG': "179.5863881",
            'DEC_TARG': "4.5840000",
            'DATE-OBS': '2005-12-09',
            'AUTHOR': 'Kirkpatrick, J. D.',
            'Reference': 'Kirk10',
        })
        file_converted +=1
        matched = True

    elif filename.startswith("spex-prism_NIR_J0447-1946_Kirkpatrick2010.txt"):
        metadata.update({
            'OBJECT': "2MASS J04470652-1946392",
            'RA_TARG': "71.7770010",
            'DEC_TARG': "-19.7775004",
            'DATE-OBS': '2004-09-08',
            'AUTHOR': 'Kirkpatrick, J. D.',
            'Reference': 'Kirk10',
        })
        file_converted += 1
        matched = True

    elif filename.startswith("spex-prism_NIR_J0142+0523_Burgasser2004.txt"):
        metadata.update({
            'OBJECT': "2MASS J01423153+0523285",
            'RA_TARG': "25.6313242",
            'DEC_TARG': "5.3914170",
            'DATE-OBS': '2003-09-17',
            'AUTHOR': 'Burgasser, A. J.',
            'Reference': 'Burg04.2856',
        })
        file_converted += 1
        matched = True

    elif filename.startswith("spex-prism_NIR_J0452-2245_Burgasser2006.txt"):
        metadata.update({
            'OBJECT': "LEHPM 2-59",
            'RA_TARG': "73.0414713",
            'DEC_TARG': "-22.7525958",
            'DATE-OBS': '2004-09-09',
            'AUTHOR': 'Burgasser, A. J.',
            'Reference': 'Burg06',
        })  
        file_converted += 1
        matched = True
    
    elif filename.startswith("spex-prism_NIR_J0333+0014_Bardalez2014.txt"):
        metadata.update({
            'OBJECT': "ULAS J033351.10+001405.8",
            'RA_TARG': "53.4614426",
            'DEC_TARG': "0.2350580",
            'DATE-OBS': '2013-12-05',
            'AUTHOR': 'Bardalez Gagliuffi, D. C.',
            'Reference': 'Bard14',
        })
        file_converted += 1
        matched = True

    elif filename.startswith("spex-prism_NIR_J2347+0219_Kirkpatrick2010.txt"):
        metadata.update({
            'OBJECT': "2MASS J23470713+0219127",
            'RA_TARG': "356.7799271",
            'DEC_TARG': "2.3202519",
            'DATE-OBS': '2007-10-12',
            'AUTHOR': 'Kirkpatrick, J. D.',
            'Reference': 'Kirk10',
        })
        file_converted += 1
        matched = True

    elif filename.startswith("spex-prism_NIR_J0402+1730_Bardalez2014.txt"):
        metadata.update({
            'OBJECT': "LSPM J0402+1730",
            'RA_TARG': "60.6799496",
            'DEC_TARG': "17.5037902",
            'DATE-OBS': '2005-10-17',
            'AUTHOR': 'Bardalez Gagliuffi, D. C.',
            'Reference': 'Bard14',
            'WAVERANGE': '0.8-2.4',
        })
        file_converted += 1
        matched = True

    elif filename.startswith("spex-prism_NIR_J1256-0224_Burgasser2009.txt"):
        metadata.update({
            'OBJECT': "SDSS J125637.13-022452.4",
            'RA_TARG': "194.1547408",
            'DEC_TARG': "-2.4145184",
            'DATE-OBS': '2005-03-23',
            'AUTHOR': 'Burgasser, A. J.',
            'Reference': 'Burg09.148',
        })
        file_converted +=1
        matched = True

    elif filename.startswith("spex-prism_NIR_J0459+1540_Kirkpatrick2014.txt"):
        metadata.update({
            'OBJECT': "WISEA J045921.21+154059.2",
            'RA_TARG': "74.8371636",
            'DEC_TARG': "15.6846616",
            'DATE-OBS': '2013-11-22',
            'AUTHOR': 'Kirkpatrick, J. D.',
            'Reference': 'Kirk14',
        })
        file_converted += 1
        matched = True

    elif filename.startswith("spex-prism_NIR_J2036+5059_Burgasser2004.txt"):
        metadata.update({
            'OBJECT': "2MASS J20362165+5100051",
            'RA_TARG': "309.0900582",
            'DEC_TARG': "51.0013042",
            'DATE-OBS': '2003-09-18',
            'AUTHOR': 'Burgasser, A. J.',
            'Reference': 'Burg04.2856',
        })
        file_converted += 1
        matched = True
    
    elif filename.startswith("spex-prism_NIR_J2331+4607_Bardalez2014.txt"):
        metadata.update({
            'OBJECT': "LSPM J2331+4607N",
            'RA_TARG': "352.8383333",
            'DEC_TARG': "46.1150000",
            'DATE-OBS': '2005-09-17',
            'AUTHOR': 'Bardalez Gagliuffi, D. C.',
            'Reference': 'Bard14',
        })
        file_converted += 1
        matched = True

    elif filename.startswith("spex-prism_NIR_J2040+6959_Luhman2014.txt"):
        metadata.update({
            'OBJECT': "WISEA J204027.30+695924.1",
            'RA_TARG': "310.1004436",
            'DEC_TARG': "69.9850469",
            'DATE-OBS': '2013-12-28',
            'AUTHOR': 'Luhman, K. L.',
            'Reference': 'Luhm14.126',
        })
        file_converted += 1
        matched = True

    elif filename.startswith("spex-prism_NIR_J1626+3925_Burgasser2004.txt"):
        metadata.update({
            'OBJECT': "2MASS J16262034+3925190",
            'RA_TARG': "246.5838887",
            'DEC_TARG': "39.4220802",
            'DATE-OBS': '2004-07-23',
            'AUTHOR': 'Burgasser, A. J.',
            'Reference': 'Burg04.73',
        })
        file_converted += 1
        matched = True

    if matched:
        metadata['BUNIT'] = 'erg / (cm2 s Angstrom)'
        metadata['INSTRUME'] = 'SpeX'   
        metadata['Regime']= 'nir'
        metadata['TELESCOP'] = 'IRTF'
        metadata['TUNIT1'] = 'Angstrom'
        metadata_list.append(metadata)

path = "/Users/guanying/SIMPLE_Archive/SIMPLE-db/scripts/spectra_convert/BONES Archive/BONES_Archive.csv"
output_csv = os.path.join(os.path.dirname(path), 'BONES_Archive.csv')

if metadata_list:
    fieldnames = [
        "FILENAME","OBJECT","RA_TARG","DEC_TARG","DATE-OBS","TELESCOP","INSTRUME","AUTHOR","Reference","BUNIT","Regime","WAVERANGE","TUNIT1"
    ]
    with open(output_csv, mode='a', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')

        for metadata in metadata_list:
            for key in fieldnames:
                metadata.setdefault(key, '')
            writer.writerow(metadata)

    print(f"Metadata added for {len(metadata_list)} files")


