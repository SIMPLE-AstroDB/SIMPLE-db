import os
import csv

path  = "/Users/guanying/SIMPLE_Archive/SIMPLE-db/scripts/spectra_convert/BONES Archive/BONES SPECTRA"
output_csv = os.path.join(os.path.dirname(path), 'BONES_Archive.csv')

"""
This convert function is to convert the data into csv, easier to make header into FITS and ingest spectr into SIMPLE
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

    if filename.startswith("Xshooter_NIR_ULASJ020858.62+020657.0.txt"):
        metadata.update({
            'OBJECT': 'ULAS J020858.62+020657.0',
            'RA_TARG': '32.2442580',
            'DEC_TARG': '2.1158600',
            'DATE-OBS': '2014-11-29',
            'AUTHOR': 'Zhang et al.',
            'Reference': 'Zhan18.2054',
        })
        matched = True
        file_converted += 1

    elif filename.startswith("Xshooter_NIR_ULASJ223302.03+062030.8.txt"):
        metadata.update({
            'OBJECT': 'ULAS J223302.03+062030.8',
            'RA_TARG': '338.2584980',
            'DEC_TARG': '6.3419010',
            'DATE-OBS': '2014-11-29',
            'AUTHOR': 'Zhang et al.',
            'Reference': 'Zhan18.2054',
        })
        matched = True
        file_converted += 1

    elif filename.startswith("Xshooter_NIR_SDSSJ010448.46+153501.8.txt"):
        metadata.update({
            'OBJECT': 'SDSS J010448.46+153501.8',
            'RA_TARG': '16.2019570',
            'DEC_TARG': '15.5838600',
            'DATE-OBS': '2014-11-29',
            'AUTHOR': 'Zhang et al.',
            'Reference': 'Zhan18.2054',
        })
        matched = True
        file_converted += 1

    elif filename.startswith("Xshooter_NIR_ULASJ141203.85+121609.9.txt"):
        metadata.update({
            'OBJECT': 'ULAS J141203.85+121609.9',
            'RA_TARG': '213.0165460',
            'DEC_TARG': '12.2694480',
            'DATE-OBS': '2015-02-25',
            'AUTHOR': 'Zhang et al.',
            'Reference': 'Zhan18.2054',
        })
        matched = True
        file_converted += 1
    
    elif filename.startswith("Xshooter_NIR_2MASSJ06164006-6407194.txt"):
        metadata.update({
            'OBJECT': '2MASS J06164006-6407194', # This item is mentioned in the research saying OSIRIS, not XShooter
            'RA_TARG': '94.1669200',
            'DEC_TARG': '-64.1220800',
            'DATE-OBS': '2007-03-12',
            'AUTHOR': 'Michael C. Cushing et al.',
            'Reference': 'Cush09',
        })
        matched = True
        file_converted += 1

    elif filename.startswith("Xshooter_NIR_ULASJ021642.96+004005.7.txt"):
        metadata.update({
            'OBJECT': 'ULAS J021642.97+004005.6',
            'RA_TARG': '34.1791030',
            'DEC_TARG': '0.6683320',
            'DATE-OBS': '2014-02-17',
            'AUTHOR': 'Zhang et al.',
            'Reference': 'Zhan17.3040',
        })
        matched = True
        file_converted += 1
    
    elif filename.startswith("Xshooter_NIR_ULASJ230711.01+014447.1.txt"):
        metadata.update({
            'OBJECT': 'ULAS J230711.01+014447.1',
            'RA_TARG': '346.7958890',
            'DEC_TARG': '1.7464400',
            'DATE-OBS': '2015-09-11',
            'AUTHOR': 'Zhang et al.',
            'Reference': 'Zhan18.2054',
        })
        matched = True
        file_converted += 1

    elif filename.startswith("Xshooter_NIR_ULASJ151913.03-000030.0.txt"):
        metadata.update({
            'OBJECT': 'ULAS J151913.03-000030.0',
            'RA_TARG': '229.8043280',
            'DEC_TARG': '-0.0083600',
            'DATE-OBS': '2016-03-22',
            'AUTHOR': 'Zhang et al.',
            'Reference': 'Zhan17.3040',
        })
        matched = True
        file_converted += 1

    elif filename.startswith("Xshooter_NIR_ULASJ024035.36+060629.3.txt"):
        metadata.update({
            'OBJECT': 'ULAS J024035.36+060629.3',
            'RA_TARG': '40.1475764',
            'DEC_TARG': '6.1083086',
            'DATE-OBS': '2015-01-17',
            'AUTHOR': 'Zhang et al.',
            'Reference': 'Zhan18.2054',
        })
        matched = True
        file_converted += 1
    
    elif filename.startswith("Xshooter_NIR_2MASSJ06453153-6646120.txt"):
        metadata.update({
            'OBJECT': '2MASS J06453153-6646120',
            'RA_TARG': '101.3813840',
            'DEC_TARG': '-66.7700120',
            'DATE-OBS': '2016-02-19',
            'AUTHOR': 'Zhang et al.',
            'Reference': 'Zhan18.2054',
        })
        matched = True
        file_converted += 1

    elif filename.startswith("Xshooter_NIR_ULASJ130710.22+151103.4.txt"):
        metadata.update({
            'OBJECT': 'ULAS J130710.22+151103.4',
            'RA_TARG': '196.7925850',
            'DEC_TARG': '15.1843030',
            'DATE-OBS': '2018-04-23',
            'AUTHOR': 'Zhang et al.',
            'Reference': 'Zhan18.2054',
            'WAVERANGE': '0-2500'
        })
        matched = True
        file_converted += 1

    elif filename.startswith("Xshooter_NIR_ULASJ135058.85+081506.8.txt"):
        metadata.update({
            'OBJECT': 'ULAS J135058.86+081506.8', # This item is mentioned in the paper saying OSIRIS, not XShooter
            'RA_TARG': '207.7452480',
            'DEC_TARG': '8.2519110',
            'DATE-OBS': '2009-04-30.', # Obtained in Lodi's paper
            'AUTHOR': 'Burgasser et al.',
            'Reference': 'Burg04.2856',
            'WAVERANGE': '1000-2500'

        })
        matched = True
        file_converted += 1

    if matched:
        metadata['INSTRUME'] = "XShooter"
        metadata['TELESCOP'] = "ESO VLT"
        metadata['BUNIT'] = 'erg / (cm2 s Angstrom)'
        metadata['Regime']= 'nir'
        metadata['TUNIT1'] = 'nm'
        metadata_list.append(metadata)


path = "/Users/guanying/SIMPLE_Archive/SIMPLE-db/scripts/spectra_convert/BONES Archive/BONES_Archive.csv"
output_csv = os.path.join(os.path.dirname(path), 'BONES_Archive.csv')

if metadata_list:
    fieldnames = [
        "FILENAME","OBJECT","RA_TARG","DEC_TARG","DATE-OBS","TELESCOP","INSTRUME","AUTHOR","Reference","BUNIT","Regime","WAVERANGE","TUNIT1"
    ]
    with open(output_csv, mode='a', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames,extrasaction='ignore')

        for metadata in metadata_list:
            for key in fieldnames:
                metadata.setdefault(key, '')
            writer.writerow(metadata)

    print(f"Metadata added for {len(metadata_list)} files")