import os
import csv

path = "/Users/guanying/SIMPLE_Archive/SIMPLE-db/scripts/spectra_convert/BONES Archive/BONES SPECTRA/"

"""
This convert function is to convert the data into csv, easier to make header into FITS and ingest spectra into SIMPLE
"""

# Process source with FIRE telescope
file_converted = 0
metadata_list = []

for filename in os.listdir(path):
    """
        Things need to be ingested into SIMPLE
        source: WISEA J101329.72-724619.2, 
                WISEA J135501.90-825838.9,
                WISEA J041451.67-585456.7
                
        publication
            reference             doi                   
            Zhan17.2438   10.1093/mnras/stw2438 
    """
    metadata = {}
    matched = False 
    filename = os.path.basename(filename)
    metadata['FILENAME'] = filename
    
    if filename.startswith("FIRE_1013m7246"):
        metadata.update({
            'OBJECT': "WISEA J101329.72-724619.2",
            'RA_TARG': "153.3770190",
            'DEC_TARG': "-72.7730870",
            'DATE-OBS': '2015-02-08',
            'AUTHOR': 'Kirkpatrick et al.',
            'Reference': 'Kirk16',
        })
        matched = True

    elif filename.startswith("fireprism"):
        if "fireprism_NIR_J0414-5854.csv" in filename:
            metadata.update({
                'OBJECT': "WISEA J041451.67-585456.7",
                'RA_TARG': "63.7152900",
                'DEC_TARG': "-58.9157500",
                'DATE-OBS': '2020-02-12',
                'AUTHOR': 'Schneider et al 2020',
                'Reference': 'Schn20'
            })
            matched = True

        elif "fireprism_NIR_ULASJ124947.04+095019.8.txt" in filename:
            metadata.update({
                'OBJECT': "ULAS J124947.04+095019.8",
                'RA_TARG': "192.4454711",
                'DEC_TARG': "9.8382541",
                'DATE-OBS': '2012-05-08',
                'AUTHOR': 'Zhang et al. 2017a',
                'Reference': 'Zhan17.2438'
            })
            matched = True

        elif "fireprism_NIR_ULASJ133836.97-022910.7.txt" in filename:
            metadata.update({
                'OBJECT': "SDSS J133837.01-022908.4",
                'RA_TARG': "204.6542120",
                'DEC_TARG': "-2.4856800",
                'DATE-OBS': '2012-05-08',
                'AUTHOR': 'Zhang et al. 2017a',
                'Reference': 'Zhan17.2438'
            })
            matched = True

    if matched:
        metadata['BUNIT'] = 'erg / (cm2 s Angstrom)'
        metadata['TELESCOP'] = 'Magellan I Baade'
        metadata['INSTRUME'] = 'FIRE'
        metadata['Regime'] = "nir"
        metadata_list.append(metadata)

path = "/Users/guanying/SIMPLE_Archive/SIMPLE-db/scripts/spectra_convert/BONES Archive/BONES_Archive.csv"
output_csv = os.path.join(os.path.dirname(path), 'BONES_Archive.csv')

if metadata_list:
    fieldnames = [
        "FILENAME","OBJECT","RA_TARG","DEC_TARG","DATE-OBS","TELESCOP","INSTRUME","AUTHOR","Reference","BUNIT","Regime"

    ]
    with open(output_csv, mode='a', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')

        for metadata in metadata_list:
            for key in fieldnames:
                metadata.setdefault(key, '')
            writer.writerow(metadata)

    print(f"Metadata added for {len(metadata_list)} files")