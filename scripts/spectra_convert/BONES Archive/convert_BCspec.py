import os
import csv

files = ["/Users/guanying/SIMPLE_Archive/SIMPLE-db/scripts/spectra_convert/BONES Archive/BONES SPECTRA/BCSpec_1227m0447.txt",
         "/Users/guanying/SIMPLE_Archive/SIMPLE-db/scripts/spectra_convert/BONES Archive/BONES SPECTRA/BCSpec_1411m4524.txt",
         "/Users/guanying/SIMPLE_Archive/SIMPLE-db/scripts/spectra_convert/BONES Archive/BONES SPECTRA/BCSpec_1614m8151.txt"]

metadata_list = []

"""
This convert function is to convert the data into csv, easier to make header into FITS and ingest spectra into SIMPLE
"""

for file in files:
    """
    Thins need to be ingested into SIMPLE
    Telescope: du Pont, Palomar
    Instrument: BCSpec, DSpec
    """
    metadata = {}
    filename = os.path.basename(file)
    metadata['FILENAME'] = filename

    if "BCSpec_1227m0447" in file:
        metadata.update({
            'OBJECT': '2MASS J12270506-0447207',
            'RA_TARG': "186.7710085",
            'DEC_TARG': "-4.7890833",
            'DATE-OBS': '2014-02-23',
            'TELESCOP': 'Palomar',
            'INSTRUME': 'DSpec',
        })

    elif "1411m4524" in file:
        metadata.update({
            'OBJECT': '2MASS J14114474-4524153',
            'RA_TARG': "212.9361037",
            'DEC_TARG': "-45.4043026",
            'DATE-OBS': '2014-05-04',
            'TELESCOP': 'du Pont',
            'INSTRUME': 'BCSpec',
        })

    elif "1614m8151" in file:
        metadata.update({
            'OBJECT': '2MASS J16141954-8151154',
            'RA_TARG': "243.1122917",
            'DEC_TARG': "-81.8578333",
            'DATE-OBS': '2014-05-02',
            'TELESCOP': 'du Pont',
            'INSTRUME': 'BCSpec',
        })

    # Add common metadata
    metadata['AUTHOR'] = 'Kirkpatrick et al.'
    metadata['Reference'] = 'Kirk16'
    metadata['BUNIT'] = 'erg / (cm2 s Angstrom)'
    metadata['Regime'] = "optical"

    metadata_list.append(metadata)

path = "/Users/guanying/SIMPLE_Archive/SIMPLE-db/scripts/spectra_convert/BONES Archive/BONES_Archive.csv"
output_csv = os.path.join(os.path.dirname(path), 'BONES_Archive.csv')

# Write metadata to CSV file
with open(output_csv, mode='w', newline='') as csvfile:
    fieldnames = metadata_list[0].keys()
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for metadata in metadata_list:
        writer.writerow(metadata)

print(f"Metadata saved to: {output_csv}")