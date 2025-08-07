import os
import csv

path = "/Users/guanying/SIMPLE_Archive/SIMPLE-db/scripts/spectra_convert/BONES Archive/BONES SPECTRA/"

# Process source with DSpec spectra
file_converted = 0
metadata_list = []

"""
This convert function is to convert the data into csv, easier to make header into FITS and ingest spectra into SIMPLE
"""

for filename in os.listdir(path):
    """
    Things need to be ingested into SIMPLE
    Telescope: Palomar
    Instrument: DSpec
    source: ULAS J124425.75+102439.3, LSPM J1425+7102
    """
    if filename.startswith("DSpec_") and filename.endswith(".txt"):
        metadata = {}
        filename = os.path.basename(filename)
        metadata['FILENAME'] = filename

        if "DSpec_0043p2212.txt" in filename:
            metadata.update({
                'OBJECT': "WISEA J004326.26+222124.0",
                'RA_TARG': "10.8588758",
                'DEC_TARG': "22.3578851",
                'DATE-OBS': '2014-10-24'
            })

        elif "DSpec_0130m1047.txt" in filename:
            metadata.update({
                'OBJECT': "WISEA J013012.66-104732.4",
                'RA_TARG': "22.5523774",
                'DEC_TARG': "-10.7913536",
                'DATE-OBS': '2015-12-10'
            })

        elif "DSpec_0336p0010.txt" in filename:
            metadata.update({
                'OBJECT': "2MASS J03361338+0010129",
                'RA_TARG': "54.055665902",
                'DEC_TARG': "0.1702744436",
                'DATE-OBS': '2014-10-24',
            })
        
        elif "DSpec_0559-2903.txt" in filename:
            metadata.update({
                'OBJECT': "APMPM J0559-2903",
                'RA_TARG': "89.7455128",
                'DEC_TARG': "-29.0574191",
                'DATE-OBS': '2014-02-24'
            })
        
        elif "DSpec_0723+0316.txt" in filename:
            metadata.update({
                'OBJECT': "LSPM J0723+0316",
                'RA_TARG': "110.9293666",
                'DEC_TARG': "3.2727198",
                'DATE-OBS': '2014-02-23'
            })

        elif "DSpec_0822+1700.txt" in filename:
            metadata.update({
                'OBJECT': "LSR J0822+1700",
                'RA_TARG': "125.6406303",
                'DEC_TARG': "17.0052578",
                'DATE-OBS': '2014-02-23'
            })
        
        elif "DSpec_1158p0447.txt" in filename:
            metadata.update({
                'OBJECT': "ULAS J115826.62+044746.8",
                'RA_TARG': "179.6109366",
                'DEC_TARG': "4.7963890",
                'DATE-OBS': '2014-02-23'
            })
        
        elif "DSpec_1244p1024.txt" in filename:
            metadata.update({
                'OBJECT': "ULAS J124425.75+102439.3",
                'RA_TARG': "191.1082044",
                'DEC_TARG': "10.4119942",
                'DATE-OBS': '2014-02-23'
            })

        elif "DSpec_1256-1408.txt" in filename:
            metadata.update({
                'OBJECT': "SSSPM J1256-1408",
                'RA_TARG': "194.0583877",
                'DEC_TARG': "-14.1445604",
                'DATE-OBS': '2014-02-23'
            })

        elif "DSpec_1416+1348.txt" in filename:
            metadata.update({
                'OBJECT': "SDSS J141624.08+134826.7",
                'RA_TARG': "214.1003082",
                'DEC_TARG': "13.8072759",
                'DATE-OBS': '2014-02-23'
            })

        elif "DSpec_1425p7102.txt" in filename:
            metadata.update({
                'OBJECT': "LSPM J1425+7102",
                'RA_TARG': "216.2709223",
                'DEC_TARG': "71.0360241",
                'DATE-OBS': '2014-02-24'
            })
        
        elif "DSpec_1434p2202.txt" in filename:
            metadata.update({
                'OBJECT': "2MASS J14343616+2202463",
                'RA_TARG': "218.6507107",
                'DEC_TARG': "22.0462443",
                'DATE-OBS': '2014-02-24'
            })


        elif "DSpec_1452p2723.txt" in filename:
            metadata.update({
                'OBJECT': "SDSS J145255.58+272324.4",
                'RA_TARG': "223.2318279",
                'DEC_TARG': "27.3904304",
                'DATE-OBS': '2015-06-08'
            })

        elif "DSpec_1941m0208.txt" in filename:
            metadata.update({
                'OBJECT': "PM J19418-0208",
                'RA_TARG': "295.3981950",
                'DEC_TARG': "-2.1460833",
                'DATE-OBS': '2014-06-26'
            })

        elif "DSpec_2134p7132.txt" in filename:
            metadata.update({
                'OBJECT': "2MASS J21340795+7132312",
                'RA_TARG': "323.5336202",
                'DEC_TARG': "71.5421521",
                'DATE-OBS': '2014-10-24'
            })

        metadata['TELESCOP']= 'Palomar'
        metadata['INSTRUME']= 'DSpec'
        metadata['AUTHOR']='Kirkpatrick et al.'
        metadata['Reference'] = 'Kirk16'
        metadata['BUNIT']= 'erg / (cm2 s Angstrom)'
        metadata['Regime'] = "optical"
        metadata['WAVERANGE'] = "4000-15000"
        metadata['TUNIT1'] = 'Angstrom'

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

