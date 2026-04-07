import pandas as pd
import numpy as np
import csv
from astropy.io.fits import Header
from astropy import units as u
from specutils import Spectrum
from astrodb_utils.spectra import check_spectrum_plottable
from astrodb_utils.fits import add_wavelength_keywords, add_missing_keywords, check_header
from specutils.manipulation import extract_region,median_smooth
from astroquery.simbad import Simbad
import os
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt
from specutils import Spectrum

"""
This script converts BONES Archive text files to FITS format using the data obtained from the CSV spreadsheet
"""

path = "/Users/guanying/SIMPLE db/SIMPLE-db/scripts/spectra_convert/BONES Archive/BONES SPECTRA/"
output_dir = "/Users/guanying/SIMPLE db/SIMPLE-db/scripts/spectra_convert/BONES Archive/Processed BONES"

spreadsheet_url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRtZ_Sl9hSi-JdIimRxbRLSTTYozlLOStmlzzcoAM7yB-duaMtzSqAIITI2ioMqlSIc6en8eiZDnUGe/pub?output=csv"
metadata = pd.read_csv(spreadsheet_url)

converted_files = 0
failed_files = 0

def make_mask(wavelength, region_str):
    """
    create a mask for the spectrum based on the specified region string format obtained from spreadsheet
    e.g:    4100;10000 (two regions) : mask region below 4100 and above 10000
            0.85;1.348-1.39;1.83-1.92;2.5 (multiple regions): mask outside the given sub-ranges
    """
    # Split the input string by ; and remove empty parts
    parts = region_str.split(';')
    parts = [p.strip() for p in parts if p.strip()]

    mask = None 

    for i, part in enumerate(parts):
        # If the part is a range like 1.5-5.7
        if '-' in part:
            start, end = map(float, part.split('-'))
            new_mask = np.logical_and(wavelength.value > start, wavelength.value < end)
        
        # if part is single value like 4100;10000
        else:
            val = float(part)

            if i == 0: # lowwer limit at edge
                new_mask = wavelength.value < val

            elif i == len(parts) - 1: # upper limit at edge
                new_mask = wavelength.value > val

            else: # middle value, mask at this value
                new_mask = wavelength.value == val

        ## combine previous mask using or statement
        if mask is None:
            mask = new_mask
        else:
            mask = np.logical_or(mask, new_mask)

    return mask

def create_spectra():
    """
    1. Converts a row of metadata into a Spectrum object,
    2. Apply mask or smoothing if needed 
    3. create a FITS header
    4. saves the spectrum to a FITS file to output dir
    """
    SKIP_FILES = [
        "BCSpec_1227m0447.txt",
        "fireprism_NIR_ULASJ124947.04+095019.8.txt",
        "fireprism_NIR_ULASJ133836.97-022910.7.txt",
        "Xshooter_NIR_2MASSJ06453153-6646120.txt",
        "Xshooter_NIR_SDSSJ010448.46+153501.8.txt",
        "Xshooter_NIR_ULASJ020858.62+020657.0.txt",
        "Xshooter_NIR_ULASJ021642.96+004005.7.txt",
        "Xshooter_NIR_ULASJ024035.36+060629.3.txt",
        "Xshooter_NIR_ULASJ130710.22+151103.4.txt",
        "Xshooter_NIR_ULASJ141203.85+121609.9.txt",
        "Xshooter_NIR_ULASJ151913.03-000030.0.txt",
        "Xshooter_NIR_ULASJ223302.03+062030.8.txt",
        "Xshooter_NIR_ULASJ230711.01+014447.1.txt",
        "spex-prism_NIR_J1439+1839_Burgasser2004.txt",
        "spex-prism_NIR_J1626+3925_Burgasser2004.txt",
        "Xshooter_NIR_ULASJ135058.85+081506.8.txt"
    ]
    converted_files = 0
    failed_files = 0
    for _, row in metadata.iterrows():

        if row['Filename'] in SKIP_FILES:
            print(f"Skipping {row['Filename']} due to known issues.")
            continue

        if pd.isnull(row['SIMPLE Name']) or pd.isnull(row['Filename']):
            continue

        filename = str(row['Filename'])
        filepath = os.path.join(output_dir, filename.replace('.txt', '_SIMPLE.fits'))

        try:
            print(f"Processing {filename}...")

            # Read data from raw file
            data = np.genfromtxt(
                os.path.join(path, filename),
                skip_header=1,
                comments='#',
                encoding="latin1"
            )

            # CASE: CSV format
            if(filename.endswith('.csv')):
                filepath = os.path.join(output_dir, filename.replace('.csv', '_SIMPLE.fits'))
                data = np.genfromtxt(
                    os.path.join(path, filename),
                    delimiter=',',
                    skip_header=1,
                )
            if("Xshooter_NIR_2MASSJ06164006-6407194.txt" in filename):
                # change Xshooter to OSIRIS
                filepath = os.path.join(output_dir, filename.replace('Xshooter', 'OSIRIS').replace('.txt', '_SIMPLE.fits'))

            # sort and remove NaN value
            data = data[~np.isnan(data).any(axis=1)]
            data = data[np.argsort(data[:, 0])]
            
            # Extract wavelength and flux
            wavelength = data[:, 0] * (u.Unit(row['TUNIT1']))
            flux = data[:, 1] * (u.erg / u.cm**2 / u.s / u.AA)
            
            # create spectrum object
            print("Creating Spectrum object...")
            spectrum = Spectrum(flux=flux, spectral_axis=wavelength)

            # mask region to cut off non-informatic region but not affect the data
            if pd.notnull(row['Mask Region']):
                print("Applying mask region: ", row['Mask Region'])
                mask = make_mask(wavelength, row['Mask Region'])

                # create the Spectrum with mask
                spectrum = Spectrum(
                    flux=flux,
                    spectral_axis=wavelength,
                    mask=mask
                )
            
            # add header to the FITS
            header = Header()
            header.set('VOPUB', 'SIMPLE Archive', 'Publication of the spectrum')
            header.set('OBJECT', str(row['SIMPLE Name']), 'Name of the object')
            result = Simbad.query_object(str(row['SIMPLE Name']))
            if result is not None and len(result) > 0:
                header.set("RA_TARG", result[0]["ra"], '[ra] Pointing position')
                header.set("DEC_TARG", result[0]["dec"], '[dec] Pointing position')
            header.set('DATE-OBS', row['DATE-OBS'], 'Date of observation')
            header.set('TELESCOP', row['TELESCOP'], 'Telescope used for observation')
            header.set('INSTRUME', row['INSTRUME'], 'Instrument used for observation')
            header.set('REGIME', row['Regime'], 'Spectral regime of the spectrum')
            header.set('REFERENC', row['Reference'], 'Reference for the spectrum')
            header.set('AUTHOR', row['OPTICAL_CITATION'] if pd.notnull(row['OPTICAL_CITATION']) else row['NIR_CITATION'])
            header.set('TITLE', row['TITLE'], 'Title of the discovery reference')
            header.set('VOREF', row['VOREF'], 'DOI of the discovery reference')
            header.set('HISTORY', f'Original file from BONES Archive: {str(row["Filename"])}')
            header.set('HISTORY', f'Converted to FITS format: {os.path.basename(filepath)}')
            header.set('CONTRIB1', 'Guan Ying Goh', 'Contributor name')
            today_date = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
            header.set('DATE', today_date, 'Date of FITS file creation')
            header.set('COMMENT', f'This FITS file was converted from the original spectrum {filename}')
            
            # Add mask info to the FITS header
            if pd.notnull(row['Mask Region']):
                header.set('MASKED', True, "Spectrum has masked regions")
                header.set("MASK_REG",row['Mask Region'], "Region used for masking the spectrum")
                header.set("COMMENT", "Masked region applied, use Spectrum.plot() to visualize the masked spectrum")

            # smooth spectrum if needed
            if row['Smooth Spectrum?'] == 'Yes':
                print("Smoothing the spectrum..")
                spectrum = median_smooth(spectrum, width=101)
                # spectrum = Spectrum(flux=spectrum.flux, spectral_axis=wavelength)
                header.set("SMOOTH", 101, "Width of median smoothing applied")
                header.set("COMMENT", "Spectrum smoothed using specutils.median_smooth with width 101")
                spectrum.meta["header"] = header

            # Header double-check
            print("Checking header keywords...")
            add_wavelength_keywords(header, wavelength)
            add_missing_keywords(header)
            check_header(header)
            
            # Save spectrum to FITS file
            print("Writing to FITS file...")
            spectrum.meta["header"] = header
            spectrum.write(filepath, format='tabular-fits', overwrite=True)
            print(f"Converted {filename} to {filepath}!\n")
            converted_files += 1

        except Exception as e:
            print(f"Error processing {filename}: {e}")
            failed_files += 1
            continue

    print(f"Successful conversions: {converted_files}")
    print(f"Failed conversions: {failed_files}")

    """
    Successful conversions: 66
    Failed conversions: 0
    """

create_spectra()

fits_dir = "/Users/guanying/SIMPLE db/SIMPLE-db/scripts/spectra_convert/BONES Archive/Processed BONES/"
new_files = os.listdir(fits_dir)
new_files = [f for f in new_files if f.endswith('.fits')]
plotted_spectra = 0
failed_spectra = 0

# Check if the final FITS spectra files are plottable and add keywords again
for file in new_files:
    new_file_path = os.path.join(output_dir, file)
    spectrum = Spectrum.read(new_file_path, format="tabular-fits")

    header = spectrum.meta["header"]
    add_wavelength_keywords(header, spectrum.spectral_axis)
    spectrum.meta["header"] = header
    spectrum.write(new_file_path, format="tabular-fits", overwrite=True)

    # Check spectra plot again one by one
    if file.endswith('.fits'):
        print(f"Checking {file}...")

        try:
            spectrum.plot()
            plt.show()
            print("    It is plottable!")
            plotted_spectra += 1
        except Exception as e:
            print(f"   Error plotting {file}: {e}")
            failed_spectra += 1
            continue     
print(f"Plotted spectra: {plotted_spectra}")
print(f"Failed spectra: {failed_spectra}")  

""" 
Plotted spectra: 66
Failed spectra: 0
"""