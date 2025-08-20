import pandas as pd
import numpy as np
import csv
from astropy.io.fits import Header
from astropy import units as u
from specutils import Spectrum
from astrodb_utils.spectra import check_spectrum_plottable
from astrodb_utils.fits import add_wavelength_keywords
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

path = "/Users/guanying/SIMPLE_Archive/SIMPLE-db/scripts/spectra_convert/BONES Archive/BONES SPECTRA/"
output_dir = "/Users/guanying/SIMPLE_Archive/SIMPLE-db/scripts/spectra_convert/BONES Archive/FITS SPECTRA"

spreadsheet_url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRtZ_Sl9hSi-JdIimRxbRLSTTYozlLOStmlzzcoAM7yB-duaMtzSqAIITI2ioMqlSIc6en8eiZDnUGe/pub?output=csv"
metadata = pd.read_csv(spreadsheet_url)

converted_files = 0
failed_files = 0

def make_mask(wavelength, region_str):
    """
    Create a mask for the spectrum based on the specified region string format obtained from the BONES Archive spreadsheet
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
    converted_files = 0
    failed_files = 0
    for _, row in metadata.iterrows():

        if pd.isnull(row['SIMPLE Name']):
            continue

        filename = str(row['Filename'])
        filepath = os.path.join(output_dir, filename.replace('.txt', '.fits'))

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
                filepath = os.path.join(output_dir, filename.replace('.csv', '.fits'))
                data = np.genfromtxt(
                    os.path.join(path, filename),
                    delimiter=',',
                    skip_header=1,
                )

            # sort and remove NaN value
            data = data[~np.isnan(data).any(axis=1)]
            data = data[np.argsort(data[:, 0])]
            
            # Extract wavelength and flux
            wavelength = data[:, 0] * (u.Unit(row['TUNIT1']))
            flux = data[:, 1] * (u.erg / u.cm**2 / u.s / u.AA)
            
            # create spectrum object
            print("Creating Spectrum object...")
            spectrum = Spectrum(flux=flux, spectral_axis=wavelength)

            # # mask region to make plot look better but not affect the data
            if pd.notnull(row['Mask Region']):
                print("Applying mask region: ", row['Mask Region'])
                mask = make_mask(wavelength, row['Mask Region'])

                # create the Spectrum with mask
                spectrum = Spectrum(
                    flux=flux,
                    spectral_axis=wavelength,
                    mask=mask
                )

                # Plot the output spectrum to check the masking
                print("Plot using specutils")
                spectrum.plot()
                plt.show()

                print("Plot using matplotlib to overplot the masked region and unmasked region") 
                fig, ax = plt.subplots()
                ax.plot(spectrum.spectral_axis, spectrum.flux, label="Masked region")
                ax.plot(spectrum.spectral_axis[~spectrum.mask], spectrum.flux[~spectrum.mask], label="Unmasked region")
                ax.set_title(f"Spectrum: {row['SIMPLE Name']}")
                ax.legend()
                if(pd.notnull(row["y-min"])):
                    y_min = float(row["y-min"])
                    y_max = float(row["y-max"])
                    ax.set_ylim(y_min, y_max)

                ax.set_xlabel(row['TUNIT1'])
                ax.set_ylabel("Flux (erg / cm² / s / Angstrom)")
                plt.show()

            # smooth spectrum if needed
            if pd.notnull(row['Smooth Spectrum?']):
                print("Smoothing the spectrum..")
                smoothed = median_smooth(spectrum, width=101)
                spectrum = Spectrum(flux=smoothed.flux, spectral_axis=wavelength)
            
            # add header to the FITS
            header = Header()
            header.set('SIMPLE', True, 'Conforms to FITS standard')
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
            header.set('FILENAME', filename + '.fits', 'Name of the file')
            header.set('REFERENC', row['Reference'], 'Reference for the spectrum')
            header.set('HISTORY', 'Converted from BONES Archive text file to FITS format.', 'Conversion history')
            header.set('CONTRIB1', 'Guan Ying Goh', 'Contributor name')
            today_date = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
            header.set('DATE', today_date, 'Date of FITS file creation')
            spectrum.meta["header"] = header

            # Add wavelength keywords
            add_wavelength_keywords(header, wavelength)
            
            # Save spectrum to FITS file
            print("Writing to FITS file...")
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
    Successful conversions: 81
    Failed conversions: 0
    """

create_spectra()

fits_dir = "/Users/guanying/SIMPLE_Archive/SIMPLE-db/scripts/spectra_convert/BONES Archive/FITS SPECTRA/"
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
            # Use spectrum.plot() as check_spectrum_plottable not working with masking right now
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
Plotted spectra: 81
Failed spectra: 0
"""