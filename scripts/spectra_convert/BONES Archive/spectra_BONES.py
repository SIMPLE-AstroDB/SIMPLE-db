import pandas as pd
import numpy as np
import csv
from astropy.io.fits import Header
from astropy import units as u
from specutils import Spectrum
from astrodb_utils.spectra import check_spectrum_plottable
from astrodb_utils.fits import add_wavelength_keywords
from specutils.manipulation import extract_region,median_smooth
from specutils import Spectrum, SpectralRegion
import os

"""
This script is to convert the BONES SPECTRA to convert to Spectrum and create FITS headers for ingestion into SIMPLE-db.
"""
csv_path = "/Users/guanying/SIMPLE_Archive/SIMPLE-db/scripts/spectra_convert/BONES Archive/BONES_Archive.csv"
metadata = pd.read_csv(csv_path)
path = "/Users/guanying/SIMPLE_Archive/SIMPLE-db/scripts/spectra_convert/BONES Archive/BONES SPECTRA/"
output_dir = "/Users/guanying/SIMPLE_Archive/SIMPLE-db/scripts/spectra_convert/BONES Archive/FITS SPECTRA"
converted_files = 0
failed_files = 0


def get_spectra_region(wavelength):
    """
    Get the wavelength ranges like 1.15-1.39 or 0.85-1.348;1.39-1.83;1.92-2.5 into SpectralRegion
     making them a list
    """
    regions = []
    for part in wavelength.split(';'):
        interval = part.strip().split('-')
        start, end = float(interval[0]), float(interval[1])
        regions.append((start, end))
    return regions
    
def create_spectra():
    """
    Converts a row of metadata into a Spectrum object, creates a FITS header, and saves the spectrum to a FITS file under the output directory.
    """
    converted_files = 0
    failed_files = 0
    with open(csv_path, 'r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:

            filename = row['FILENAME']
            filepath = os.path.join(output_dir, filename.replace('.txt', '.fits'))  

            try:
                print(f"Processing {filename}")

                # Read data from raw file
                data = np.genfromtxt(
                    os.path.join(path, filename),
                    skip_header=2,
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

                # extract region from some fixed spectra
                if row['WAVERANGE'] != '':
                    region = get_spectra_region(row['WAVERANGE'])

                    spectral_regions = [
                        (start * u.Unit(row['TUNIT1']), end * u.Unit(row['TUNIT1']))
                        for start, end in region
                    ]
                    spectrum = extract_region(
                        spectrum,
                        SpectralRegion(spectral_regions),
                        return_single_spectrum=True
                    )

                # This spectrum is from WISE J155349.98+693355.2, apply median smoothing
                if("nires_NIR_J1553+6934_20200707.txt" in filename):
                    smoothed = median_smooth(spectrum, width=101)
                    spectrum = Spectrum(flux=smoothed.flux, spectral_axis=wavelength)
                
                # convert spectrum
                header = Header()
                header.set('SIMPLE', True, 'Conforms to FITS standard')
                header.set('VOPUB', 'SIMPLE Archive', 'Publication of the spectrum')
                header.set('OBJECT', row['OBJECT'], 'Name of the object')
                header.set('RA_TARG', row['RA_TARG'], '[deg] Pointing position')
                header.set('DEC_TARG', row['DEC_TARG'], '[deg] Pointing position')
                header.set('DATE-OBS', row['DATE-OBS'], 'Date of observation')
                header.set('TELESCOP', row['TELESCOP'], 'Telescope used for observation')
                header.set('INSTRUME', row['INSTRUME'], 'Instrument used for observation')
                header.set('BUNIT', row['BUNIT'], 'Flux unit of the spectrum')
                header.set('TUNIT1', row['TUNIT1'], 'Wavelength unit of the spectrum')
                header.set('REGIME', row['Regime'], 'Spectral regime of the spectrum')
                header.set('FILENAME', filename + '.fits', 'Name of the file')
                header.set('AUTHOR', row['AUTHOR'], 'Author of the spectrum')
                header.set('REFERENC', row['Reference'], 'Reference for the spectrum')
                header.set('HISTORY', 'Converted from BONES Archive text file to FITS format.', 'Conversion history')
                header.set('DATE', '2025-08-06', 'Date of FITS file creation')
                header.set('CONTRIB1', 'Guan Ying Goh', 'Contributor name')
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

create_spectra()

fits_dir = "/Users/guanying/SIMPLE_Archive/SIMPLE-db/scripts/spectra_convert/BONES Archive/FITS SPECTRA/"
new_files = os.listdir(fits_dir)
new_files = [f for f in new_files if f.endswith('.fits')]

for file in new_files:
    new_file_path = os.path.join(output_dir, file)
    spectrum = Spectrum.read(new_file_path, format="tabular-fits")

    header = spectrum.meta["header"]
    add_wavelength_keywords(header, spectrum.spectral_axis)
    spectrum.meta["header"] = header
    spectrum.write(new_file_path, format="tabular-fits", overwrite=True)

    # Check spectra plot again one by one
    if file.endswith('.fits') and ("FIRE" in file or "fire" in file ):
        print(f"Checking {file}...")
        if check_spectrum_plottable(spectrum, show_plot=True):
            print(f"    It is plottable!")
        else:
            print(f"{file} is not plottable.")
