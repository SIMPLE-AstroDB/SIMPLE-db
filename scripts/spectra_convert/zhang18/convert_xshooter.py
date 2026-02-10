import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from astropy.io import fits
from astropy.wcs import WCS
from astropy.time import Time
from datetime import date
import astropy.units as u
from specutils import Spectrum1D, Spectrum
from specutils.manipulation import median_smooth
from astrodb_utils.fits import add_missing_keywords, add_wavelength_keywords
from astrodb_utils.spectra import check_spectrum_plottable
from astroquery.simbad import Simbad
from pathlib import Path

# Path config
PATH = "/Users/guanying/SIMPLE db/SIMPLE-db/scripts/spectra_convert/zhang18/sty2054_supplemental_files"
OUTPUT_PATH = "/Users/guanying/SIMPLE db/SIMPLE-db/scripts/spectra_convert/zhang18/processed_spectra"
SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vS_VuhqnOHq9Zqu2GOcPSJks6Te161VaGJVkkN1EJYVplDqoBHgK2N1yTuD7MDPcwyI4BPUB0x2gKKo/pub?output=csv"


"""
This script is to convert the X-shooter spectra from Zhang 2018.
"""

def process_fits_by_row(row, input_path, output_path):
    """
    Clean and convert FITS
    """
    filename = row['filename']
    filepath = os.path.join(input_path, filename)
    
    # Skip IMACS, txt, OSIRIS
    if ("IMACS" in filename) or "txt" in filename or "OSIRIS" in filename:
        print(f"Skipping file: {filename}")
        return False

    # Open FITS file
    try:
        hdul = fits.open(filepath)
    except Exception as e:
        print(f"Error opening file {filename}: {e}")
        return False
    
    header = hdul[0].header
    data = hdul[0].data
    wcs = WCS(header)
    
    print(f"\nProcessing file: {filename}")
        
    # make sure data is 1D and fix 2D WCS
    if data.ndim > 1 or hdul[0].header["NAXIS"] > 1:
        print(f"  Original data shape: {data.shape}, NAXIS: {header['NAXIS']}")
        
        # Create proper 1D WCS
        wcs_1d = WCS(header, naxis=1)
        
        # get wavelength array from WCS
        last_idx = data.shape[-1] 
        wavelength = wcs_1d.pixel_to_world(np.arange(last_idx))
        print(f"  Generated wavelength array of length {len(wavelength)}")
        
        # Extract 1D data
        if data.ndim == 3:
            hdul[0].data = data[0, 0, :]  #get (N,)
        else:
            hdul[0].data = data
        
        # Remove 2D/3D header keywords
        hdul[0].header.remove("CRPIX2", ignore_missing=True)
        hdul[0].header.remove("CDELT2", ignore_missing=True)
        hdul[0].header.remove("CUNIT2", ignore_missing=True)
        hdul[0].header.remove("CRVAL2", ignore_missing=True)
        hdul[0].header.remove("CRDER2", ignore_missing=True)
        hdul[0].header.remove("CSYER2", ignore_missing=True)
        hdul[0].header.remove("SPATRMS", ignore_missing=True)
        print("  Removed unnecessary WCS keywords for 2D/3D data")
   
        add_wavelength_keywords(hdul[0].header, wavelength)
        
    else:
        print(f"  Data already 1D: {hdul[0].data.shape}")


    header.set('CONTRIB1', 'Guan Ying Goh', 'Contributor name')
    header.set("DATE", date.today().isoformat(), 'Date of FITS file creation')
    
    # Format flux units
    flux_unit = header.get("BUNIT")
    if flux_unit == "erg/s/cm2/Angstrom":
        hdul[0].header["BUNIT"] = "erg / (cm2 s Angstrom)"
        print(f"  Updated BUNIT")
    elif not flux_unit:
        hdul[0].header["BUNIT"] = "erg / (cm2 s Angstrom)"

    
    # Save updated FITS
    out_filename = filename.replace(".fits", "_SIMPLE.fits")
    out_file = os.path.join(output_path, out_filename)
    
    try:
        fits.writeto(
            out_file,
            hdul[0].data,
            header=hdul[0].header,
            overwrite=True,
            output_verify="fix",
        )
        print(f"  Saved to {out_filename}\n")
        hdul.close()
        return True
    except Exception as e:
        print(f"  Error saving file: {e}\n")
        hdul.close()
        return False


def create_spec_file(row, output_path):
    """
    Create spectrum object from processed FITS file
    """
    filename = row['filename']
    spec_file = Path(output_path) / filename.replace(".fits", "_SIMPLE.fits")
    
    if not spec_file.exists():
        print(f"Processed file not found: {spec_file.name}")
        return False
    
    if ("IMACS" in spec_file.name) or "txt" in spec_file.name or "OSIRIS" in spec_file.name:
        print(f"Skipping file: {spec_file.name}")
        return False
    
    print(f"\nCreating spectrum from: {spec_file.name}")

    try:
        spectrum = Spectrum.read(spec_file, format="wcs1d-fits")
    except Exception as e:
        print(f"  Error reading spectrum with wcs1d-fits: {e}")
        # get wave + flux manually and create spectrum object
        try:
            print("  Attempting manual spectrum creation...")
            with fits.open(spec_file) as hdul:
                data = hdul[0].data
                header = hdul[0].header
                
                # get wavelength from WCS
                wcs = WCS(header, naxis=1)
                wave = len(data)
                wavelength = wcs.pixel_to_world(np.arange(wave))
                
                # get flux
                flux_unit = header.get("BUNIT", "erg / (cm2 s Angstrom)")
                flux = data * u.Unit(flux_unit)
                
                # Create spectrum
                spectrum = Spectrum1D(
                    spectral_axis=wavelength, 
                    flux=flux, 
                    meta={"header": header}
                )
                print("  Successfully created spectrum manually")
        except Exception as e2:
            print(f"  Error creating spectrum manually: {e2}")
            return False

    header = spectrum.meta["header"]
            
    # Smooth spectrum and create new spectrum object for smoothed spec
    smoothing_value = row.get("Smoothing")
    smoothed_spectrum = None
    
    if not pd.isna(smoothing_value) and smoothing_value > 0:
        print(f"  Smoothing with width={int(smoothing_value)}")
        smoothed_spectrum = median_smooth(spectrum, width=int(smoothing_value))
        
        smooth_header = smoothed_spectrum.meta["header"].copy()
        # keep all original headers
        for key in header.keys():
            if key not in smooth_header:
                smooth_header[key] = header[key]
        # add new header
        smooth_header.set("SMOOTH", int(smoothing_value), "Width of median smoothing applied")
        smooth_header.set("COMMENT", "Spectrum smoothed using specutils.median_smooth with width 101")
        smoothed_spectrum.meta["header"] = smooth_header
    
    #update spectrum header
    header["SMOOTH"] = "None" if smoothed_spectrum is None else 0
    header["HISTORY"] = "FITS file created with specutils.Spectrum.write"
    spectrum.meta["header"] = header
    
    # Write original spectrum
    try:
        spectrum.write(
            os.path.join(output_path, spec_file.name),
            overwrite=True,
            format="tabular-fits",
        )
        print(f"  Saved spectrum: {spec_file.name}")
    except Exception as e:
        print(f"  Error writing spectrum: {e}")
        return False
    
    # Write separate new spectrum for smoothed object
    if smoothed_spectrum is not None:
        smoothed_filename = spec_file.name.replace("_SIMPLE.fits", "_SMOOTHED_SIMPLE.fits")
        try:
            smoothed_spectrum.write(
                os.path.join(output_path, smoothed_filename),
                overwrite=True,
                format="tabular-fits",
            )
            print(f"  Saved smoothed spectrum: {smoothed_filename}")
        except Exception as e:
            print(f"  Error writing smoothed spectrum: {e}")
    
    # check if spectrum is plottable
    try:
        spec = Spectrum.read(os.path.join(output_path, spec_file.name))
        if check_spectrum_plottable(spec, show_plot=True):
            print(f"   -- Spectrum is plottable")
        
        # Create new file for smoothed spectrum and check if plottable
        if smoothed_spectrum is not None:
            smoothed_filename = spec_file.name.replace("_SIMPLE.fits", "_SMOOTHED_SIMPLE.fits")
            spec_smoothed = Spectrum.read(os.path.join(output_path, smoothed_filename))
            if check_spectrum_plottable(spec_smoothed, show_plot=True):
                print(f"   -- Smoothed spectrum is plottable")
        
        return True
    except Exception as e:
        print(f"  Error verifying spectrum: {e}")
        return False


def process_all_files(spreadsheet_url, input_path, output_path):
    """
    Process all FITS files from spreadsheet
    """
    os.makedirs(output_path, exist_ok=True)
    
    # Read spreadsheet
    print(f"Reading spreadsheet from: {spreadsheet_url}\n")
    df = pd.read_csv(spreadsheet_url)
    
    # Outcome summary 
    outcome = {
        'total': len(df),
        'processed': 0,
        'failed': 0,
        'spectra_created': 0,
        'spectra_failed': 0,
        'smoothed': 0,
    }
    
    failed_files = []

    # Read first 27 Xshooter file (exclude IMACS, OSIRIS, or txt files)
    for idx, row in df.iloc[:27].iterrows():

        filename = row['filename']
        print(f"\n{'='*60}")
        print(f"Processing row {idx+1}/{len(df)}: {filename}")
        print('='*60)
        
        # clean and convert FITS file
        success = process_fits_by_row(row, input_path, output_path)
        
        if success:
            outcome['processed'] += 1
            
            # Step 2: Create spectrum object
            spec_success = create_spec_file(row, output_path)
            
            if spec_success:
                outcome['spectra_created'] += 1
                # Check if smoothed version was created
                if not pd.isna(row.get("Smoothing")) and row.get("Smoothing") > 0:
                    outcome['smoothed'] += 1
            else:
                outcome['spectra_failed'] += 1
                failed_files.append(filename)
        else:
            outcome['failed'] += 1
            failed_files.append(filename)
    
    # Print summary
    print("\n=========== PROCESSING SUMMARY ===========")
    print(f"Total files in spreadsheet: {outcome['total']}")
    print(f"Xshooter processed: {outcome['processed']}")
    print(f"Spectra created: {outcome['spectra_created']}")
    print(f"Smoothed spectra: {outcome['smoothed']}")
    print(f"Total failed: {outcome['failed']}")
    print(f"Spectrum create failed: {outcome['spectra_failed']}")
    
    if failed_files:
        print(f"\nFailed files ({len(failed_files)}):")
        for f in failed_files:
            print(f" - {f}")

process_all_files(SPREADSHEET_URL, PATH, OUTPUT_PATH)