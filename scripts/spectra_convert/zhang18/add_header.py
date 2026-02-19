import os
import numpy as np
import pandas as pd
from astropy.io import fits
from astropy.time import Time
from datetime import datetime
import astropy.units as u
from specutils import Spectrum
from astrodb_utils.fits import add_missing_keywords, add_wavelength_keywords, check_header
from astrodb_utils.spectra import check_spectrum_plottable
from astroquery.simbad import Simbad
from pathlib import Path

# Path config
path = "/Users/guanying/SIMPLE db/SIMPLE-db/scripts/spectra_convert/zhang18/processed_spectra"
spreadsheet = "https://docs.google.com/spreadsheets/d/e/2PACX-1vS_VuhqnOHq9Zqu2GOcPSJks6Te161VaGJVkkN1EJYVplDqoBHgK2N1yTuD7MDPcwyI4BPUB0x2gKKo/pub?gid=220627455&single=true&output=csv"


def get_paper_metadata(filename):
    if "-I_SIMPLE.fits" in filename or "-I_SMOOTHED_SIMPLE.fits" in filename:
        title = "Primeval very low-mass stars and brown dwarfs - I. Six new L subdwarfs, classification and atmospheric properties"
        voref = "10.1093/mnras/stw2438"
    elif "-II_SIMPLE.fits" in filename or "-II_SMOOTHED_SIMPLE.fits" in filename:
        title = "Primeval very low-mass stars and brown dwarfs - II. The most metal-poor substellar object"
        voref = "10.1093/mnras/stx350"
    elif "-III_SIMPLE.fits" in filename or "-III_SMOOTHED_SIMPLE.fits" in filename:
        title = "Primeval very low-mass stars and brown dwarfs - III. The halo transitional brown dwarfs"
        voref = "10.1093/mnras/sty1352"
    elif "-IV_SIMPLE.fits" in filename or "IV_SMOOTHED_SIMPLE.fits" in filename:
        title = "Primeval very low-mass stars and brown dwarfs - IV. New L subdwarfs, Gaia astrometry, population properties, and a blue brown dwarf binary"
        voref = "10.1093/mnras/sty2054"
    else:
        title = "Unknown Paper"
        voref = "Unknown DOI"

    return title, voref

def get_regime(filename):
    df = pd.read_csv(spreadsheet)

    # Clean filename to match spreadsheet's filename
    base_filename = filename.replace("_SMOOTHED_SIMPLE.fits", ".fits").replace("_SIMPLE.fits", ".fits")
    row = df[df['filename'] == base_filename]

    # Get regime for SPECBAND
    if not row.empty:
        regime = row.iloc[0]['regime']
    else:
        regime = None
        print(f"  WARNING: Filename {base_filename} not found in spreadsheet.")

    return regime
    

def add_header(path):
    missing_telescop_instrument = []
    missing_dateobs = []
    file_proceed = 0


    fits_files = list(Path(path).glob("*.fits"))

    for fits_file in fits_files:
        filename = fits_file.name
        print(f"\nProcessing {filename}...")
        spectrum = Spectrum.read(fits_file)

        title, voref = get_paper_metadata(filename)

        with fits.open(fits_file, mode="update") as hdul:
            header = hdul[0].header

            # add SIMPLE FITS headers
            header["VOPUB"] = ("SIMPLE Archive", "Publication of the spectrum")

            # object name
            object_name = " ".join(filename.split("_")[:2])
            header["OBJECT"] = (object_name, "Name of the object")

            # Query simbad for ra/dec
            try:
                result = Simbad.query_object(object_name)
                if result is not None and len(result) > 0:
                    header["RA_TARG"] = (result[0]["ra"], "[ra] Pointing position")
                    header["DEC_TARG"] = (result[0]["dec"], "[dec] Pointing position")
            except Exception as e:
                print(f"  SIMBAD lookup failed: {e}")

            # Remove deprecated keyword
            if "RADECSYS" in header:
                header.remove("RADECSYS", ignore_missing=True)
                print("  Removed RADECSYS")

            # Check TELESCOP + INSTRUME
            if "TELESCOP" not in header or "INSTRUME" not in header:
                missing_telescop_instrument.append(filename)
            header["OBSERVAT"] = (header.get("TELESCOP"), "Observatory")

            # Get SPECBAND (regime)
            regime = get_regime(filename)
            header["SPECBAND"] = (regime)

            # Get APERTURE (slit width)
            if (header.get("SLITW")) is not None:
                header["APERTURE"] = (header.get("SLITW"))
            else:
                if "Xshooter" in filename :
                    header["APERTURE"] = ("1.2", "Slit width in arcsec")

            add_wavelength_keywords(header, spectrum.spectral_axis)

            # Paper metadata
            header["TITLE"] = (title, "Title of the paper")
            header["VOREF"] = (voref, "DOI of the paper")
            header["AUTHOR"] = ("Zhang, Z.H. et al.", "Original authors")
            header["DATE"] = (datetime.now().strftime("%Y-%m-%dT%H:%M:%S"))
            header["CONTRIB1"] = ("Guan Ying Goh")
            header["TELAPSE"] = (header.get("EXPTIME"))
            mjd_obs = header.get("MJD-OBS")
            exptime = header.get("EXPTIME")
            if mjd_obs is None or exptime is None:
                print(f"  WARNING: MJD-OBS or EXPTIME missing in header for {filename}. Cannot calculate TMID.")
            if mjd_obs is not None and exptime is not None:
                tmid = mjd_obs + (exptime / 2) / (60 * 60 * 24)
                header.set("TMID", tmid, "[d] MJD of exposure mid-point")
            print("   Added paper metadata")

            # Fix flux units
            if "BUNIT" not in header:
                header["BUNIT"] = "erg / (cm2 s Angstrom)"
            elif header["BUNIT"] == "erg/s/cm2/Angstrom":
                header["BUNIT"] = "erg / (cm2 s Angstrom)"

            # date-obs check
            if "DATE-OBS" not in header or header["DATE-OBS"] in ["", "UNKNOWN", None]:
                missing_dateobs.append(filename)

            add_missing_keywords(header)
            check_header(header)

            hdul.verify('exception')
            hdul.verify('warn')
            hdul.flush()
            print(f"  Finished {filename}")
            file_proceed += 1

    # Summary
    print(f"\File processed: {file_proceed}/{len(fits_files)}")
    if missing_telescop_instrument:
        print("\nFiles missing TELESCOP or INSTRUME:")
        for f in missing_telescop_instrument:
            print(f"  - {f}")

    if missing_dateobs:
        print("\nFiles missing DATE-OBS:")
        for f in missing_dateobs:
            print(f"  - {f}")

"""
\File processed: 120/120
    64 OSIRIS
    52 Xshooter with smoothed and unsmoothed (ignored UVB)
    2 FIRE_Magellan
    1 SDSS
    1 IMACS
"""
add_header(path)