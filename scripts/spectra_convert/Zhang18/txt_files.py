from astrodb_utils import load_astrodb, AstroDBError
from simple.utils.spectra import check_spectrum_plottable
from astrodb_utils.fits import add_missing_keywords, add_wavelength_keywords, check_header
from astropy.io import fits
import astropy.units as u
import os
import numpy as np
from specutils import Spectrum, SpectralRegion
from specutils.manipulation import extract_region, snr_threshold
from astropy.wcs import WCS

path = "/Users/kelle/Hunter College Dropbox/Kelle Cruz/SIMPLE/SIMPLE-db/scripts/ingests/Zhang18/sty2054_supplemental_files"
print(os.path.exists(os.path.join(path,"SDSS_J134749.74+333601.7_sdL0_SDSS_Primeval-I.txt")))


# Handle txt files first separately
file_plotted = 0
file_failed = 0
for filename in os.listdir(path):
    if filename.endswith(".fits") or filename.startswith("README"):
        # print(f"SKIPPING FITS file: {filename}")
        continue
    
    print(f"Reading text file: {filename}")

    file_path = os.path.join(path, filename)
    
    try:
        data = np.loadtxt(file_path, comments="#", encoding="latin1")

        # column1: #w         column2:flux          
        if (filename == "SDSS_J134749.74+333601.7_sdL0_SDSS_Primeval-I.txt"):
            wavelength = (data[:, 0] * u.AA).to(u.um)
            flux = data[:, 1] * (u.erg / (u.cm**2 * u.s * u.micron))

        # column1: #w (micron)         column2:flux          
        else:
            wavelength = (data[:, 0] * u.um)
            flux = data[:, 1] * (u.watt / u.micron/ (u.meter**2)) # Flux(lambda) in W/um/m2
        # check plottability
        spectrum = Spectrum(spectral_axis=wavelength, flux=flux)
        spectrum = extract_region(spectrum, SpectralRegion(0.4*u.um, 2.4*u.um))
        check_spectrum_plottable(spectrum, show_plot=True)
        file_plotted += 1
    
    except Exception as e:
        print(f"Could not read {filename}: {e}")
        file_failed += 1
print(f"Total files plotted: {file_plotted}")