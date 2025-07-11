import astropy.units as u
from astropy.io.fits import getheader, Header
from astropy.io import fits
from astrodb_utils.spectra import check_spectrum_plottable
from astrodb_utils.fits import add_wavelength_keywords
from specutils import Spectrum1D
import numpy as np
from astroquery.simbad import Simbad
import os


file= ["/Users/guanying/SIMPLE_Archive/SIMPLE-db/scripts/spectra_convert/data_target_WISE1810_comb_Jun2021_YJ_STD_bb.txt",
       "/Users/guanying/SIMPLE_Archive/SIMPLE-db/scripts/spectra_convert/WISE1810m10_OB0001_R1000R_06Sept2020.txt"]

for filename in file:

    # Read the data, there are 2 columns: wavelength and flux
    data = np.loadtxt(filename, comments="#")
    if "data_target_" in filename:
        print("Reading", filename)

        # select the range limit from tested file handle_WISE1810_txt.ipynb
        wave = data[224:1380, 0] * u.AA
        flux = data[224:1380, 1] * (u.erg / u.cm**2 / u.s / u.AA)

        # create spectrum object
        spectrum = Spectrum1D(spectral_axis=wave, flux=flux)

        # convert spectrum
        header = Header()
        header.set('DATE-OBS', "2021-06-01T00:00:00")
        header.set('INSTRUME', "EMIR")


    else:
        print(f"Reading {filename}\n")

        wave = data[:1870, 0] * u.AA
        flux = data[:1870, 1] * (u.erg / u.cm**2 / u.s / u.AA)

        # create spectrum object
        spectrum = Spectrum1D(spectral_axis=wave, flux=flux)

        # convert spectrum
        header = Header()
        header.set('DATE-OBS', "2020-09-06T00:00:00")
        header.set('INSTRUME', "OSIRIS")


    # --- modify the following header to both spectrum --- ##
    header.set('OBJECT', "CWISEP J181006.00-101001.1")
    header.set('BUNIT', "erg / (cm2 s Angstrom)")
    header.set('TELESCOP', "GTC")
    header.set('VOREF', "2022A&A...663A..84L")
    header.set('TITLE', "Physical properties and trigonometric distance of the peculiar dwarf WISE J181005.5 101002.3")
    header.set("AUTHOR", "N. Lodieu, et al.")
    header.set("CONTRIB1", "Guan Ying Goh, converted to SIMPLE format")


    # get RA and DEC from Simbad
    try:
        result = Simbad.query_object("CWISEP J181006.00-101001.1")
        header["RA_TARG"] = result[0]["ra"]
        header["DEC_TARG"] = result[0]["dec"]
    except Exception as e:
        print(f"Error getting ra/deg: {e}")

    header.set('RA_TARG', 272.52575) # got from simbad
    header.set('DEC_TARG', -10.16675)
    
    spectrum.meta["header"] = header

    # add Spectrum to FITS file
    output_file = os.path.splitext(filename)[0] + ".fits"
    spectrum.write(output_file, format="tabular-fits", overwrite=True)
    print(f"Wrote to fits format")

output_dir = os.path.dirname(output_file)
new_files = os.listdir(output_dir)
new_files = [f for f in new_files if f.endswith('.fits')]

# check header and spectrum
for file in new_files:
    new_file_path = os.path.join(output_dir, file)
    spectrum = Spectrum1D.read(new_file_path, format="tabular-fits")

    header = spectrum.meta["header"]
    add_wavelength_keywords(header, spectrum.spectral_axis)
    spectrum.meta["header"] = header
    spectrum.write(new_file_path, format="tabular-fits", overwrite=True)

    if check_spectrum_plottable(spectrum, show_plot=True):
        print(f"{file} is plottable.")
