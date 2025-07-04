from simple.utils.spectra import check_spectrum_plottable
from astrodb_utils.fits import (
    add_missing_keywords,
    add_wavelength_keywords,
    check_header,
)
from astropy.io import fits
import os
from specutils import Spectrum
from astropy.wcs import WCS
from astropy.coordinates import SkyCoord
from astroquery.simbad import Simbad
from specutils.manipulation import median_smooth
import matplotlib.pyplot as plt


# VIS Xshooter
file_plotted = 0
file_failed = 0
extra_2 = 0
no_bunit = 0
no_dcflag = 0
radecsys_fix = 0

path = "/Users/kelle/Hunter College Dropbox/Kelle Cruz/SIMPLE/SIMPLE-db/scripts/spectra_convert/Zhang18/"
original_path = os.path.join(path, "sty2054_supplemental_files")
out_path = os.path.join(path, "SIMPLE")


original_files = os.listdir(original_path)
x_shooter_vis_files = [
    f
    for f in original_files
    if f.endswith(".fits") and "xshooter" in f.lower() and "VIS" in f.upper()
]

# Remove files with too many axis
x_shooter_vis_files.remove(
    "ULAS_J020858.62+020657.0_esdL3_Xshooter_VIS_Primeval-III.fits"
)
x_shooter_vis_files.remove(
    "ULAS_J135058.85+081506.8_usdL3_Xshooter_VIS_Primeval-III.fits"
)

for filename in x_shooter_vis_files:

    file_path = os.path.join(original_path, filename)
    print(f"\n Reading FITS file: {filename}")

    hdul = fits.open(file_path)
    header = hdul[0].header
    wcs = WCS(header)

    hdul[0].header.remove("RADECSYS", ignore_missing=True)

    if wcs.world_n_dim == 2 and hdul[0].header["NAXIS"] == 1:
        hdul[0].header.remove("CRPIX2", ignore_missing=True)
        hdul[0].header.remove("CDELT2", ignore_missing=True)
        hdul[0].header.remove("CUNIT2", ignore_missing=True)
        hdul[0].header.remove("CRVAL2", ignore_missing=True)
        hdul[0].header.remove("CRDER2", ignore_missing=True)
        hdul[0].header.remove("CSYER2", ignore_missing=True)
        hdul[0].header.remove("SPATRMS", ignore_missing=True)
        print("Removed unnecessary WCS keywords for 2D or 3D data.")
        extra_2 += 1

    # Fix flux units
    try:
        flux_unit = hdul[0].header["BUNIT"]
        if flux_unit == "erg/s/cm2/Angstrom":
            hdul[0].header["BUNIT"] = "erg / (cm2 s Angstrom)"
            print(
                "BUNIT keyword found and extra '/'s removed, set to 'erg / (cm2 s Angstrom)'"
            )
    except KeyError:
        hdul[0].header["BUNIT"] = "erg / (cm2 s Angstrom)"
        print("BUNIT keyword not found, set to 'erg / (cm2 s Angstrom)'")
        no_bunit += 1

    # Add IVOA SpectrumDM keywords if missing
    add_missing_keywords(hdul[0].header, format="simple-spectrum")
    print("Adding values to missing IVOA SpectrumDM keywords.")
    header.set("OBSERVAT", "VLT")
    header.set("INSTRUME", "XSHOOTER")
    header.set("TELESCOP", "ESO VLT")

    if "I.fits" in filename:
        title = "Primeval very low-mass stars and brown dwarfs - I. Six new L subdwarfs, classification and atmospheric properties"
        voref = "10.1093/mnras/stw2438"
    elif "II.fits" in filename:
        title = "Primeval very low-mass stars and brown dwarfs - II. The most metal-poor substellar object"
        voref = "10.1093/mnras/stx350"
    elif "III.fits" in filename:
        title = "Primeval very low-mass stars and brown dwarfs - III. The halo transitional brown dwarfs"
        voref = "10.1093/mnras/sty1352"
    elif "IV.fits" in filename:
        title = "Primeval very low-mass stars and brown dwarfs - IV. New L subdwarfs, Gaia astrometry, population properties, and a blue brown dwarf binary"
        voref = "10.1093/mnras/sty2054"

    header.set("TITLE", title)
    header.set("VOREF", voref)
    header.set("AUTHOR", "Zhang, Z.H. et al. ")
    header.set("CONTRIB1", "Kelle Cruz, converted to SIMPLE format")

    if header["OBJECT"] is not None:
        old_object_name = header["OBJECT"]
        print(f"OBJECT keyword found: {old_object_name}")
    strings = filename.split("_")
    object_name = " ".join(strings[:2])
    header.set("OBJECT", object_name)
    print(f"Setting OBJECT  based on filename: {object_name}")

    if header["RA_TARG"] is None or header["DEC_TARG"] is None:
        print(
            f"RA_TARG or DEC_TARG keyword not found, querying Simbad for coordinates using OBJECT: {object_name}."
        )
        try:
            result = Simbad.query_object(object_name)
            header["RA_TARG"] = result[0]["ra"]
            header["DEC_TARG"] = result[0]["dec"]
        except Exception as e:
            print(f"Could not query Simbad for coordinates: {e}")
            raise e

    # check_header(hdul[0].header)

    # Write out modified FITS file
    out_filename = filename.replace(".fits", "_SIMPLE.fits")
    outfile_path = os.path.join(path, "SIMPLE", out_filename)

    fits.writeto(
        outfile_path,
        hdul[0].data,
        header=hdul[0].header,
        overwrite=True,
        output_verify="fix",
    )

print(f"Total FITS files converted: {len(x_shooter_vis_files)}")
print(f"Total FITS files with extra WCS keywords removed: {extra_2}")
print(f"Total FITS files with no BUNIT keyword: {no_bunit}")

new_files = os.listdir(out_path)
new_files = [f for f in new_files if f.endswith(".fits")]

for new_file in new_files:
    file_path = os.path.join(out_path, new_file)
    print(f"\nReading new FITS file: {new_file}")
    spectrum = Spectrum.read(file_path)

    header = spectrum.meta["header"]
    add_wavelength_keywords(header, spectrum.spectral_axis)
    spectrum.meta["header"] = header
    spectrum.write(file_path, overwrite=True, format="tabular-fits")

    if check_spectrum_plottable(spectrum, show_plot=False, raise_error=False):
        smoothed = median_smooth(spectrum, width=101)
        ax = plt.subplots()[1]
        ax.plot(smoothed.spectral_axis, smoothed.flux)
        plt.title(header.get("OBJECT"))
        plt.show()
        file_plotted += 1
        print(f"File {new_file} is plottable.")
    else:
        file_failed += 1
        print(f"File {new_file} is not plottable.")

print(f"Total FITS files plotted: {file_plotted}")
print(f"Total FITS files failed: {file_failed}")
