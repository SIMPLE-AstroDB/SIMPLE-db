from specutils import Spectrum1D
from astropy.io import fits
import astropy.units as u
import numpy as np

spec_1935 = Spectrum1D.read(
    "/Users/kelle/Dropbox (Personal)/Mac/Downloads/jw02124-o051_s00001_nirspec_f290lp-g395h-s200a1-subs200a1_x1d_manual.fits",
    format="tabular-fits",
)
