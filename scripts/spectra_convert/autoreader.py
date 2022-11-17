from specutils import Spectrum1D
import urllib
from astropy.io import ascii

table= 'wcs1d_spectrum_table.dat'
data = ascii.read(table)

for row in data:
    specs = urllib.request.urlopen(row['wcs1d-fits Spectra'])
    Spectrum1D.read(specs, format='wcs1d-fits')

#does not work without format = 'wcs1d-fits' :(
