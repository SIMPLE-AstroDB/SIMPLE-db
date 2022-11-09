from specutils import Spectrum1D
import urllib
from astropy.io import ascii

table= 'wcs1d_spectrum_table.dat'
data = ascii.read(table)
spectrum = data['wcs1d-fits Spectra']

for row in data:
    specs = urllib.request.urlopen(row['wcs1d-fits Spectra'])
    Spectrum1D.read(specs)
#does not work without format = 'wcs1d-fits' :(
