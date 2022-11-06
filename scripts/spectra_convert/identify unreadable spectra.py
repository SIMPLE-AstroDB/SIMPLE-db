
#save file w 1081 wcs1d files to explore auto reader
# save the list as an astropy table for the spectra -> can be txt
# push
# commit and push !!!!!!!!!!!!!!!!!!
#open pull request once its in spectra convert folder

from scripts.ingests.utils import *
import csv
from scripts.ingests.utils import *
from specutils import Spectrum1D
import astropy.units as u

logger.setLevel(logging.DEBUG)

db = load_simpledb('SIMPLE.db', recreatedb=False)
#data = db.inventory('2MASS J13571237+1428398',pretty_print= True)

table = db.query(db.Spectra.c.spectrum).table()
#print(table.info)

wcs1d_fits_tally = 0
spex_prism_tally = 0
iraf_tally = 0
tabularfits_tally = 0
fits_units = 0
spex_units = 0
ascii_tally = 0
not_working_tally =0


not_working_errors = [] #prints out the errors for the ones that don't work that dont work
not_working_spectra = []
not_working_txt = []
wcs1d_fits_spectra = []


#ASCII, iraf, tabular-fits, wcs1d-fits, Spex Prism, ecsv

for n ,spectrum in enumerate(table):
    file = os.path.basename(spectrum[0])
    file_root = os.path.splitext(file)[1]
    #print(n,spectrum[0])
    try:
        spec = Spectrum1D.read(spectrum[0], format = 'wcs1d-fits')
        wcs1d_fits_tally +=1
        wcs1d_fits_spectra.append(spectrum[0])
        try:
            spec.wavelength.to(u.micron).value
        except:
            fits_units += 1
    except Exception as e_wcs1d:
        not_working_errors.append(f'wcs1d err: {e_wcs1d} \n') # this does not work -
        # add to every error? does this just add #?
        not_working_spectra.append(spectrum[0])
        try:
            spec = Spectrum1D.read(spectrum[0], format = 'Spex Prism')
            spex_prism_tally += 1

            try:
                spec.wavelength.to(u.micron).value
            except:
                spex_units += 1
        except Exception as e_spex:
            not_working_errors.append(f'spex prism err: {e_spex} \n') # add to every error
            not_working_spectra.append(spectrum[0])
            try:
                spec = Spectrum1D.read(spectrum[0], format = 'iraf')
                iraf_tally += 1
            except Exception as e_iraf:
                try:
                    spec = Spectrum1D.read(spectrum[0], format = 'tabular-fits')
                    tabularfits_tally += 1
                except Exception as e_tabular:
                    try:
                        spec = Spectrum1D.read(spectrum[0], format = 'ASCII')
                        ascii_tally += 1
                    except Exception as e_ascii:
                        not_working_errors.append(f'ascii err: {e_ascii} \n') # add to every error
                        not_working_spectra.append(spectrum[0])
                        not_working_tally += 1
                        if file_root == '.txt':
                            not_working_txt.append(spectrum[0])





print(f'wcs1d fits tally: {wcs1d_fits_tally}')
print(f'fits units broken: {fits_units}')
print(f'spex prism tally: {spex_prism_tally}')
print(f'spex units broken: {spex_units}')
print(f'iraf tally: {iraf_tally}')
print(f'tabularfits tally: {tabularfits_tally}')
print(f'ascii tally: {ascii_tally}')
print(f'not_working_tally: {not_working_tally}')

not_working_spectrum_table = Table([not_working_spectra],
                       names=['Not Working Spectra (all)'])

not_working_txt_spectrum_table = Table([not_working_txt],
                       names=['Not Working Spectra (.txt)'])

wcs1d_spectrum_table = Table([wcs1d_fits_spectra],
                       names=['wcs1d-fits Spectra'])

print(not_working_spectrum_table)
print(not_working_txt_spectrum_table)
print(wcs1d_spectrum_table)


#plt.plot(spec.wavelength, spec.flux)
#plt.show()
