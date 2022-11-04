
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

not_working_set = set()
not_working_spectra = set()
not_working_txt = set()
# list = []
# .append


#ASCII, iraf, tabular-fits, wcs1d-fits, Spex Prism, ecsv
for n ,spectrum in enumerate(table):
    file = os.path.basename(spectrum[0])
    file_root = os.path.splitext(file)[1]
    #print(n,spectrum[0])
    try:
        spec = Spectrum1D.read(spectrum[0], format = 'wcs1d-fits')
        wcs1d_fits_tally +=1
        try:
            spec.wavelength.to(u.micron).value
        except:
            fits_units += 1
        #print(f'wcs1d fits tally: {wcs1d_fits_tally}')
    except Exception as e_wcs1d:
        not_working_set.add(f'wcs1d err: {e_wcs1d} \n') # add to every error
        not_working_set.add(e_wcs1d)
        not_working_spectra.add(spectrum[0])
        #not_working_tally += 1
        try:
            spec = Spectrum1D.read(spectrum[0], format = 'Spex Prism')
            spex_prism_tally += 1

            try:
                spec.wavelength.to(u.micron).value
            except:
                spex_units += 1
            #print(f'spex prism tally: {spex_prism_tally}')
        except Exception as e_spex:
            not_working_set.add(f'wcs1d err: {e_spex} \n') # add to every error
            not_working_set.add(e_spex)
            not_working_spectra.add(spectrum[0])
            #not_working_tally += 1
            try:
                spec = Spectrum1D.read(spectrum[0], format = 'iraf')
                iraf_tally += 1
                #print(f'iraf prism tally: {iraf_tally}')
            except Exception as e_iraf:
                try:
                    spec = Spectrum1D.read(spectrum[0], format = 'tabular-fits')
                    tabularfits_tally += 1
                    #print(f'tabular fits tally: {tabularfits_tally}')
                except Exception as e_tabular:
                    try:
                        spec = Spectrum1D.read(spectrum[0], format = 'ASCII')
                        ascii_tally += 1
                        #print(f'ascii tally: {ascii_tally}')
                    except Exception as e_ascii:
                        not_working_set.add(f'wcs1d err: {e_ascii} \n') # add to every error
                        not_working_set.add(e_ascii)
                        not_working_spectra.add(spectrum[0])
                        not_working_tally += 1
                        if file_root == '.txt':
                            not_working_txt.add(spectrum[0])
                        #print(f'not_working_tally: {not_working_tally}')





print(f'wcs1d fits tally: {wcs1d_fits_tally}')
print(f'fits units broken: {fits_units}')
print(f'spex prism tally: {spex_prism_tally}')
print(f'spex units broken: {spex_units}')
print(f'iraf tally: {iraf_tally}')
print(f'tabularfits tally: {tabularfits_tally}')
print(f'ascii tally: {ascii_tally}')
print(f'not_working_tally: {not_working_tally}')


#print(not_working_set)
#print(not_working_spectra) # save as a csv


with open('notworkingspectra.csv', 'w') as f:

    write = csv.writer(f)

    write.writerow(not_working_spectra)


with open('notworking_txt_spectra.csv', 'w') as f:

    write = csv.writer(f)

    write.writerow(not_working_txt)


#make a loader like in convert_VHS1256b.py to load in unreadable txt files
#with file kelle sent
#not loop yet focus on one file

##############################################################
#check what unreadible were - optical?

#plt.plot(spec.wavelength, spec.flux)
#plt.show()
