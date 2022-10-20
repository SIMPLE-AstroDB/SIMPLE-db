
from scripts.ingests.utils import *
from specutils import Spectrum1D


logger.setLevel(logging.DEBUG)

db = load_simpledb('SIMPLE.db', recreatedb=False)
#data = db.inventory('2MASS J13571237+1428398',pretty_print= True)

table = db.query(db.Spectra.c.spectrum).table()
#print(table.info)

tabularfits_tally = 0
ascii_tally = 0
iraf_tally = 0
spex_prism_tally = 0
wcs1d_fits_tally = 0
not_working_tally =0
ecsv_tally = 0

not_working_set = set()
not_working_spectra = set()


#ASCII, iraf, tabular-fits, wcs1d-fits, Spex Prism, ecsv
for n ,spectrum in enumerate(table):
    #print(n,spectrum[0])
    try:
        spec = Spectrum1D.read(spectrum[0], format = 'iraf')
        iraf_tally +=1
        #print(f'iraf tally: {iraf_tally}')
    except Exception as e_iraf:
        try:
            spec = Spectrum1D.read(spectrum[0], format = 'tabular-fits')
            tabularfits_tally += 1
            #print(f'tabularfits tally: {tabularfits_tally}')
        except Exception as e_tabularfits:
            try:
                spec = Spectrum1D.read(spectrum[0], format = 'ASCII')
                ascii_tally += 1
                #print(f'ascii tally: {ascii_tally}')
            except Exception as e_ascii:
                try:
                    spec = Spectrum1D.read(spectrum[0], format = 'Spex Prism')
                    spex_prism_tally += 1
                    #print(f'spex prism tally: {spex_prism_tally}')
                except Exception as e_spex:
                    try:
                        spec = Spectrum1D.read(spectrum[0], format = 'wcs1d-fits')
                        wcs1d_fits_tally += 1
                        #print(f'wcs1d fits tally: {wcs1d_fits_tally}')
                    except Exception as e_wcs1d:
                        try:
                            spec = Spectrum1D.read(spectrum[0], format = 'ECSV')
                            ecsv_tally += 1
                            #print(f'ECSV tally : {ecsv_tally}')
                        except Exception as e_ecsv:
                            not_working_set.add(e_ecsv)
                            not_working_spectra.add(spectrum[0])
                            not_working_tally += 1
                            #print(f'not_working_tally: {not_working_tally}')






print(f'iraf tally: {iraf_tally}')
print(f'tabularfits tally: {tabularfits_tally}')
print(f'ascii tally: {ascii_tally}')
print(f'spex prism tally: {spex_prism_tally}')
print(f'wcs1d fits tally: {wcs1d_fits_tally}')
print(f'not_working_tally: {not_working_tally}')
print(f'ECSV tally : {ecsv_tally}')


print(not_working_set)
print(not_working_spectra)

#check what unreadible were - optical?

#plt.plot(spec.wavelength, spec.flux)
#plt.show()



