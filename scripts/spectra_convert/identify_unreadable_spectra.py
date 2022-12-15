from astropy.io import ascii
from scripts.ingests.utils import *
from specutils import Spectrum1D
import astropy.units as u

logger.setLevel(logging.DEBUG)

db = load_simpledb('SIMPLE.db', recreatedb=False)
data = db.inventory('2MASS J13571237+1428398',pretty_print= True)


table = db.query(db.Spectra).table()
print(table.info)

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
not_working_names =[]

not_working_txt = []
txt_names=[]

wcs1d_fits_spectra = []
wcs1d_fits_units_issue = []
wcs1d_fits_names = []
units_issue_names =[]

spex_prism_spectra = []
spex_prism_names= []
#ASCII, iraf, tabular-fits, wcs1d-fits, Spex Prism, ecsv
#spectrum

for n ,spectrum in enumerate(table):
    file = os.path.basename(spectrum['spectrum'])
    file_root = os.path.splitext(file)[1]
    length = len(table)
    #print(n,spectrum[0])
    try:
        spec = Spectrum1D.read(spectrum['spectrum'], format = 'wcs1d-fits')
        wcs1d_fits_tally +=1
        wcs1d_fits_spectra.append(spectrum['spectrum'])
        wcs1d_fits_names.append(spectrum['source'])
        print(spectrum['source'])
        try:
            spec.wavelength.to(u.micron).value
        except:
            fits_units += 1
            wcs1d_fits_units_issue.append(spectrum['spectrum'])
            units_issue_names.append(spectrum['source'])
    except Exception as e_wcs1d:
        not_working_errors.append(f'wcs1d err: {e_wcs1d} \n') # this does not work -
        try:
            spec = Spectrum1D.read(spectrum['spectrum'], format = 'Spex Prism')
            spex_prism_tally += 1
            spex_prism_spectra.append(spectrum['spectrum'])
            spex_prism_names.append(spectrum['source'])
            try:
                spec.wavelength.to(u.micron).value
            except:
                spex_units += 1
        except Exception as e_spex:
            not_working_errors.append(f'spex prism err: {e_spex} \n') # add to every error
            try:
                spec = Spectrum1D.read(spectrum['spectrum'], format = 'iraf')
                iraf_tally += 1
            except Exception as e_iraf:
                try:
                    spec = Spectrum1D.read(spectrum['spectrum'], format = 'tabular-fits')
                    tabularfits_tally += 1
                except Exception as e_tabular:
                    try:
                        spec = Spectrum1D.read(spectrum['spectrum'], format = 'ASCII')
                        ascii_tally += 1
                    except Exception as e_ascii:
                        not_working_errors.append(f'ascii err: {e_ascii} \n') # add to every error
                        not_working_spectra.append(spectrum['spectrum'])
                        not_working_names.append(spectrum['source'])
                        not_working_tally += 1
                        if file_root == '.txt':
                            not_working_txt.append(spectrum['spectrum'])
                            txt_names.append(spectrum['source'])





print(f'wcs1d fits tally: {wcs1d_fits_tally}')
print(f'fits units broken: {fits_units}')
print(f'spex prism tally: {spex_prism_tally}')
print(f'spex units broken: {spex_units}')
print(f'iraf tally: {iraf_tally}')
print(f'tabularfits tally: {tabularfits_tally}')
print(f'ascii tally: {ascii_tally}')
print(f'not_working_tally: {not_working_tally}')
print(f'number of spectra in database: {length}')
print(f'total tally:{wcs1d_fits_tally + spex_prism_tally + iraf_tally + tabularfits_tally +ascii_tally +not_working_tally}')

#table for all not wokring spectra
data_not_working = Table([not_working_names,not_working_spectra],
                       names=('source','spectrum')) #add column names source and url
ascii.write(data_not_working, 'not_working_table.dat', overwrite=True)


#table for not wokring .txt spectra
data_not_working_txt = Table([txt_names, not_working_txt],
                       names=('source','spectrum'))
ascii.write(data_not_working_txt, 'not_working_txt_table.dat', overwrite=True)


#table for wcs1d-fits spectra
data = Table([wcs1d_fits_names,wcs1d_fits_spectra],
                       names=('source','spectrum'))
ascii.write(data, 'wcs1d_spectrum_table.dat', overwrite=True)


#table for wcs1d spectra w units errors
units_data = Table([units_issue_names, wcs1d_fits_units_issue],
                        names=('source','spectrum'))
ascii.write(units_data, 'wcs1d_convert_unit_table.dat', overwrite=True)


#table for spex prism spectra
spex_data = Table([spex_prism_names, spex_prism_spectra],
                        names=('source','spectrum'))
ascii.write(spex_data, 'spex_prism_table.dat', overwrite=True)


#plt.plot(spec.wavelength, spec.flux)
#plt.show()
