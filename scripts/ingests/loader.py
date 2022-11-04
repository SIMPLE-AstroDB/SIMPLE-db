from urllib.request import urlopen
from astropy.table import Table
import astropy.units as u

#reorganize loader
#find start of header
#end of header
#start of data
#use data_start in Table
#loop thorugh the file until you find wavelength
#data starts at |wavelength
#try to no use names=[]
# adn untis = [] bc shoudl be able to auto identify
#fomrat is: format = 'ipac'

#function for loading the data##
#############################################
#need to generate dictionaries -> should just need one dictionary
#try to get header by reading it in as a fits header (TRY THIS FIRST)
#if doesn't work:
# make the header a keyword and value usign the ascii read but add comment to key and value
#can delimit twice for = and /
#add date file was generate and converted by reprocessed
#pass dictionary to convert_to_fits()   function!!!

#

spectrum_path = 'https://bdnyc.s3.amazonaws.com/BRI+0021-0214.txt'
wavelength = []
flux = []
flux_unc = []

def text_spectrum_loader(spectrum_path):
    data = urlopen(spectrum_path)
    lines = data.readlines()[243:]
    # find start of data using string matching
    header_end = 240
    header = lines[:header_end]
    data_start = 243
    spectrum_table = Table([wavelength,flux,flux_unc],
                                names=['wavelength', 'flux', 'flux_uncertainty'], #may not need
                                units=[u.micron, u.Jy, u.Jy],
                           data_start = data_start)
"""    for line in lines: # files are iterable
        split_line = line.split()

        wavelength.append(split_line[0])
        flux.append(split_line[1])
        flux_unc.append(split_line[2])
        spectrum_table = Table([wavelength,flux,flux_unc],
                                names=['wavelength', 'flux', 'flux_uncertainty'],
                                units=[u.micron, u.Jy, u.Jy])"""
    print(spectrum_table)

    return spectrum_table

text_spectrum_loader(spectrum_path)


#function for loading the data
#nned to generate dictionaries -> should just need one dictionary






'''def text_spectrum_loader(spectrum_path):
    data = urlopen(spectrum_path) # it's a file like object and works just like a file
    for line in data: # files are iterable
        split_line = line.split()
        if split_line:
            if split_line[0] == '\\\\':
                line[0] = line[0]+line[1]
                line.delete[1]
            #add the zeroeth element as the key to your dictionary and the rest as a value of that key in your dict
            #dict.add(split_line[0], split_line[1:])
        #How to extract header data?
        try:
            print(split_line[0])
            print(split_line[1])
        except IndexError:
            pass

    return

text_spectrum_loader(spectrum_path)'''

