
#function for loading the data##
#############################################
#need to generate dictionaries -> should just need one dictionary
#try to get header by reading it in as a fits header (TRY THIS FIRST)
#if doesn't work:
# make the header a keyword and value usign the ascii read but add comment to key and value
#can delimit twice for = and /
#add date file was generate and converted by reprocessed
#pass dictionary to convert_to_fits()   function!!!

from urllib.request import urlopen
from astropy.table import Table

spectrum_path = 'https://bdnyc.s3.amazonaws.com/BRI+0021-0214.txt'

def text_spectrum_loader(spectrum_path):
    data = urlopen(spectrum_path).read().decode('UTF-8')
    header_end = '|wavelength'
    lines = data.splitlines()
    for i in range(len(lines)):
        if header_end in lines[i]:
            data_start = i
    header = lines[:data_start]
    spectrum_table = Table.read(data, format='ipac',
                                    data_start = data_start)#need to convert units to Jy
    print(spectrum_table)
    return spectrum_table

text_spectrum_loader(spectrum_path)


#function for loading the data
#nned to generate dictionaries -> should just need one dictionary
