from astrodbkit import astrodb
import pandas as pd

db = astrodb.get_db('./BDNYCv1.0.db')

txt = 'SELECT * FROM sources'
#data = db.query(txt, fmt='table', export='temp.txt') # if fmt=table or dict, the columns are sorted. if array no columns names are given
data = db.query(txt, fmt='table')

txt = 'SELECT s.id, s.ra, s.dec, s.shortname, p.source_id, p.band, p.magnitude ' \
      'FROM sources as s JOIN photometry as p ON s.id=p.source_id ' \
      'WHERE s.dec<=-10 AND (p.band IN ("J","H","Ks","W1","W2"))'
t = db.query(txt, fmt='table')

# Complex way:
data = pd.DataFrame()
for col in t.keys():
    data.loc[:,col] = pd.Series(t[col])
# EASIER THIS WAY:
data = t.to_pandas()

table = 'sources'
columns = db.query("PRAGMA table_info({})".format(table), unpack=True)[1]
# List of tables
tables = db.query("SELECT * FROM sqlite_master WHERE type='table'", unpack=True)[1]
# Tables to consider for column sorting
tables = ['sources', 'spectral_types', 'photometry', 'parallaxes', 'proper_motions', 'systems', \
          'radial_velocities', 'instruments', 'telescopes', 'publications', 'spectra', 'modes']

# None of these work like I would like...
pivoted = data.pivot(columns='band', values='magnitude')
pivoted.groupby(level=0).mean()
melted = pd.melt(data, id_vars=['id','source_id','shortname','ra','dec'])
p2 = pd.pivot_table(data, columns=['band','magnitude'])


import matplotlib.pyplot as plt
txt = 'SELECT spectrum FROM spectra WHERE id=3'
t = db.query(txt, fetch='one', fmt='dict')
spec = t['spectrum']
plt.loglog(spec.data[0], spec.data[1])

