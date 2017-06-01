from flask import Flask, render_template, request, redirect, make_response, url_for
app_onc = Flask(__name__)

import astrodbkit
from astrodbkit import astrodb
import os
import sys
import re
from io import StringIO
from bokeh.plotting import figure
from bokeh.embed import components
from bokeh.models import ColumnDataSource, HoverTool, OpenURL, TapTool, Range1d
from bokeh.models.widgets import Panel, Tabs
from ONCdb import make_onc
from astropy import units as u
from astropy.coordinates import SkyCoord
from astroquery.simbad import Simbad
from scipy.ndimage.interpolation import zoom
import pandas as pd
from collections import OrderedDict
from math import sqrt
import numpy as np

app_onc.vars = dict()
app_onc.vars['query'] = ''
app_onc.vars['search'] = ''
app_onc.vars['specid'] = ''
app_onc.vars['source_id'] = ''

db_file = os.path.join(os.path.dirname(make_onc.__file__),'orion.db')
pd.set_option('max_colwidth', -1)

# Redirect to the main page
@app_onc.route('/')
@app_onc.route('/index')
@app_onc.route('/index.html')
@app_onc.route('/query.html')
def onc_home():
    return redirect('/query')

# Page with a text box to take the SQL query
@app_onc.route('/query', methods=['GET', 'POST'])
def onc_query():
    defquery = 'SELECT * FROM sources'
    if app_onc.vars['query']=='':
        app_onc.vars['query'] = defquery

    return render_template('query.html', defquery=app_onc.vars['query'],
                           defsearch=app_onc.vars['search'], specid=app_onc.vars['specid'],
                           source_id=app_onc.vars['source_id'], version=astrodbkit.__version__)

# Grab results of query and display them
@app_onc.route('/runquery', methods=['POST','GET'])
def onc_runquery():
    db = astrodb.Database(db_file)
    app_onc.vars['query'] = request.form['query_to_run']
    htmltxt = app_onc.vars['query'].replace('<', '&lt;')

    # Only SELECT commands are allowed
    if not app_onc.vars['query'].lower().startswith('select'):
        return render_template('error.html', headermessage='Error in Query',
                               errmess='<p>Only SELECT queries are allowed. You typed:</p><p>'+htmltxt+'</p>')

    # Run the query
    stdout = sys.stdout  # Keep a handle on the real standard output
    sys.stdout = mystdout = StringIO()  # Choose a file-like object to write to
    try:
        t = db.query(app_onc.vars['query'], fmt='table', use_converters=False)
    except ValueError:
        t = db.query(app_onc.vars['query'], fmt='array', use_converters=False)
    except:
        return render_template('error.html', headermessage='Error in Query',
                               errmess='<p>Error in query:</p><p>'+htmltxt+'</p>')
    sys.stdout = stdout

    # Check for any errors from mystdout
    if mystdout.getvalue().lower().startswith('could not execute'):
        return render_template('error.html', headermessage='Error in Query',
                               errmess='<p>Error in query:</p><p>'+mystdout.getvalue().replace('<', '&lt;')+'</p>')

    # Check how many results were found
    if type(t)==type(None):
        return render_template('error.html', headermessage='No Results Found',
                               errmess='<p>No entries found for query:</p><p>' + htmltxt +
                                       '</p><p>'+mystdout.getvalue().replace('<', '&lt;')+'</p>')

    # Convert to Pandas data frame
    try:
        data = t.to_pandas()
    except AttributeError:
        return render_template('error.html', headermessage='Error in Query',
                               errmess='<p>Error for query:</p><p>'+htmltxt+'</p>')
    
    # Create checkbox first column
    buttonlist = []
    for index, row in data.iterrows():
        button = '<input type="checkbox" name="{}" value="{}" />'.format(str(index),repr(list(row)))
        buttonlist.append(button)
    data['Select'] = buttonlist
    cols = data.columns.tolist()
    cols.pop(cols.index('Select'))
    data = data[['Select']+cols]

    try:
        script, div, warning_message = onc_skyplot(t)
    except:
        script = div = warning_message = ''
        
    # Get column names
    cols = [strip_html(str(i)) for i in list(data)[1:]]
    cols = """<input class='hidden' type='checkbox', name='cols' value="{}" checked=True />""".format(cols)
    
    # Change id column to a link
    if 'source_id' in data:
        linklist = []
        for i, elem in enumerate(data['source_id']):
            link = '<a href="inventory/{}">{}</a>'.format(data.iloc[i]['source_id'], elem)
            linklist.append(link)
        data['source_id'] = linklist
    
    # Change spectrum column to a link
    if 'spectrum' in data:
        speclist = []
        for index, row in data.iterrows():
            spec = '<a href="../spectrum/{}"><img class="view" src="static/view.png" /></a>'.format(row['id'])
            speclist.append(spec)
        data['spectrum'] = speclist
        
    # Change image column to a link
    if 'image' in data:
        imglist = []
        for index, row in data.iterrows():
            img = '<a href="../image/{}"><img class="view" src="static/view.png" /></a>'.format(row['id'])
            imglist.append(img)
        data['image'] = imglist

    return render_template('view_search.html', table=data.to_html(classes='display', index=False).replace('&lt;','<').replace('&gt;','>'), query=app_onc.vars['query'],
                            script=script, plot=div, warning=warning_message, cols=cols)

@app_onc.route('/export', methods=['POST'])
def onc_export():
    
    # Get all the checked rows
    checked = request.form
    
    # Get column names
    results = [list(eval(checked.get('cols')))]
    
    for k in sorted(checked):
        if k.isdigit():
            
            # Convert string to list and strip HTML
            vals = eval(checked[k])
            for i,v in enumerate(vals):
                try:
                    vals[i] = str(v).split('>')[1].split('<')[0]
                except:
                    pass
            
            results.append(vals)
    
    # Make an array to export
    results = np.array(results, dtype=str)
    filename = 'ONCdb_results.txt'
    np.savetxt(filename, results, delimiter='|', fmt='%s')
    
    with open(filename, 'r') as f:
        file_as_string = f.read()
    os.remove(filename)  # Delete the file after it's read

    response = make_response(str(file_as_string))
    response.headers["Content-type"] = 'text; charset=utf-8'
    response.headers["Content-Disposition"] = "attachment; filename={}".format(filename)
    return response

# Perform a search
@app_onc.route('/search', methods=['POST'])
def onc_search():
    db = astrodb.Database(db_file)
    app_onc.vars['search'] = request.form['search_to_run']
    search_table = 'sources'
    search_value = app_onc.vars['search']

    # Process search
    search_value = search_value.replace(',', ' ').split()
    if len(search_value) == 1:
        search_value = search_value[0]
    else:
        try:
            search_value = [float(s) for s in search_value]
        except:
            return render_template('error.html', headermessage='Error in Search',
                                   errmess='<p>Could not process search input:</p>' +
                                           '<p>' + app_onc.vars['search'] + '</p>')

    # Run the search
    stdout = sys.stdout  # Keep a handle on the real standard output
    sys.stdout = mystdout = StringIO()  # Choose a file-like object to write to
    
    # Get table of results
    t = db.search(search_value, search_table, fetch=True)
    sys.stdout = stdout

    try:
        data = t.to_pandas()
    except AttributeError:
        return render_template('error.html', headermessage='Error in Search',
                               errmess=mystdout.getvalue().replace('<', '&lt;'))
                               
    # Create checkbox first column
    buttonlist = []
    for index, row in data.iterrows():
        button = '<input type="checkbox" name="{}" value="{}">'.format(str(index),repr(list(row)))
        buttonlist.append(button)
    data['Select'] = buttonlist
    cols = data.columns.tolist()
    cols.pop(cols.index('Select'))
    data = data[['Select']+cols]

    try:
        script, div, warning_message = onc_skyplot(t)
    except:
        script = div = warning_message = ''
        
    # Get column names
    cols = [strip_html(str(i)) for i in list(data)[1:]]
    cols = """<input class='hidden' type='checkbox', name='cols' value="{}" checked=True />""".format(cols)

    return render_template('view_search.html', table=data.to_html(classes='display', index=False).replace('&lt;','<').replace('&gt;','>'), query=search_value,
                            script=script, plot=div, warning=warning_message, cols=cols)


# Plot a spectrum
@app_onc.route('/spectrum', methods=['POST'])
@app_onc.route('/spectrum/<int:specid>')
def onc_spectrum(specid=None):
    db = astrodb.Database(db_file)
    if specid is None:
        app_onc.vars['specid'] = request.form['spectrum_to_plot']
        path = ''
    else:
        app_onc.vars['specid'] = specid
        path = '../'
        
    # If not a number, error
    if not str(app_onc.vars['specid']).isdigit():
        return render_template('error.html', headermessage='Error in Input',
                               errmess='<p>Input was not a number.</p>')

    # Grab the spectrum
    stdout = sys.stdout  # Keep a handle on the real standard output
    sys.stdout = mystdout = StringIO()  # Choose a file-like object to write to
    query = 'SELECT spectrum, flux_units, wavelength_units, source_id, instrument_id, telescope_id ' + \
            'FROM spectra WHERE id={}'.format(app_onc.vars['specid'])
    t = db.query(query, fetch='one', fmt='dict')
    sys.stdout = stdout

    # Check for errors first
    if mystdout.getvalue().lower().startswith('could not execute'):
        return render_template('error.html', headermessage='Error in Query',
                               errmess='<p>Error in query:</p><p>'+mystdout.getvalue().replace('<', '&lt;')+'</p>')

    # Check if found anything
    if isinstance(t, type(None)):
        return render_template('error.html', headermessage='No Result', errmess='<p>No spectrum found.</p>')

    spec = t['spectrum']

    query = 'SELECT shortname FROM sources WHERE id='+str(t['source_id'])
    shortname = db.query(query, fetch='one', fmt='dict')['shortname']
    filepath = t['spectrum'].path

    # Make the plot
    tools = "resize,crosshair,pan,wheel_zoom,box_zoom,reset"

    # create a new plot
    wav = 'Wavelength ('+t['wavelength_units']+')'
    flux = 'Flux ('+t['flux_units']+')'
    # can specify plot_width if needed
    p = figure(tools=tools, title=shortname, x_axis_label=wav, y_axis_label=flux, plot_width=800)

    p.line(spec.data[0], spec.data[1], line_width=2)

    script, div = components(p)

    return render_template('spectrum.html', script=script, plot=div, download=filepath)

# Display an image
@app_onc.route('/image', methods=['POST'])
@app_onc.route('/image/<int:imgid>')
def onc_image(imgid=None):
    db = astrodb.Database(db_file)
    if imgid is None:
        app_onc.vars['imgid'] = request.form['image_to_plot']
        path = ''
    else:
        app_onc.vars['imgid'] = imgid
        path = '../'
        
    # If not a number, error
    if not str(app_onc.vars['imgid']).isdigit():
        return render_template('error.html', headermessage='Error in Input',
                               errmess='<p>Input was not a number.</p>')
                               
    # Grab the spectrum
    stdout = sys.stdout  # Keep a handle on the real standard output
    sys.stdout = mystdout = StringIO()  # Choose a file-like object to write to
    query = 'SELECT image, source_id, instrument_id, telescope_id ' + \
            'FROM images WHERE id={}'.format(app_onc.vars['imgid'])
    t = db.query(query, fetch='one', fmt='dict')
    sys.stdout = stdout
    
    # Check for errors first
    if mystdout.getvalue().lower().startswith('could not execute'):
        return render_template('error.html', headermessage='Error in Query',
                               errmess='<p>Error in query:</p><p>'+mystdout.getvalue().replace('<', '&lt;')+'</p>')
                               
    # Check if found anything
    if isinstance(t, type(None)):
        return render_template('error.html', headermessage='No Result', errmess='<p>No image found.</p>')
        
    try:
        img = t['image'].data
        
        # Down sample so the figure displays faster
        img = zoom(img, 0.05, prefilter=False)
        
        query = 'SELECT shortname FROM sources WHERE id='+str(t['source_id'])
        shortname = db.query(query, fetch='one', fmt='dict')['shortname']
        filepath = t['image'].path
        
        # Make the plot
        tools = "resize,crosshair,pan,wheel_zoom,box_zoom,reset"
        
        # create a new plot
        p = figure(tools=tools, title=shortname, plot_width=800)
        
        # Make the plot
        p.image(image=[img], x=[0], y=[0], dw=[img.shape[0]], dh=[img.shape[1]])
        
        p.x_range = Range1d(0, img.shape[0])
        p.y_range = Range1d(0, img.shape[1])
        
        script, div = components(p)
        
    except IOError:
        script, div, filepath = '', '', ''
    
    return render_template('image.html', script=script, plot=div, download=filepath)
    
# Check inventory
@app_onc.route('/inventory', methods=['POST'])
@app_onc.route('/inventory/<int:source_id>')
def onc_inventory(source_id=None):
    db = astrodb.Database(db_file)
    if source_id is None:
        app_onc.vars['source_id'] = request.form['id_to_check']
        path = ''
    else:
        app_onc.vars['source_id'] = source_id
        path = '../'
    
    # Grab inventory
    stdout = sys.stdout
    sys.stdout = mystdout = StringIO()
    t = db.inventory(app_onc.vars['source_id'], fetch=True, fmt='table')
    sys.stdout = stdout

    # Check for errors (no results)
    if mystdout.getvalue().lower().startswith('no source'):
        return render_template('error.html', headermessage='No Results Found',
                               errmess='<p>'+mystdout.getvalue().replace('<', '&lt;')+'</p>')

    # Empty because of invalid input
    if len(t) == 0:
        return render_template('error.html', headermessage='Error',
                               errmess="<p>You typed: "+app_onc.vars['source_id']+"</p>")
                               
    # Add links to plot spectra and images in inventory tables
    # Direct to data_product view (/spectrum view above) with option to download
    
    # Grab object information
    objname = t['sources']['designation'][0]
    ra = t['sources']['ra'][0]
    dec = t['sources']['dec'][0]
    c = SkyCoord(ra=ra*u.degree, dec=dec*u.degree)
    coords = c.to_string('hmsdms', sep=':', precision=2)
    allnames = t['sources']['names'][0]

    # Grab distance
    try:
        distance = 1000./t['parallaxes']['parallax']
        dist_string = ', '.join(['{0:.2f}'.format(i) for i in distance])
        dist_string += ' pc'
    except:
        dist_string = 'N/A'

    # Grab spectral types; gravity indicators are ignored for now
    try:
        sptype_txt = ''
        types = np.array(t['spectral_types']['spectral_type'].tolist())
        regime = np.array(t['spectral_types']['regime'].tolist())
        if 'OPT' in regime:
            sptype_txt += 'Optical: '
            # Parse string
            ind = np.where(regime == 'OPT')
            sptype_txt += ', '.join([parse_sptype(s) for s in types[ind]])
            sptype_txt += ' '
        if 'IR' in regime:
            sptype_txt += 'Infrared: '
            ind = np.where(regime == 'IR')
            sptype_txt += ', '.join([parse_sptype(s) for s in types[ind]])
            sptype_txt += ' '
        else:
            sptype_txt += ', '.join([parse_sptype(s) for s in types])
    except:
        sptype_txt = ''

    # Grab comments
    comments = t['sources']['comments'][0]
    
    # Get external queries
    smbd = 'http://simbad.u-strasbg.fr/simbad/sim-coo?Coord={}+%2B{}&CooFrame=ICRS&CooEpoch=2000&CooEqui=2000&CooDefinedFrames=none&Radius=10&Radius.unit=arcsec&submit=submit+query'.format(ra,dec)
    vzr = ''
    
    # Create link to spectra
    if 'spectra' in t:
        speclist = []
        for idx,row in enumerate(t['spectra']):
            spec = '<a href="../spectrum/{}"><img class="view" src="../static/view.png" /></a>'.format(row['id'])
            speclist.append(spec)
        t['spectra']['spectrum'] = speclist
    
    # Create link to images
    if 'images' in t:
        imglist = []
        for idx,row in enumerate(t['images']):
            img = '<a href="../image/{}"><img class="view" src="../static/view.png" /></a>'.format(row['id'])
            imglist.append(img)
        t['images']['image'] = imglist
    
    return render_template('inventory.html',
                           tables=[t[x].to_pandas().to_html(classes='display', index=False).replace('&lt;','<').replace('&gt;','>') for x in t.keys()],
                           titles=['na']+list(t.keys()), path=path, source_id=app_onc.vars['source_id'],
                           name=objname, coords=coords, allnames=allnames, distance=dist_string,
                           comments=comments, sptypes=sptype_txt, ra=ra, dec=dec, simbad=smbd, vizier=vzr)


# Check Schema
# @app_onc.route('/schema.html', methods=['GET', 'POST'])
@app_onc.route('/schema', methods=['GET', 'POST'])
def onc_schema():
    db = astrodb.Database(db_file)
    # Get table names and their structure
    try:
        table_names = db.query("SELECT name FROM sqlite_sequence", unpack=True)[0]
    except:
        table_names = db.query("SELECT * FROM sqlite_master WHERE type='table'")['tbl_name']

    table_dict = {}
    for name in table_names:
        temptab = db.query('PRAGMA table_info('+name+')', fmt='table')
        table_dict[name] = temptab

    return render_template('schema.html',
                           tables=[table_dict[x].to_pandas().to_html(classes='display', index=False)
                                   for x in sorted(table_dict.keys())],
                           titles=['na']+sorted(table_dict.keys()))

@app_onc.route('/browse')
def onc_browse():
    """Examine the full source list with clickable links to object summaries"""
    db = astrodb.Database(db_file)
    # Run the query
    t = db.query('SELECT id, ra, dec, shortname, names, comments FROM sources', fmt='table')
    
    try:
        script, div, warning_message = onc_skyplot(t)
    except IOError:
        script = div = warning_message = ''

    # Convert to Pandas data frame
    data = t.to_pandas()
    data.index = data['id']

    # Change column to a link
    linklist = []
    for i, elem in enumerate(data['id']):
        link = '<a href="inventory/{0}">{1}</a>'.format(data.iloc[i]['id'], elem)
        linklist.append(link)
    data['id'] = linklist
    
    # Create checkbox first column
    buttonlist = []
    for index, row in data.iterrows():
        row = [strip_html(str(i)) for i in list(row)]
        button = '<input type="checkbox" name="{}" value="{}">'.format(str(index),repr(row))
        buttonlist.append(button)
    data['Select'] = buttonlist
    cols = data.columns.tolist()
    cols.pop(cols.index('Select'))
    data = data[['Select']+cols]

    # Rename columns
    translation = {'id': 'Source ID', 'ra': '<span title="Right Ascension (deg)">RA</span>',
                   'dec': '<span title="Declination (deg)">Dec</span>', 'names': 'Alternate Designations',
                   'comments': 'Comments', 'shortname': 'Object Shortname'}
    column_names = data.columns.tolist()
    for i, name in enumerate(column_names):
        if name in translation.keys():
            column_names[i] = translation[name]
    data.columns = column_names

    # Count up photometry and spectroscopy for new columns
    df_phot = db.query('SELECT id, source_id FROM photometry', fmt='table').to_pandas()
    phot_counts = df_phot.groupby(by='source_id').count()
    phot_counts.columns = ['<span title="Amount of photometry data available">Photometry</span>']
    df_spec = db.query('SELECT id, source_id FROM spectra', fmt='table').to_pandas()
    spec_counts = df_spec.groupby(by='source_id').count()
    spec_counts.columns = ['<span title="Amount of spectroscopic observations available">Spectroscopy</span>']

    final_data = pd.concat([data, phot_counts, spec_counts], axis=1, join='outer')
    final_data['<span title="Amount of photometry data available">Photometry</span>'].fillna(value=0, inplace=True)
    final_data['<span title="Amount of spectroscopic observations available">Spectroscopy</span>'].fillna(value=0, inplace=True)
    
    cols = [strip_html(str(i)) for i in list(column_names)[1:]]
    cols = """<input class='hidden' type='checkbox', name='cols' value="{}" checked=True />""".format(cols)

    return render_template('browse.html', script=script, div=div, table=final_data.to_html(classes='display', index=False, escape=False), cols=cols)

def strip_html(s):
    return re.sub(r'<[^<]*?/?>','',s)

def onc_skyplot(t):
    """
    Create a sky plot of the database objects
    """
    # Convert to Pandas data frame
    data = t.to_pandas()
    data.index = data['id']

    # Remove objects without RA/Dec
    num_missing = np.sum(pd.isnull(data['ra']))
    if num_missing > 0:
        warning_message = 'Note: {} objects had missing coordinate information and were removed.'.format(num_missing)
        data = data[pd.notnull(data['ra'])]
    else:
        warning_message = ''

    # Coerce to numeric
    data['ra'] = pd.to_numeric(data['ra'])
    data['dec'] = pd.to_numeric(data['dec'])
    
    source = ColumnDataSource(data=data)

    tools = "resize,tap,pan,wheel_zoom,box_zoom,reset"
    p = figure(tools=tools, title='', plot_width=500, plot_height=300, min_border=0, min_border_bottom=0)

    # Add the data
    p.scatter('ra', 'dec', source=source, size=8, alpha=0.6)
    tooltip = [("Source ID", "@id"), ("Name", "@shortname"), ("(RA, Dec)", "(@ra, @dec)")]
    p.add_tools(HoverTool(tooltips=tooltip))

    # When clicked, go to the Summary page
    url = "inventory/@id"
    taptool = p.select(type=TapTool)
    taptool.callback = OpenURL(url=url)
    
    # Axis labels
    p.yaxis.axis_label = 'Decl. (deg)'
    p.xaxis.axis_label = 'R.A. (deg)'

    script, div = components(p)

    return script, div, warning_message


@app_onc.route('/feedback')
def onc_feedback():
    return render_template('feedback.html')

# Photometry dictionary in microns
phot_dict = {'J': 1.24, 'H': 1.66, 'K': 2.19, 'Ks': 2.16, 'W1': 3.35, 'W2': 4.6, 'W3': 11.56, 'W4': 22.09,
             '[3.6]': 3.51, '[4.5]': 4.44, '[5.8]': 5.63, '[8]': 7.59, '[24]': 23.68, 'g': .48, 'i': .76, 'r': .62,
             'u': .35, 'z': .91, '2MASS_J': 1.24, '2MASS_H': 1.66, '2MASS_Ks': 2.16, 'WISE_W1': 3.35,
             'WISE_W2': 4.6, 'WISE_W3': 11.56, 'WISE_W4': 22.09, 'IRAC_ch1': 3.51, 'IRAC_ch2': 4.44,
             'IRAC_ch3': 5.63, 'IRAC_ch4': 7.59, 'SDSS_g': .48, 'SDSS_i': .76, 'SDSS_r': .62, 'SDSS_u': .35,
             'SDSS_z': .91, 'HST_F105W': 1.0552, 'HST_F110W': 1.1534, 'HST_F125W': 1.2486, 'HST_F140W': 1.3923,
             'Johnson_B': 0.442, 'Johnson_U': 0.364, 'Johnson_V': 0.540,
             'MKO_H': 1.635, 'MKO_J': 1.25, 'MKO_K': 2.20, "MKO_L'": 3.77, "MKO_M'": 4.68, 'MKO_Y': 1.02,
             'DENIS_I': 0.82, 'DENIS_J': 1.25, 'DENIS_K': 2.15, 'DENIS_Ks': 2.15,
             'Cousins_I': 0.647, 'Cousins_R': 0.7865,
             'I': 0.806, 'L': 3.45, 'Y': 1.02, 'Z': 0.9, 'y': 1.02,
             'GALEX_FUV': 0.1528, 'GALEX_NUV': 0.2271,
             'FourStar_J2': 1.144, 'FourStar_J3': 1.287}

def parse_sptype(spnum):
    """Parse a spectral type number and return a string"""

    # Attempt to convert to float
    try:
        spnum = float(spnum)
    except ValueError:
        pass

    if spnum >= 30:
        sptxt = 'Y'
    if (spnum >= 20) & (spnum < 30):
        sptxt = 'T'
    if (spnum >= 10) & (spnum < 20):
        sptxt = 'L'
    if (spnum >= 0) & (spnum < 10):
        sptxt = 'M'
    if spnum < 0:
        sptxt = 'K'

    if isinstance(spnum, type('')):
        sptxt = spnum
    else:
        sptxt += '{0:.1f}'.format(abs(spnum) % 10)

    return sptxt
    
    
    
    
    
    