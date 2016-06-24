from flask import Flask, render_template, request, redirect, make_response
from astrodbkit import astrodb
import os
import sys
from cStringIO import StringIO
from bokeh.plotting import figure
from bokeh.embed import components
from bokeh.models import ColumnDataSource, HoverTool, OpenURL, TapTool
from astropy import units as u
from astropy.coordinates import SkyCoord
import pandas as pd
from collections import OrderedDict
from math import sqrt
import numpy as np

app_bdnyc = Flask(__name__)

app_bdnyc.vars = dict()
app_bdnyc.vars['query'] = ''
app_bdnyc.vars['search'] = ''
app_bdnyc.vars['specid'] = ''
app_bdnyc.vars['source_id'] = ''

# Redirect to the main page
@app_bdnyc.route('/')
@app_bdnyc.route('/index')
@app_bdnyc.route('/index.html')
@app_bdnyc.route('/query.html')
def bdnyc_home():
    return redirect('/query')

# Page with a text box to take the SQL query
@app_bdnyc.route('/query', methods=['GET', 'POST'])
def bdnyc_query():
    defquery = 'SELECT * FROM sources'
    if app_bdnyc.vars['query']=='':
        app_bdnyc.vars['query'] = defquery

    # Get the number of objects
    db = astrodb.Database('./database.db')
    t = db.query('SELECT id FROM sources', fmt='table')
    bd_num = len(t)


    return render_template('query.html', defquery=app_bdnyc.vars['query'],
                           defsearch=app_bdnyc.vars['search'], specid=app_bdnyc.vars['specid'],
                           source_id=app_bdnyc.vars['source_id'], bd_num=bd_num)


# Grab results of query and display them
@app_bdnyc.route('/runquery', methods=['POST'])
def bdnyc_runquery():
    app_bdnyc.vars['query'] = request.form['query_to_run']
    htmltxt = app_bdnyc.vars['query'].replace('<', '&lt;')

    # Load the database
    db = astrodb.Database('./database.db')

    # Only SELECT commands are allowed
    if not app_bdnyc.vars['query'].lower().startswith('select'):
        return render_template('error.html', headermessage='Error in Query',
                               errmess='<p>Only SELECT queries are allowed. You typed:</p><p>'+htmltxt+'</p>')

    # Run the query
    stdout = sys.stdout  # Keep a handle on the real standard output
    sys.stdout = mystdout = StringIO()  # Choose a file-like object to write to
    try:
        t = db.query(app_bdnyc.vars['query'], fmt='table')
    except ValueError:
        t = db.query(app_bdnyc.vars['query'], fmt='array')
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

    return render_template('view.html', table=data.to_html(classes='display', index=False))


@app_bdnyc.route('/savefile', methods=['POST'])
def bdnyc_savefile():
    export_fmt = request.form['format']
    if export_fmt == 'votable':
        filename = 'bdnyc_table.vot'
    else:
        filename = 'bdnyc_table.txt'

    db = astrodb.Database('./database.db')
    db.query(app_bdnyc.vars['query'], fmt='table', export=filename)
    with open(filename, 'r') as f:
        file_as_string = f.read()
    os.remove(filename)  # Delete the file after it's read

    response = make_response(file_as_string)
    response.headers["Content-Disposition"] = "attachment; filename=%s" % filename
    return response


# Perform a search
@app_bdnyc.route('/search', methods=['POST'])
def bdnyc_search():
    app_bdnyc.vars['search'] = request.form['search_to_run']
    search_table = 'sources'
    search_value = app_bdnyc.vars['search']

    # Load the database
    db = astrodb.Database('./database.db')

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
                                           '<p>' + app_bdnyc.vars['search'] + '</p>')

    # Run the search
    stdout = sys.stdout  # Keep a handle on the real standard output
    sys.stdout = mystdout = StringIO()  # Choose a file-like object to write to
    t = db.search(search_value, search_table, fetch=True)
    sys.stdout = stdout

    try:
        data = t.to_pandas()
    except AttributeError:
        return render_template('error.html', headermessage='Error in Search',
                               errmess=mystdout.getvalue().replace('<', '&lt;'))

    return render_template('view_search.html', table=data.to_html(classes='display', index=False))


# Plot a spectrum
@app_bdnyc.route('/spectrum', methods=['POST'])
def bdnyc_plot():
    app_bdnyc.vars['specid'] = request.form['spectrum_to_plot']

    # If not a number, error
    if not app_bdnyc.vars['specid'].isdigit():
        return render_template('error.html', headermessage='Error in Input',
                               errmess='<p>Input was not a number.</p>')

    # Load the database
    db = astrodb.Database('./database.db')

    # Grab the spectrum
    stdout = sys.stdout  # Keep a handle on the real standard output
    sys.stdout = mystdout = StringIO()  # Choose a file-like object to write to
    query = 'SELECT spectrum, flux_units, wavelength_units, source_id, instrument_id, telescope_id ' + \
            'FROM spectra WHERE id=' + app_bdnyc.vars['specid']
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

    # Make the plot
    tools = "resize,crosshair,pan,wheel_zoom,box_zoom,reset"

    # create a new plot
    wav = 'Wavelength ('+t['wavelength_units']+')'
    flux = 'Flux ('+t['flux_units']+')'
    # can specify plot_width if needed
    p = figure(tools=tools, title=shortname, x_axis_label=wav, y_axis_label=flux, plot_width=800)

    p.line(spec.data[0], spec.data[1], line_width=2)

    script, div = components(p)

    return render_template('spectrum.html', script=script, plot=div)


# Check inventory
@app_bdnyc.route('/inventory', methods=['POST'])
@app_bdnyc.route('/inventory/<int:source_id>')
def bdnyc_inventory(source_id=None):
    if source_id is None:
        app_bdnyc.vars['source_id'] = request.form['id_to_check']
        path = ''
    else:
        app_bdnyc.vars['source_id'] = source_id
        path = '../'

    # Load the database
    db = astrodb.Database('./database.db')

    # Grab inventory
    stdout = sys.stdout
    sys.stdout = mystdout = StringIO()
    t = db.inventory(app_bdnyc.vars['source_id'], fetch=True, fmt='table')
    sys.stdout = stdout

    # Check for errors (no results)
    if mystdout.getvalue().lower().startswith('no source'):
        return render_template('error.html', headermessage='No Results Found',
                               errmess='<p>'+mystdout.getvalue().replace('<', '&lt;')+'</p>')

    # Empty because of invalid input
    if len(t) == 0:
        return render_template('error.html', headermessage='Error',
                               errmess="<p>You typed: "+app_bdnyc.vars['source_id']+"</p>")

    return render_template('inventory.html',
                           tables=[t[x].to_pandas().to_html(classes='display', index=False) for x in t.keys()],
                           titles=['na']+t.keys(), path=path, source_id=app_bdnyc.vars['source_id'])


# Check Schema
@app_bdnyc.route('/schema.html', methods=['GET', 'POST'])
@app_bdnyc.route('/schema', methods=['GET', 'POST'])
def bdnyc_schema():

    # Load the database
    db = astrodb.Database('./database.db')

    # Get table names and their structure
    table_names = db.query("SELECT name FROM sqlite_sequence", unpack=True)[0]

    table_dict = dict()
    for name in table_names:
        temptab = db.query('PRAGMA table_info('+name+')', fmt='table')
        table_dict[name] = temptab

    return render_template('schema.html',
                           tables=[table_dict[x].to_pandas().to_html(classes='display', index=False)
                                   for x in sorted(table_dict.keys())],
                           titles=['na']+sorted(table_dict.keys()))


@app_bdnyc.route('/summary/<int:source_id>')
def bdnyc_summary(source_id):
    """Create a summary page for the requested star"""

    # Load the database
    db = astrodb.Database('./database.db')

    t = db.inventory(source_id, fetch=True, fmt='table')

    # Grab object information
    objname = t['sources']['designation'][0]
    ra = t['sources']['ra'][0]
    dec = t['sources']['dec'][0]
    c = SkyCoord(ra=ra * u.degree, dec=dec * u.degree)
    coords = c.to_string('hmsdms', sep=':', precision=2)
    allnames = t['sources']['names'][0]

    # TODO: Grab distance
    # distance = 1000./t['parallaxes']['parallax'][0]

    # TODO: Grab spectral types
    # t['spectral_types']['spectral_type'].tolist()
    # t['spectral_types']['regime'].tolist()

    # TODO: Consider getting a 2MASS finder chart?

    phot_dict = {'J': 1.24, 'H': 1.66, 'K': 2.19, 'Ks': 2.16, 'W1': 3.35, 'W2': 4.6, 'W3': 11.56, 'W4': 22.09,
                 '[3.6]': 3.51, '[4.5]': 4.44, '[5.8]': 5.63, '[8]': 7.59, 'g': .48, 'i': .76, 'r': .62, 'u': .35,
                 'z': .91}
    phot_data = t['photometry'].to_pandas()
    phot_txt = '<p>'
    for band in OrderedDict(sorted(phot_dict.items(), key=lambda t: t[1])):
        if band in phot_data['band'].tolist():
            if phot_data[phot_data['band']==band]['magnitude_unc'].values[0] == 'null':
                phot_txt += '<strong>{0}</strong>: >{1:.2f}<br>'.format(band,
                                                                           phot_data[phot_data['band'] == band][
                                                                               'magnitude'].values[0])
            else:
                phot_txt += '<strong>{0}</strong>: {1:.2f} +/- {2:.2f}<br>'.format(band,
                                                             phot_data[phot_data['band']==band]['magnitude'].values[0],
                                                             float(phot_data[phot_data['band']==band]['magnitude_unc'].values[0]))
    phot_txt += '</p>'

    # Grab spectra
    spec_list = t['spectra']['id']
    plot_list = list()
    warnings = list()

    for spec_id in spec_list:
        stdout = sys.stdout  # Keep a handle on the real standard output
        sys.stdout = mystdout = StringIO()  # Choose a file-like object to write to
        query = 'SELECT spectrum, flux_units, wavelength_units, source_id, instrument_id, telescope_id ' + \
                'FROM spectra WHERE id={}'.format(spec_id)
        q = db.query(query, fetch='one', fmt='dict')
        sys.stdout = stdout

        if mystdout.getvalue().lower().startswith('could not retrieve spectrum'):
            warnings.append(mystdout.getvalue())
            continue

        spec = q['spectrum']

        # Get spectrum name
        try:
            query = 'SELECT name FROM telescopes WHERE id={}'.format(q['telescope_id'])
            n1 = db.query(query, fetch='one', fmt='array')[0]
            query = 'SELECT name FROM instruments WHERE id={}'.format(q['instrument_id'])
            n2 = db.query(query, fetch='one', fmt='array')[0]
            plot_name = n1 + ':' + n2
        except:
            plot_name = ''

        # print spec_id, plot_name

        # Make the plot
        tools = "resize,crosshair,pan,wheel_zoom,box_zoom,reset"

        # create a new plot
        wav = 'Wavelength (' + q['wavelength_units'] + ')'
        flux = 'Flux (' + q['flux_units'] + ')'
        # can specify plot_width if needed
        p = figure(tools=tools, title=plot_name, x_axis_label=wav, y_axis_label=flux, plot_width=600)

        p.line(spec.data[0], spec.data[1], line_width=2)

        plot_list.append(p)

    script, div = components(plot_list)

    # table = t['photometry'].to_pandas().to_html(classes='display', index=False)

    return render_template('summary.html',
                           table=phot_txt,
                           script=script, plot=div, name=objname, coords=coords, allnames=allnames, warnings=warnings,
                           source_id=source_id)


@app_bdnyc.route('/browse')
def bdnyc_browse():
    """Examine the full source list with clickable links to object summaries"""

    # Load the database
    db = astrodb.Database('./database.db')

    # Run the query
    t = db.query('SELECT id, ra, dec, shortname, names, comments FROM sources', fmt='table')

    # Convert to Pandas data frame
    data = t.to_pandas()
    data.index = data['id']

    # Change column to a link
    linklist = []
    for i, elem in enumerate(data['shortname']):
        link = '<a href="summary/{0}">{1}</a>'.format(data.iloc[i]['id'], elem)
        linklist.append(link)
    data['shortname'] = linklist
    # TODO: Consider on-hover text tooltips

    # Rename columns
    translation = {'id':'Source ID', 'ra':'RA', 'dec':'Dec', 'names':'Alternate Designations',
                   'comments':'Comments', 'shortname':'Object Shortname'}
    column_names = data.columns.tolist()
    for i, name in enumerate(column_names):
        if name in translation.keys():
            column_names[i] = translation[name]
    data.columns = column_names

    # Count up photometry and spectroscopy for new columns
    df_phot = db.query('SELECT id, source_id FROM photometry', fmt='table').to_pandas()
    phot_counts = df_phot.groupby(by='source_id').count()
    phot_counts.columns = ['Photometry']
    df_spec = db.query('SELECT id, source_id FROM spectra', fmt='table').to_pandas()
    spec_counts = df_spec.groupby(by='source_id').count()
    spec_counts.columns = ['Spectroscopy']

    final_data = pd.concat([data, phot_counts, spec_counts], axis=1, join='inner')

    return render_template('browse.html', table=final_data.to_html(classes='display', index=False, escape=False))


@app_bdnyc.route('/skyplot')
def bdnyc_skyplot():
    """
    Create a sky plot of the database objects
    """

    # Load the database
    db = astrodb.Database('./database.db')
    t = db.query('SELECT id, ra, dec, shortname FROM sources', fmt='table')

    # Convert to Pandas data frame
    data = t.to_pandas()
    data.index = data['id']

    # Coordinate conversion
    c = SkyCoord(ra=data['ra'] * u.degree, dec=data['dec'] * u.degree)
    pi = np.pi
    proj = 'hammer'
    data['x'], data['y'] = projection(c.ra.radian - pi, c.dec.radian, use=proj)
    # data['x'], data['y'] = projection(c.galactic.l.radian-pi, c.galactic.b.radian, use=proj)

    # Make the plot
    source = ColumnDataSource(data=data)

    tools = "resize,tap,pan,wheel_zoom,box_zoom,reset"
    p = figure(tools=tools, title='', plot_width=800, plot_height=600,
               x_range=(-1.05 * (2.0 ** 1.5), 1.3 * 2.0 ** 1.5), y_range=(-2.0 * sqrt(2.0), 1.2 * sqrt(2.0)),
               min_border=0, min_border_bottom=0)

    # Initial figure formatting
    p.axis.visible = None
    p.outline_line_color = None
    p.xgrid.grid_line_color = None
    p.ygrid.grid_line_color = None

    # Add the grid
    rangepts = 50
    raseps = 12
    decseps = 9
    rarange = [-pi + i * 2.0 * pi / rangepts for i in range(0, rangepts + 1)]
    decrange = [-pi / 2.0 + i * pi / rangepts for i in range(0, rangepts + 1)]
    ragrid = [-pi + i * 2.0 * pi / raseps for i in range(0, raseps + 1)]
    decgrid = [-pi / 2.0 + i * pi / decseps for i in range(0, decseps + 1)]

    raxs = []
    rays = []
    for rg in ragrid:
        t1 = [projection(rg, x, use=proj) for x in decrange]
        tx, ty = zip(*t1)
        raxs.append(tx)
        rays.append(ty)

    decxs = []
    decys = []
    for dg in decgrid:
        t1 = [projection(x, dg, use=proj) for x in rarange]
        tx, ty = zip(*t1)
        decxs.append(tx)
        decys.append(ty)

    p.multi_line(raxs, rays, color='#bbbbbb')
    p.multi_line(decxs, decys, color='#bbbbbb')

    # Add the data
    p.scatter('x', 'y', source=source, size=8, alpha=0.6)
    tooltip = [("Source ID", "@id"), ("Name", "@shortname"), ("(RA, Dec)", "(@ra, @dec)")]
    p.add_tools(HoverTool(tooltips=tooltip))

    # When clicked, go to the Summary page
    url = "summary/@id"
    taptool = p.select(type=TapTool)
    taptool.callback = OpenURL(url=url)

    script, div = components(p)

    return render_template('skyplot.html', script=script, plot=div)


def projection(lon, lat, use='aitoff'):
    """
    Convert x,y to Aitoff projection. Lat and Lon should be in radians. RA=lon, Dec=lat
    """
    # TODO: Figure out why aitoff is failing

    # Note that np.sinc is normalized (hence the division by pi)
    if use.lower() == 'hammer':
        x = 2.0 ** 1.5 * np.cos(lat) * np.sin(lon / 2.0) / np.sqrt(1.0 + np.cos(lat) * np.cos(lon / 2.0))
        y = sqrt(2.0) * np.sin(lat) / np.sqrt(1.0 + np.cos(lat) * np.cos(lon / 2.0))
    else: # Aitoff by default
        alpha_c = np.arccos(np.cos(lat) * np.cos(lon / 2.0))
        x = 2.0 * np.cos(lat) * np.sin(lon) / np.sinc(alpha_c / np.pi)
        y = np.sin(lat) / np.sinc(alpha_c / np.pi)
    return x, y
