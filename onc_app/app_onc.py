from flask import Flask, render_template, request, redirect, make_response, url_for
app_onc = Flask(__name__)

import astrodbkit
from astrodbkit import astrodb
from SEDkit import sed
from SEDkit import utilities as u
import os
import sys
import re
from io import StringIO
from bokeh.plotting import figure
from bokeh.embed import components
from bokeh.models import ColumnDataSource, HoverTool, OpenURL, TapTool, Range1d
from bokeh.models.widgets import Panel, Tabs
from astropy import units as q
from astropy.coordinates import SkyCoord
import astropy.constants as ac
from scipy.ndimage.interpolation import zoom
import pandas as pd
import numpy as np

app_onc.vars = dict()
app_onc.vars['query'] = ''
app_onc.vars['search'] = ''
app_onc.vars['specid'] = ''
app_onc.vars['source_id'] = ''

db_file = os.environ['ONC_database']
db = astrodb.Database(db_file)
pd.set_option('max_colwidth', -1)

# Redirect to the main page
@app_onc.route('/')
@app_onc.route('/index')

# Page with a text box to take the SQL query
@app_onc.route('/index', methods=['GET', 'POST'])
def onc_query():
    defquery = 'SELECT * FROM sources'
    if app_onc.vars['query']=='':
        app_onc.vars['query'] = defquery

    table_names = db.query("select * from sqlite_master where type='table'")['name']

    tables = '\n'.join(['<option value="{0}" {1}> {0}</option>'.format(t,'selected=selected' if t=='sources' else '') for t in table_names])

    columns_html = []
    columns_js = []
    for tab in table_names:
        cols = list(db.query("pragma table_info('{}')".format(tab))['name'])

        col_html = ''.join(['<input type="checkbox" value="{0}" name="selections"> {0}<br>'.format(c) for c in cols])
        columns_html.append('<div id="{}" class="columns" style="display:none">{}</div>'.format(tab,col_html))

        col_js = ','.join(["{id:'"+c+"',label:'"+c+"',type:'string'}" for c in cols])
        columns_js.append(col_js)

    column_select = ''.join(columns_html)
    column_script = ''.join(columns_js)

    return render_template('index.html',
                           defsearch=app_onc.vars['search'], specid=app_onc.vars['specid'],
                           source_id=app_onc.vars['source_id'], version=astrodbkit.__version__,
                           tables=tables, column_select=column_select, column_script=col_js)

# Grab results of query and display them
@app_onc.route('/runquery', methods=['POST','GET'])
def onc_runquery():
    # db = astrodb.Database(db_file)
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
                                        
    # Remane RA and Dec columns
    for idx,name in enumerate(t.colnames):
        if name.endswith('.ra'):
            t[name].name =  'ra'
        if name.endswith('.dec'):
            t[name].name =  'dec'
        if name.endswith('.id'):
            t[name].name =  'id'
        if name.endswith('.source_id'):
            t[name].name =  'source_id'
            
    # Convert to Pandas data frame
    try:
        data = t.to_pandas()
    except AttributeError:
        return render_template('error.html', headermessage='Error in Query',
                               errmess='<p>Error for query:</p><p>'+htmltxt+'</p>')
    
    try:
        sources = data[['ra','dec','id']].values.tolist()
        sources = [[i[0], i[1], 'Source {}'.format(int(i[2])), int(i[2])] for i in sources]
    except:
        try:
            sources = data[['ra','dec','source_id']].values.tolist()
            sources = [[i[0], i[1], 'Source {}'.format(int(i[2])), int(i[2])] for i in sources]
        except:
            sources = ''
    
    # Create checkbox first column
    data = add_checkboxes(data)
        
    # Toggle columns
    cols = 'Toggle Column: '+' - '.join(['<a class="toggle-vis" />{}</a>'.format(name) for i,name in enumerate(t.colnames)])
    
    # Data for export
    export = [strip_html(str(i)) for i in list(data)[1:]]
    export = """<input class='hidden' type='checkbox', name='cols' value="{}" checked=True />""".format(export)
    
    # Add links to columns
    data = link_columns(data, db, ['id','source_id','spectrum','image'])
    
    # Get numerical x and y axes for plotting
    columns = [c for c in t.colnames if any([isinstance(i, (int, float)) for i in t[c]])]
    axes = '\n'.join(['<option value="{}"> {}</option>'.format(repr(b)+","+repr(list(t[b])), b) for b in columns])
    
    table_html = data.to_html(classes='display', index=False).replace('&lt;','<').replace('&gt;','>')
    print(table_html)
    
    return render_template('results.html', table=table_html, query=app_onc.vars['query'], cols=cols,
                            sources=sources, axes=axes, export=export)

# Grab results of query and display them
@app_onc.route('/buildquery', methods=['POST', 'GET'])
def onc_buildquery():

    # Build the query from all the input
    entries = request.form
    print(entries)
    selections, builder_rules = [], []
    for key in entries.keys():
        for value in entries.getlist(key):
            if key=='selections':
                selections.append(value)
            if key.startswith('builder_rule'):
                builder_rules.append((key,value))
    
    # Translate the builder rules into a SQL WHERE clause
    where_clause = ''
    for k,v in builder_rules:
        pass
    
    if where_clause:
        where_clause = ' WHERE {}'.format(where_clause)

    build_query = "SELECT {} FROM {}{}".format(','.join(selections), entries['table'], where_clause)

    # db = astrodb.Database(db_file)
    app_onc.vars['query'] = build_query
    htmltxt = app_onc.vars['query'].replace('<', '&lt;')

    # Only SELECT commands are allowed
    if not app_onc.vars['query'].lower().startswith('select'):
        return render_template('error.html', headermessage='Error in Query',
                               errmess='<p>Only SELECT queries are allowed. You typed:</p><p>' + htmltxt + '</p>')

    # Run the query
    stdout = sys.stdout  # Keep a handle on the real standard output
    sys.stdout = mystdout = StringIO()  # Choose a file-like object to write to
    try:
        t = db.query(app_onc.vars['query'], fmt='table', use_converters=False)
    except ValueError:
        t = db.query(app_onc.vars['query'], fmt='array', use_converters=False)
    except:
        return render_template('error.html', headermessage='Error in Query',
                               errmess='<p>Error in query:</p><p>' + htmltxt + '</p>')
    sys.stdout = stdout

    # Check for any errors from mystdout
    if mystdout.getvalue().lower().startswith('could not execute'):
        return render_template('error.html', headermessage='Error in Query',
                               errmess='<p>Error in query:</p><p>' + mystdout.getvalue().replace('<', '&lt;') + '</p>')

    # Check how many results were found
    if type(t) == type(None):
        return render_template('error.html', headermessage='No Results Found',
                               errmess='<p>No entries found for query:</p><p>' + htmltxt +
                                       '</p><p>' + mystdout.getvalue().replace('<', '&lt;') + '</p>')

    # Remane RA and Dec columns
    for idx, name in enumerate(t.colnames):
        if name.endswith('.ra'):
            t[name].name = 'ra'
        if name.endswith('.dec'):
            t[name].name = 'dec'
        if name.endswith('.id'):
            t[name].name = 'id'

    # Convert to Pandas data frame
    try:
        data = t.to_pandas()
    except AttributeError:
        return render_template('error.html', headermessage='Error in Query',
                               errmess='<p>Error for query:</p><p>' + htmltxt + '</p>')

    # Create checkbox first column
    data = add_checkboxes(data)

    try:
        script, div, warning_message = onc_skyplot(t)
    except:
        script = div = warning_message = ''

    # Add links to columns
    data = link_columns(data, db, ['id', 'source_id', 'spectrum', 'image'])

    # Get numerical x and y axes for plotting
    columns = [c for c in t.colnames if isinstance(t[c][0], (int, float))]
    axes = '\n'.join(['<option value="{}"> {}</option>'.format(repr(b) + "," + repr(list(t[b])), b) for b in columns])
    
    # Data for export
    export = [strip_html(str(i)) for i in list(data)[1:]]
    export = """<input class='hidden' type='checkbox', name='cols' value="{}" checked=True />""".format(export)

    # Generate HTML
    table_html = data.to_html(classes='display', index=False).replace('&lt;', '<').replace('&gt;', '>')

    return render_template('results.html', table=table_html, query=app_onc.vars['query'],
                           script=script, plot=div, warning=warning_message, axes=axes, export=export)

# Grab results of query and display them
@app_onc.route('/plot', methods=['POST','GET'])
def onc_plot():

    # Get the axes to plot
    xaxis, xdata = eval(request.form['xaxis'])
    yaxis, ydata = eval(request.form['yaxis'])

    # Make the plot
    tools = "resize,crosshair,pan,wheel_zoom,box_zoom,reset"
    p = figure(tools=tools, x_axis_label=xaxis, y_axis_label=yaxis, plot_width=800)
    p.circle(xdata, ydata)

    title = '{} v. {}'.format(xaxis,yaxis)

    script, div = components(p)

    # Also make a table
    table = pd.DataFrame(np.array([xdata,ydata]).T, columns=[xaxis,yaxis])
    table = table.to_html(classes='display', index=False).replace('&lt;','<').replace('&gt;','>')

    return render_template('plot.html', title=title, script=script, plot=div, table=table)

# Grab selected inventory and plot SED
@app_onc.route('/sed', methods=['POST'])
@app_onc.route('/inventory/sed', methods=['POST'])
def onc_sed():

    # Get the ids of all the data to use
    entries = request.form
    age = (float(entries['age_min'])*q.Myr, float(entries['age_max'])*q.Myr)
    radius = (float(entries['radius'])*ac.R_sun,float(entries['radius_unc'])*ac.R_sun)
    source_id = int(entries['sources'])
    spt_id = int(entries.get('spectral_types', 0))
    plx_id = int(entries.get('parallaxes', 0))
    
    # Collect all spec_ids and phot_ids
    phot_ids, spec_ids = [], []
    for key in entries.keys():
        for value in entries.getlist(key):
            if key=='photometry':
                phot_ids.append(int(value))
            elif key=='spectra':
                spec_ids.append(int(value))
                
    # Make the astropy tables
    sed_dict = {}
    sed_dict['sources'] = source_id
    if plx_id:
        sed_dict['spectral_types'] = spt_id
    if plx_id:
        sed_dict['parallaxes'] = plx_id
    if spec_ids:
        sed_dict['spectra'] = spec_ids
    if phot_ids:
        sed_dict['photometry'] = phot_ids
    
    # Include ONC distance as default if no parallax
    dist, warning = '', ''
    if 'parallaxes' not in sed_dict:
        dist = (388*q.pc,20*q.pc)
        warning = "No distance given for this source. Using \(388\pm 20 pc\) from Kounkel et al. (2016)"
    
    # Make the SED
    try:
        # stdout = sys.stdout
        # sys.stdout = mystdout = StringIO()
        SED = sed.MakeSED(source_id, db, from_dict=sed_dict, dist=dist, age=age, radius=radius)
        # sys.stdout = stdout
        # results = mystdout.getvalue()
    except:
        return render_template('error.html', headermessage='SED Error', errmess='<p>At least one spectrum or photometric point is required to construct an SED.</p>')
    
    # Get photometric and spectroscopic data
    phot = SED.photometry
    spec = SED.app_spec_SED
    full_sed = SED.app_SED
    
    # Put the fundamental parameters into a table for display
    # TODO
    
    # Get the axes to plot
    xaxis = 'Wavelength [{}]'.format(SED.wave_units)
    yaxis = 'Flux Density [{}]'.format(SED.flux_units)
    
    # Make the plot
    tools = "resize,pan,wheel_zoom,box_zoom,reset"
    p = figure(tools=tools, x_axis_label=xaxis, y_axis_label=yaxis, plot_width=1000, plot_height=500, \
                y_axis_type="log", x_axis_type="log")
    p.xgrid.grid_line_color = None
    p.ygrid.grid_line_color = None
    
    # PLot photometry
    if phot!='':
        
        # Plot points with errors
        pts = np.array([(x,y,z) for x,y,z in np.array(phot['eff','app_flux','app_flux_unc']) if not np.isnan(z)]).T
        # pts = np.array([(x,y,z) for x,y,z in zip(*phot) if not np.isnan(z)]).T
        try:
            p.circle(pts[0], pts[1], size=8, legend='Photometry')
            errs = error_bars(*pts)
            p.multi_line(*errs)
        except:
            pass
        
        # Plot saturated photometry
        err = np.array([(x,y,z) for x,y,z in np.array(phot['eff','app_flux','app_flux_unc']) if np.isnan(z)]).T
        try:
            p.circle_x(err[0], err[1], size=8, fill_color='white', legend='Saturated')
        except:
            pass
            
        # Plot upper limits
        err = np.array([(x,y,z) for x,y,z in np.array(phot['eff','app_flux','app_flux_unc']) if np.isnan(z)]).T
        try:
            p.circle(err[0], err[1], size=8, fill_color='white', legend='Nondetection')
        except:
            pass
        
    # Plot spectra
    if spec!='':
        source = ColumnDataSource(data=dict(x=spec[0], y=spec[1], z=spec[2]))
        hover = HoverTool(tooltips=[( 'wavelength', '$x'),( 'flux', '$y'),('unc','$z')], mode='vline')
        p.add_tools(hover)
        p.line('x', 'y', source=source, legend='Spectra')
        
    # Plot curve of integration
    if full_sed!='':
        sed_w, sed_f, sed_e = [full_sed[i][(full_sed[0]>0.4)&(full_sed[0]<15)] for i in [0,1,2]]
        
        p.line(sed_w, sed_f, line_color='grey', line_dash='dashed', line_alpha=0.5, legend='SED')
        
    # Generate the HTML
    script, div = components(p)
    
    # Get params to print
    fbol = '\({:.3e} \pm {:.3e}\)'.format(SED.fbol.value,SED.fbol_unc.value)
    mbol = '\({} \pm {}\)'.format(SED.mbol,SED.mbol_unc)
    teff = '\({} \pm {}\)'.format(int(SED.Teff.value),SED.Teff_unc.value if np.isnan(SED.Teff_unc.value) else int(SED.Teff_unc.value)) if SED.distance else '-'
    Lbol = '\({:.3f} \pm {:.3f}\)'.format(SED.Lbol_sun,SED.Lbol_sun_unc) if SED.distance else '-'
    radius = '\({:.3f} \pm {:.3f}\)'.format(SED.radius.to(ac.R_sun).value,SED.radius_unc.to(ac.R_sun).value) if SED.radius else '-'
    
    results = [[title,tbl2html(tab, roles='grid', classes='dataframe display no_pagination dataTable no-footer')] for tab,title in zip([SED.sources,SED.spectral_types,SED.parallaxes,SED.photometry,SED.spectra],['sources','spectral_types','parallaxes','photometry','spectra']) if len(tab)>0]
    
    return render_template('sed.html', script=script, plot=div, spt=SED.SpT or '-', mbol=mbol, fbol=fbol, 
                            teff=teff, Lbol=Lbol, radius=radius, title=SED.name, warning=warning, results=results)

def error_bars(xs, ys, zs):
    """
    Generate errorbars for the photometry since Bokeh doesn't do it
    """
    # Create the coordinates for the errorbars
    err_xs, err_ys = [], []
    for x, y, yerr in zip(xs, ys, zs):
        if not np.isnan(yerr):
            err_xs.append((x, x))
            err_ys.append((y-yerr, y+yerr))
            
    return (err_xs, err_ys)

def link_columns(data, db, columns):
    
    # Change id to a link
    if 'id' in columns and 'id' in data and 'source_id' not in data:
        linklist = []
        for i, elem in enumerate(data['id']):
            link = '<a href="inventory/{0}">{1}</a>'.format(data.iloc[i]['id'], elem)
            linklist.append(link)
        data['id'] = linklist
    
    # Change source_id column to a link
    if 'source_id' in columns and 'source_id' in data:
        linklist = []
        for i, elem in enumerate(data['source_id']):
            link = '<a href="inventory/{}">{}</a>'.format(data.iloc[i]['source_id'], elem)
            linklist.append(link)
        data['source_id'] = linklist
        
    # Change spectrum column to a link
    if 'spectrum' in columns and 'spectrum' in data:
        speclist = []
        for index, row in data.iterrows():
            spec = '<a href="../spectrum/{}"><img class="view" src="static/images/view.png" /></a>'.format(row['id'])
            speclist.append(spec)
        data['spectrum'] = speclist
        
    # Change image column to a link
    if 'image' in columns and 'image' in data:
        imglist = []
        for index, row in data.iterrows():
            img = '<a href="../image/{}"><img class="view" src="static/images/view.png" /></a>'.format(row['id'])
            imglist.append(img)
        data['image'] = imglist
    
    return data

@app_onc.route('/export', methods=['POST'])
def onc_export():
    
    # Get all the checked rows
    checked = request.form
    
    # Get column names
    print(checked.get('cols'))
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

def add_checkboxes(data, type='checkbox', id_only=False, table_name='', all_checked=False):
    """
    Create checkbox first column in Pandas dataframe
    """
    buttonlist = []
    for index, row in data.iterrows():
        val = strip_html(repr(list(row)))
        if id_only:
            val = val.split(',')[0].replace('[','')
        tab = table_name or str(index)
        button = '<input type="{}" name="{}" value="{}"{}>'.format(type,tab,val,' checked' if (index==0 and type=='radio') or (all_checked and type=='checkbox') else ' checked')
        buttonlist.append(button)
    data['Select'] = buttonlist
    cols = data.columns.tolist()
    cols.pop(cols.index('Select'))
    data = data[['Select']+cols]

    return data

# Perform a search
@app_onc.route('/search', methods=['POST'])
def onc_search():
    # db = astrodb.Database(db_file)
    app_onc.vars['search'] = request.form['search_to_run']
    search_table = request.form['table']
    search_value = app_onc.vars['search']
    search_radius = 1/60.

    # Process search
    search_value = search_value.replace(',', ' ').split()
    if len(search_value) == 1:
        search_value = search_value[0]
    else:
        try:
            search_value = [float(s) for s in search_value]
            search_radius = float(request.form['radius'])/60.
        except:
            return render_template('error.html', headermessage='Error in Search',
                                   errmess='<p>Could not process search input:</p>' +
                                           '<p>' + app_onc.vars['search'] + '</p>')

    # Run the search
    stdout = sys.stdout  # Keep a handle on the real standard output
    sys.stdout = mystdout = StringIO()  # Choose a file-like object to write to
    
    # Get table of results
    t = db.search(search_value, search_table, radius=search_radius, fetch=True)
    sys.stdout = stdout

    try:
        data = t.to_pandas()
    except AttributeError:
        return render_template('error.html', headermessage='Error in Search',
                               errmess=mystdout.getvalue().replace('<', '&lt;'))
                               
    try:
        sources = data[['ra','dec','id']].values.tolist()
        sources = [[i[0], i[1], 'Source {}'.format(int(i[2])), int(i[2])] for i in sources]
    except:
        try:
            sources = data[['ra','dec','source_id']].values.tolist()
            sources = [[i[0], i[1], 'Source {}'.format(int(i[2])), int(i[2])] for i in sources]
        except:
            sources = ''
                               
    if not data.empty:
        # Create checkbox first column
        data = add_checkboxes(data)
        
        # Toggle columns
        cols = 'Toggle Column: '+' - '.join(['<a class="toggle-vis" />{}</a>'.format(name) for i,name in enumerate(t.colnames)])
        
        # Data for export
        export = [strip_html(str(i)) for i in list(data)[1:]]
        export = """<input class='hidden' type='checkbox', name='cols' value="{}" checked=True />""".format(export)

        # Add links to columns
        data = link_columns(data, db, ['id', 'source_id', 'image','spectrum'])

        # Get numerical x and y axes for plotting
        columns = [c for c in t.colnames if isinstance(t[c][0], (int, float))]
        axes = '\n'.join(['<option value="{}"> {}</option>'.format(repr(b)+","+repr(list(t[b])), b) for b in columns])

        return render_template('results.html', table=data.to_html(classes='display', index=False).replace('&lt;','<').replace('&gt;','>'), query=search_value,
                                sources=sources, cols=cols, axes=axes, export=export)
    
    else:
        return render_template('error.html', headermessage='Error in Search',
                               errmess='<p>This input returns no results:</p>' +
                                       '<p>' + app_onc.vars['search'] + '</p>')

# Plot a spectrum
@app_onc.route('/spectrum', methods=['POST'])
@app_onc.route('/spectrum/<int:specid>')
def onc_spectrum(specid=None):
    # db = astrodb.Database(db_file)
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
    query = 'SELECT * FROM spectra WHERE id={}'.format(app_onc.vars['specid'])
    t = db.query(query, fmt='table')
    sys.stdout = stdout

    # Check for errors first
    if mystdout.getvalue().lower().startswith('could not execute'):
        return render_template('error.html', headermessage='Error in Query',
                               errmess='<p>Error in query:</p><p>'+mystdout.getvalue().replace('<', '&lt;')+'</p>')

    # Check if found anything
    if isinstance(t, type(None)):
        return render_template('error.html', headermessage='No Result', errmess='<p>No spectrum found.</p>')

    # Get data
    wav = 'Wavelength ('+t[0]['wavelength_units']+')'
    flux = 'Flux ('+t[0]['flux_units']+')'
    spec = t[0]['spectrum']
    filepath = spec.path

    # Make the plot
    tools = "resize,pan,wheel_zoom,box_zoom,reset"
    p = figure(tools=tools, x_axis_label=wav, y_axis_label=flux, plot_width=800)
    source = ColumnDataSource(data=dict(x=spec.data[0], y=spec.data[1]))
    hover = HoverTool(tooltips=[( 'wavelength', '$x'),( 'flux', '$y')], mode='vline')
    p.add_tools(hover)
    p.line('x', 'y', source=source)

    script, div = components(p)

    t['spectrum'] = [sp.path for sp in t['spectrum']]
    meta = t.to_pandas().to_html(classes='display', index=False)

    return render_template('spectrum.html', script=script, plot=div, meta=meta, download=filepath)

# Display an image
@app_onc.route('/image', methods=['POST'])
@app_onc.route('/image/<int:imgid>')
def onc_image(imgid=None):
    # db = astrodb.Database(db_file)
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
    query = 'SELECT * FROM images WHERE id={}'.format(app_onc.vars['imgid'])
    t = db.query(query, fmt='table')
    sys.stdout = stdout
    
    # Check for errors first
    if mystdout.getvalue().lower().startswith('could not execute'):
        return render_template('error.html', headermessage='Error in Query',
                               errmess='<p>Error in query:</p><p>'+mystdout.getvalue().replace('<', '&lt;')+'</p>')
                               
    # Check if found anything
    if isinstance(t, type(None)):
        return render_template('error.html', headermessage='No Result', errmess='<p>No image found.</p>')
        
    try:
        img = t[0]['image'].data
        
        # Down sample so the figure displays faster
        img = zoom(img, 0.05, prefilter=False)
        
        filepath = t[0]['image'].path
        
        # Make the plot
        tools = "resize,crosshair,pan,wheel_zoom,box_zoom,reset"
        
        # create a new plot
        p = figure(tools=tools, plot_width=800)
        
        # Make the plot
        p.image(image=[img], x=[0], y=[0], dw=[img.shape[0]], dh=[img.shape[1]])
        
        p.x_range = Range1d(0, img.shape[0])
        p.y_range = Range1d(0, img.shape[1])
        
        script, div = components(p)

        t['image'] = [sp.path for sp in t['image']]
        meta = t.to_pandas().to_html(classes='display', index=False)
        
    except IOError:
        script, div, filepath = '', '', ''
    
    return render_template('image.html', script=script, plot=div, meta=meta, download=filepath)

# Check inventory
@app_onc.route('/inventory', methods=['POST'])
@app_onc.route('/inventory/<int:source_id>')
def onc_inventory(source_id=None):
    # db = astrodb.Database(db_file)
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
    t = {name:t[name][[col for col in t[name].colnames if col!='source_id']] for name in t.keys()}
    
    # Check for errors (no results)
    if mystdout.getvalue().lower().startswith('no source'):
        return render_template('error.html', headermessage='No Results Found',
                               errmess='<p>'+mystdout.getvalue().replace('<', '&lt;')+'</p>')
                               
    # Empty because of invalid input
    if len(t) == 0:
        return render_template('error.html', headermessage='Error',
                               errmess="<p>You typed: "+app_onc.vars['source_id']+"</p>")
    
    # Grab object information
    objname = t['sources']['designation'][0] or 'Source {}'.format(app_onc.vars['source_id'])
    ra = t['sources']['ra'][0]
    dec = t['sources']['dec'][0]
    c = SkyCoord(ra=ra*q.degree, dec=dec*q.degree)
    coords = c.to_string('hmsdms', sep=':', precision=2)
    allnames = t['sources']['names'][0]
    
    # Grab distance
    try:
        distance = 1000./t['parallaxes']['parallax']
        dist_string = ', '.join(['{0:.2f}'.format(i) for i in distance])
        dist_string += ' pc'
    except:
        dist_string = 'N/A'
        
    # Grab spectral type
    try:
        sptype_txt = []
        for row in t['spectral_types'][['spectral_type','spectral_type_unc','suffix','gravity','luminosity_class']]:
            spt = u.specType(list(row))
            sptype_txt.append(spt.replace('None',''))
            
        sptype_txt = ' / '.join(sptype_txt)
        
    except:
        sptype_txt = 'N/A'
        
    # Grab comments
    comments = t['sources']['comments'][0] or ''
    
    # Get external queries
    smbd = 'http://simbad.u-strasbg.fr/simbad/sim-coo?Coord={}+%2B{}&CooFrame=ICRS&CooEpoch=2000&CooEqui=2000&CooDefinedFrames=none&Radius=10&Radius.unit=arcsec&submit=submit+query'.format(ra,dec)
    vzr = 'http://vizier.u-strasbg.fr/viz-bin/VizieR?-source=&-out.add=_r&-out.add=_RAJ%2C_DEJ&-sort=_r&-to=&-out.max=20&-meta.ucd=2&-meta.foot=1&-c.rs=20&-c={}+{}'.format(ra,dec)
    
    # Create link to spectra
    if 'spectra' in t:
        speclist = []
        for idx,row in enumerate(t['spectra']):
            spec = '<a href="../spectrum/{}"><img class="view" src="../static/images/view.png" /></a>'.format(row['id'])
            speclist.append(spec)
        t['spectra']['spectrum'] = speclist
        
    # Create link to images
    if 'images' in t:
        imglist = []
        for idx,row in enumerate(t['images']):
            img = '<a href="../image/{}"><img class="view" src="../static/images/view.png" /></a>'.format(row['id'])
            imglist.append(img)
        t['images']['image'] = imglist
        
    # Add order to names for consistent printing
    ordered_names = ['sources','spectral_types','parallaxes','photometry','spectra','images']
            
    # Make the HTML
    html_tables = []
    for name in ordered_names:
        
        try:
        
            # Convert to pandas
            table = t[name].to_pandas()
        
            # Add checkboxes for SED creation
            type = 'radio' if name in ['sources','spectral_types','parallaxes'] else 'checkbox'
            table = add_checkboxes(table, type=type, id_only=True, table_name=name)
        
            # Convert to HTML
            table = table.to_html(classes='display no_pagination no_wrap', index=False).replace('&lt;', '<').replace('&gt;', '>')
            
        except KeyError:
            
            table = 'No records in the <code>{}</code> table for this source.'.format(name)
        
        html_tables.append(table)
    
    if 'photometry' in t:
        phots = [[p['ra'],p['dec'],p['band'],'{}, {}'.format(p['ra'],p['dec']), '{} ({})'.format(p['magnitude'],p['magnitude_unc'])] for p in t['photometry']]
    else:
        phots = []
    
    delta_ra = delta_dec = 0.025
    sources = db.query("SELECT id,ra,dec,names FROM sources WHERE (ra BETWEEN {1}-{0} AND {1}+{0}) AND (dec BETWEEN {3}-{2} AND {3}+{2}) AND (ra<>{1} AND dec<>{3})".format(delta_ra, ra, delta_dec, dec), fmt='array')

    warning = ''
    if any(['d{}'.format(i) in comments for i in range(20)]):
        warning = "Warning: This source is confused with its neighbors and the data listed below may not be trustworthy."
        
    return render_template('inventory.html', tables=html_tables, warning=warning, phots=phots, sources=sources,
                           titles=['sources']+ordered_names, path=path, source_id=app_onc.vars['source_id'],
                           name=objname, coords=coords, allnames=allnames, distance=dist_string,
                           comments=comments, sptypes=sptype_txt, ra=ra, dec=dec, simbad=smbd, vizier=vzr)

# Check Schema
# @app_onc.route('/schema.html', methods=['GET', 'POST'])
@app_onc.route('/schema', methods=['GET', 'POST'])
def onc_schema():
    # db = astrodb.Database(db_file)

    # Get table names and their structure
    try:
        table_names = db.query("SELECT name FROM sqlite_sequence", unpack=True)[0]
    except:
        table_names = db.query("SELECT * FROM sqlite_master WHERE type='table'")['tbl_name']

    table_dict = {}
    for name in table_names:
        temptab = db.query('PRAGMA table_info('+name+')', fmt='table')
        table_dict[name] = temptab

    table_html = [[db.query("select count(id) from {}".format(x))[0][0], table_dict[x].to_pandas().to_html(classes='display', index=False)] for x in sorted(table_dict.keys())]
    titles = ['na']+sorted(table_dict.keys())
    
    return render_template('schema.html', tables=table_html, titles=titles)

@app_onc.route('/browse')
def onc_browse():
    """Examine the full source list with clickable links to object summaries"""
    # db = astrodb.Database(db_file)

    # Run the query
    query = 'SELECT * FROM browse WHERE id IN (SELECT id FROM browse ORDER BY RANDOM() LIMIT 100)'
    t = db.query(query, fmt='table')
    
    try:
        script, div, warning_message = onc_skyplot(t)
    except IOError:
        script = div = warning_message = ''

    # Convert to Pandas data frame
    data = t.to_pandas()
    data.index = data['id']
    
    try:
        sources = data[['ra','dec','id']].values.tolist()
        sources = [[i[0], i[1], 'Source {}'.format(int(i[2])), int(i[2])] for i in sources]
    except:
        sources = ''

    # Change column to a link
    linklist = []
    for i, elem in enumerate(data['id']):
        link = '<a href="inventory/{0}">{1}</a>'.format(data.iloc[i]['id'], elem)
        linklist.append(link)
    data['id'] = linklist
    
    # Create checkbox first column
    data = add_checkboxes(data)
    
    cols = [strip_html(str(i)) for i in data.columns.tolist()[1:]]
    cols = """<input class='hidden' type='checkbox', name='cols' value="{}" checked=True />""".format(cols)

    # Get numerical x and y axes for plotting
    columns = [c for c in t.colnames if isinstance(t[c][0], (int, float))]
    axes = '\n'.join(['<option value="{}"> {}</option>'.format(repr(b)+","+repr(list(t[b])), b) for b in columns])

    return render_template('results.html', table=data.to_html(classes='display', index=False).replace('&lt;','<').replace('&gt;','>'), query=query,
                            sources=sources, cols=cols, axes=axes)

def strip_html(s):
    return re.sub(r'<[^<]*?/?>','',s)

def tbl2html(table, classes='', ids='', roles=''):
    """
    Sloppily converts an astropy table to html (when mixin columns won't let you do table.)
    """
    # Get header
    columns = ''.join(['<th>{}</th>'.format(col) for col in table.colnames])
    
    # Build table and header
    out = "<table class='{}' id='{}' role='{}'><thead>{}</thead><tbody>".format(classes,ids,roles,columns)
    
    # Add rows
    for row in np.array(table):
        out += '<tr><td>'+'</td><td>'.join(list(map(str,row)))+'</td></tr>'
    
    out += "</tbody></table>"
    
    return out

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

## -- RUN
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app_onc.run(host='0.0.0.0', port=port, debug=True)
    
    
    
    