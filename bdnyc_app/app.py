from flask import Flask,render_template,request,redirect,make_response
from astrodbkit import astrodb
import os, sys
from cStringIO import StringIO
from bokeh.plotting import figure
from bokeh.embed import components

app_bdnyc = Flask(__name__)

app_bdnyc.vars={}
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
@app_bdnyc.route('/query', methods=['GET','POST'])
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
    htmltxt = app_bdnyc.vars['query'].replace('<','&lt;')

    # Load the database
    db = astrodb.Database('./database.db')

    # Only SELECT commands are allowed
    if not app_bdnyc.vars['query'].lower().startswith('select'):
        return render_template('error.html', headermessage='Error in Query',
                               errmess='<p>Only SELECT queries are allowed. You typed:</p><p>'+htmltxt+'</p>')

    # Run the query
    stdout = sys.stdout  #keep a handle on the real standard output
    sys.stdout = mystdout = StringIO() #Choose a file-like object to write to
    try:
        t = db.query(app_bdnyc.vars['query'], fmt='table')
    except ValueError:
        t = db.query(app_bdnyc.vars['query'], fmt='array')
    sys.stdout = stdout

    # Check for any errors from mystdout
    if mystdout.getvalue().lower().startswith('could not execute'):
        return render_template('error.html', headermessage='Error in Query',
                               errmess='<p>Error in query:</p><p>'+mystdout.getvalue().replace('<','&lt;')+'</p>')

    # Check how many results were found
    if type(t)==type(None):
        return render_template('error.html', headermessage='No Results Found',
                               errmess='<p>No entries found for query:</p><p>' + htmltxt + \
                                       '</p><p>'+mystdout.getvalue().replace('<','&lt;')+'</p>')

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
    if export_fmt=='votable':
        filename = 'bdnyc_table.vot'
    else:
        filename = 'bdnyc_table.txt'

    db = astrodb.Database('./database.db')
    db.query(app_bdnyc.vars['query'], fmt='table', export=filename)
    with open(filename, 'r') as f:
        file_as_string = f.read()
    os.remove(filename) # delete the file after it's read

    response = make_response(file_as_string)
    response.headers["Content-Disposition"] = "attachment; filename=%s" % filename
    return response

# Perform a search
@app_bdnyc.route('/search', methods=['POST'])
def bdnyc_search():
    app_bdnyc.vars['search'] = request.form['search_to_run']
    search_table = 'sources' #request.form['table_to_search']
    search_value = app_bdnyc.vars['search']

    # Load the database
    db = astrodb.Database('./database.db')

    # Process search
    search_value = search_value.replace(',',' ').split()
    if len(search_value)==1:
      search_value = search_value[0]
    else:
        try:
            search_value = [float(s) for s in search_value]
        except:
            return render_template('error.html', headermessage='Error in Search',
                                   errmess='<p>Could not process search input:</p>' + \
                                           '<p>' + app_bdnyc.vars['search'] + '</p>')

    # Run the search
    stdout = sys.stdout  #keep a handle on the real standard output
    sys.stdout = mystdout = StringIO() #Choose a file-like object to write to
    t = db.search(search_value, search_table, fetch=True)
    sys.stdout = stdout

    try:
        data = t.to_pandas()
    except AttributeError:
        return render_template('error.html', headermessage='Error in Search',
                               errmess=mystdout.getvalue().replace('<','&lt;'))

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
    stdout = sys.stdout  #keep a handle on the real standard output
    sys.stdout = mystdout = StringIO() #Choose a file-like object to write to
    query = 'SELECT spectrum, flux_units, wavelength_units, source_id, instrument_id, telescope_id FROM spectra WHERE id=' + \
            app_bdnyc.vars['specid']
    t = db.query(query, fetch='one', fmt='dict')
    sys.stdout = stdout

    # Check for errors first
    if mystdout.getvalue().lower().startswith('could not execute'):
        return render_template('error.html', headermessage='Error in Query',
                               errmess='<p>Error in query:</p><p>'+mystdout.getvalue().replace('<','&lt;')+'</p>')

    # Check if found anything
    if type(t)==type(None):
        return render_template('error.html', headermessage='No Result', errmess='<p>No spectrum found.</p>')

    spec = t['spectrum']

    # Get shortname and other information
    # plt.figtext(0.15,0.88,
    #             '{}\n{}\n{}\n{}'.format(i['filename'],
    #             self.query("SELECT name FROM telescopes WHERE id={}".format(i['telescope_id']), fetch='one')[0]
    #             if i['telescope_id'] else '',self.query("SELECT name FROM instruments WHERE id={}".format(i['instrument_id']),
    #                                                     fetch='one')[0] if i['instrument_id'] else '',i['obs_date']),
    #             verticalalignment='top')

    query = 'SELECT shortname FROM sources WHERE id='+str(t['source_id'])
    shortname = db.query(query, fetch='one', fmt='dict')['shortname']

    # Make the plot
    TOOLS="resize,crosshair,pan,wheel_zoom,box_zoom,reset"

    # create a new plot
    wav = 'Wavelength ('+t['wavelength_units']+')'
    flux = 'Flux ('+t['flux_units']+')'
    # can specify plot_width if needed
    p = figure(tools=TOOLS, title=shortname, x_axis_label=wav, y_axis_label=flux, plot_width=800)

    # add some renderers
    p.line(spec.data[0], spec.data[1], line_width=2)

    script, div = components(p)

    return render_template('spectrum.html', script=script, plot=div)

# Check inventory
@app_bdnyc.route('/inventory', methods=['POST'])
def bdnyc_inventory():
    app_bdnyc.vars['source_id'] = request.form['id_to_check']

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
                               errmess='<p>'+mystdout.getvalue().replace('<','&lt;')+'</p>')

    # Empty because of invalid input
    if len(t)==0:
        return render_template('error.html', headermessage='Error',
                               errmess="<p>You typed: "+app_bdnyc.vars['source_id']+"</p>")

    return render_template('inventory.html',
                           tables=[t[x].to_pandas().to_html(classes='display', index=False) for x in t.keys()],
                           titles=['na']+t.keys())

# Check Schema
@app_bdnyc.route('/schema.html', methods=['GET','POST'])
@app_bdnyc.route('/schema', methods=['GET','POST'])
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
                           tables=[table_dict[x].to_pandas().to_html(classes='display', index=False) for x in sorted(table_dict.keys())],
                           titles=['na']+sorted(table_dict.keys()))