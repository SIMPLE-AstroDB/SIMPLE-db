from flask import Flask,render_template,request,redirect,make_response
from astrodbkit import astrodb
import os, sys
from cStringIO import StringIO
from bokeh.plotting import figure, output_file, show
from bokeh.embed import components
#import pandas as pd

app_bdnyc = Flask(__name__)

# Go to
# http://127.0.0.1:5000/query
# To access the query form

app_bdnyc.vars={}
app_bdnyc.vars['query'] = ''
app_bdnyc.vars['search'] = ''
app_bdnyc.vars['specid'] = ''

@app_bdnyc.route('/')
def bdnyc_home():
    return redirect('/query')

@app_bdnyc.route('/index')
def bdnyc_index():
    return redirect('/query')

# Page with a text box to take the SQL query
@app_bdnyc.route('/query', methods=['GET','POST'])
def bdnyc_query():
    defquery = 'SELECT * FROM sources'
    if app_bdnyc.vars['query']=='':
        app_bdnyc.vars['query'] = defquery

    if request.method == 'GET':
        return render_template('query.html', defquery=app_bdnyc.vars['query'],
                               defsearch=app_bdnyc.vars['search'])
    else:
        return 'This was a POST request'

# Grab results of query and display them
@app_bdnyc.route('/runquery', methods=['POST'])
def bdnyc_runquery():
    app_bdnyc.vars['query'] = request.form['query_to_run']
    htmltxt = app_bdnyc.vars['query'].replace('<','&lt;')

    # Load the database
    db = astrodb.get_db('./database.db')

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

    # Check how many results were found
    try:
        len(t)
    except TypeError:
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

    db = astrodb.get_db('./database.db')
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
    search_table = 'sources'#request.form['table_to_search']
    search_value = app_bdnyc.vars['search']

    # Load the database
    db = astrodb.get_db('./database.db')

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

    print app_bdnyc.vars['search']

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
    db = astrodb.get_db('./database.db')

    # Grab the spectrum
    query = 'SELECT spectrum FROM spectra WHERE id='+app_bdnyc.vars['specid']
    t = db.query(query, fetch='one', fmt='dict')
    spec = t['spectrum']

    # Make the plot
    TOOLS="resize,crosshair,pan,wheel_zoom,box_zoom,reset"

    # create a new plot
    p = figure(
        tools=TOOLS, title="object name",
        x_axis_label='Wavelength', y_axis_label='Flux'
    )

    # add some renderers
    p.line(spec.data[0], spec.data[1], legend="data")

    script, div = components(p)

    return render_template('spectrum.html', script=script, plot=div)

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app_bdnyc.run(host='0.0.0.0', port=port, debug=True)
    #app_bdnyc.run(debug=False)