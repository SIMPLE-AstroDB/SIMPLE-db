from flask import Flask,render_template,request,redirect
from astrodbkit import astrodb
import pandas as pd


app_bdnyc = Flask(__name__)

# Go to
# http://127.0.0.1:5000/query
# To access the query form

app_bdnyc.vars={}

@app_bdnyc.route('/')
def bdnyc_home():
    return redirect('/query')

@app_bdnyc.route('/index')
def bdnyc_index():
    return redirect('/query')

# Page with a text box to take the SQL query
@app_bdnyc.route('/query', methods=['GET','POST'])
def bdnyc_query():
    defquery = 'SELECT * FROM sources LIMIT 25'

    if request.method == 'GET':
        return render_template('query.html', defquery=defquery)
    else:
        return 'This was a POST request'

@app_bdnyc.route('/runquery', methods=['POST'])
def bdnyc_runquery():
    app_bdnyc.vars['query'] = request.form['query_to_run']

    # Load the database
    db = astrodb.get_db('./BDNYCv1.0.db')

    # Run the query
    try:
        t = db.query(app_bdnyc.vars['query'], fmt='table')
    except ValueError:
        t = db.query(app_bdnyc.vars['query'], fmt='array')
        htmltxt = app_bdnyc.vars['query'].replace('<','&lt;')
        if len(t)==0: # no entries found
            return render_template('view.html', table='<p>No entries found for query:<br>'+htmltxt+'</p>')
        else:
            return render_template('view.html', table='<p>Error for query:<br>'+htmltxt+'</p>')

    # Convert to Pandas data frame
    try:
        data = pd.DataFrame()
        for col in t.keys():
            data.loc[:,col] = pd.Series(t[col])
    except AttributeError:
        htmltxt = app_bdnyc.vars['query'].replace('<','&lt;')
        return render_template('view.html', table='<p>Error for query:<br>'+htmltxt+'</p>')

    return render_template('view.html', table=data.to_html(classes='display', index=False))


if __name__ == '__main__':
    app_bdnyc.run(debug=True)