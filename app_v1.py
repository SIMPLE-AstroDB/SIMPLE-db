from flask import Flask,render_template,request,redirect
from astrodbkit import astrodb
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
    return 'This is an index page.'

# Page with a text box to take the SQL query
@app_bdnyc.route('/query', methods=['GET','POST'])
def bdnyc_query():
    defquery = 'SELECT * FROM sources'
    if request.method == 'GET':
        return render_template('query.html', defquery=defquery)
    else:
        return 'This was a POST'

@app_bdnyc.route('/runquery', methods=['POST'])
def bdnyc_runquery():
    app_bdnyc.vars['query'] = request.form['query_to_run']

    # Load the database
    db = astrodb.get_db('./BDNYCv1.0.db')
    data = db.query(app_bdnyc.vars['query'], fmt='array')
    # Parse the output

    return app_bdnyc.vars['query']

if __name__ == '__main__':
    app_bdnyc.run(debug=True)