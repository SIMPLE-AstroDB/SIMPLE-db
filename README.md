# AstrodbWeb

This web application allows you to query the BDNYC database ([Filippazzo et al. 2015](http://adsabs.harvard.edu/abs/2015ApJ...810..158F)) from your browser. 
It uses the [astrodbkit](https://github.com/BDNYC/astrodbkit) python package to perform queries and returns tables in [JQuery-DataTable](http://datatables.net/) format and 
figures created with [Bokeh](http://bokeh.pydata.org/en/latest/). [Aladin Lite](http://aladin.u-strasbg.fr/AladinLite/doc/) is used to generate finder charts in the Summary pages.  

This code can be downloaded or forked for use with your own SQL database. 
You will want to modify the static and template HTML files to use your styling and 
remove the Google Analytics tracking information from ours. 
Running a local version of this application is easy, just run:   
```
python runapp.py
```
and navigate to the indicated URL.

A live version of this app can be found [here](http://database.bdnyc.org) with a backup available at [Heroku](http://bdnyc-app.herokuapp.com/).

## How to Cite

Please include a citation to the [Zenodo record](http://dx.doi.org/10.5281/zenodo.47866) in publications that utilize this application.

[![DOI](https://zenodo.org/badge/4730/dr-rodriguez/AstrodbWeb.svg)](https://zenodo.org/badge/latestdoi/4730/dr-rodriguez/AstrodbWeb)