# ONCdbWeb

ONCdbWeb is a Web application which provides data access, analysis, visualization, and downloads from the [Orion Nebula Cluster database (ONCdb)](https://github.com/ONCdb/ONCdb), a curated collection of astrometry, photometry, and spectra for most known ONC sources.

The live Web application can be found at [http://orion.stsci.edu](http://orion.stsci.edu). 

To run the application locally, clone the database and application repos and set the database environment variable with:

```
git clone https://github.com/ONCdb/ONCdbWeb.git
git clone https://github.com/ONCdb/ONCdb.git
export ONC_database="/path/to/ONCdb/orion.db"
```

The dependencies include `SEDkit` and `astrodbkit`. Get them here:
```
git clone https://github.com/hover2pi/SEDkit.git
git clone https://github.com/bdnyc/astrodbkit.git
```

Then run the application with `python ONCdbWeb/onc_app/app_onc.py`. Launch a browser and enter the URL `http://0.0.0.0:5000/`.

For feedback, questions, or if you've found an error, please [create an Issue here](https://github.com/ONCdb/ONCdbWeb/issues).

### The ONCdb Team at STScI
[Joe Filippazzo](https://github.com/hover2pi)
[Massimo Robberto](https://github.com/mrobberto)
Andrea Lin
[Mario Gennaro](https://github.com/mgennaro)
