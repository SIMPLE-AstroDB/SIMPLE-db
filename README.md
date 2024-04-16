# SIMPLE

The vision of the SIMPLE project is to create a *collaborative* database of low-mass stars, brown dwarfs, and directly 
imaged exoplanets: a simple archive of complex objects. The tables and fields currently included in the 
database are described in the [Documentation](documentation/README.md) 
and currently include names, coordinates, photometry and reference and data provenance information and is visualized 
in the [schema](#simple-database-schema) below. 
The archive includes kinematics, spectra, modeled and retrieved parameters. 

There are several different methods to interact with the database, including: 
- Python
- a website and API at https://simple-bd-archive.org/
- database browsers such as [DB Browser for SQLite](https://sqlitebrowser.org/)

To report missing data, data bugs or make feature requests, please use the [SIMPLE-db issue tracker](https://github.com/SIMPLE-AstroDB/SIMPLE-db/issues).

For day-to-day discussions, please join us in the #simple-db-dev channel in the [Astropy Slack workspace](https://astropy.slack.com/).
If you are not already in the Astropy Slack, [request an account](http://joinslack.astropy.org/).

To see more details about how this project got started and our initial discussions, check out the [archived running notes in the Wiki](https://github.com/SIMPLE-AstroDB/SIMPLE-db/wiki/Original-Notes).

## Getting Started

If you'd like to set up your own copy of the SIMPLE database, here's what we recommend:

1. Clone or download a copy of this repo locally onto your computer.
 
2. Set up an environment for the Python code and install dependencies. 
A conda environment file `environment.yml` exists for convenience. The following commands will use that file to create and activate an 
   environment called `simple-db`:

    ```bash
    conda env create -f environment.yml
    conda activate simple-db
    ```
   
3. In Python, connect a database file `SIMPLE.sqlite` as a Database object called `db` and recreate the database using the JSON files in the 'data/' directory.
      
   ```python
   from astrodb_utils import load_astrodb
   from simple.schema import *
   
   db = load_astrodb("SIMPLE.sqlite", recreatedb=True)
    ```

    This step generates a "SIMPLE.sqlite" file which can be opened, explored, and modified using the [DB Browser for SQLite](https://sqlitebrowser.org/).

4. Use `astrodbkit2` to [explore](https://astrodbkit2.readthedocs.io/en/latest/#exploring-the-schema), [query](https://astrodbkit2.readthedocs.io/en/latest/#querying-the-database), and/or [modify](https://astrodbkit2.readthedocs.io/en/latest/#modifying-data) the database.
For example:
    - Find all objects in the database with "0141" in the name
        ```
        db.search_object('0141', fmt='astropy')
        ```
    
    - See all the data in the database for 2MASS J01415823-4633574

        ```
        db.inventory('2MASS J01415823-4633574', pretty_print=True)
        ```
5. The database can also be modified using helper scripts found in `simple/utils` and in the `astrodb_utils` package. Previously used scripts to modify and/or update the database are stored in the `scripts/` directory.

## Contributor Instructions
If you've made changes to the SIMPLE Archive that you would like to contribute to the public verion, here's how to make a contribution.

1. If you've made changes using Python or the DB Browser, write out the new/modified JSON files:
    ```python
    db.save_database(directory="data/")
    ```

2. (Optional) Run and modify tests as necessary


3. Open a pull request with the modified JSON files and optionally, your ingest script.


## SIMPLE Database Schema

The schema for the SIMPLE database is described
in the [Documentation](documentation) and can be found in `simple/schema.py`.

A graphical representation of the SIMPLE schema:
<img src="https://github.com/SIMPLE-AstroDB/SIMPLE-db/blob/main/documentation/figures/schema2023.png?raw=true" width=75%>
