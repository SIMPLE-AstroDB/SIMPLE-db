# SIMPLE

The vision of the SIMPLE project is to create a *collaborative* database of low-mass stars, brown dwarfs, and directly 
imaged exoplanets: a simple archive of complex objects. The tables and fields currently included in the 
database are described in the [Documentation](documentation/README.md) 
and currently include names, coordinates, photometry and reference and data provenance information and is visualized 
in the [schema](#simple-database-schema) below. 
We are currently working on including kinematics, spectra, images, and modeled and retrieved parameters. 

We are developing several different methods to interact with the database, including python, a website and API, and 
database browsers.

While we are using brown dwarfs to build out the SIMPLE database, our intention is to build a database schema and 
software which could be used by other subfields to roll their own collaborative databases and web interfaces.

This database uses the [SQLAlchemy ORM](https://docs.sqlalchemy.org/en/14/orm/index.html) and is designed to be
interacted with via the `astrodbkit2` package.

If you'd like to participate or just stay in the loop as this project progresses, please request to join this discussion
 list:
https://groups.google.com/forum/#!forum/simple-archive

To see more details about what we've discussed so far, check out the running notes on the project: 
https://docs.google.com/document/d/1zDayF4ERMjj22QI3RaUZeTeb6nIVl3c2KDNw5WLLUqE/edit

## Getting Started

If you'd like to set up your own copy of the SIMPLE database, here's what we recommend:

1. Clone or download a copy of this repo locally onto your computer.

2. Set up an environment for the python code. 
A conda environment file `environment.yml` exists for convenience. The following commands will use that file to create and activate an 
   environment called `simple-db`:

    ```bash
    conda env create -f environment.yml
    conda activate simple-db
    ```
    
3. Install the latest version of the AstrodbKit2 package:
    
    ```bash
     pip install git+https://github.com/dr-rodriguez/AstrodbKit2
     ```

3. Connect to the database file `SIMPLE.db` as a Database object called `db`

    ```python
    from astrodbkit2.astrodb import Database
    
    connection_string = 'sqlite:///SIMPLE.db'  # Assumes SQLite connection to database in local folder
    db = Database(connection_string)
    ```

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

    
## SIMPLE Database Schema

The schema for the SIMPLE database is described
in the [Documentation](documentation) and can be found in `simple/schema.py`.

A graphical representation of the SIMPLE schema:
![schema](documentation/figures/schema.png)

Ella was here learning how to use GitHub!
