#!/usr/bin/env python
"""
Generates the whole database as a table on page

Methods
-------
results_unpack
    Unpacks the results query into name, ra, dec
make_table
    Creates the columndatasource and datatable (bokeh objects) using query of sources
main
    Control module, designed to be called directly, will query simple database return the make_table outputs
"""
# 3rd party libs
from bokeh.models import DataTable, TableColumn, ColumnDataSource
import pandas as pd

# local libs
from web_base import load_db


def results_unpack(results: pd.DataFrame):
    """
    Converts the results dataframe into a dictionary of just name, ra, dec

    Parameters
    ----------
    results
        The dataframe of the full returned results from queried string

    Returns
    -------
    data
        The dictionary of the searched for objects just name, ra, dec
    """
    if len(results):
        data = dict(
            sname=results['source'].astype(str).values,  # object name
            ra=results['ra'].astype(float).values,  # object ra
            dec=results['dec'].astype(float).values  # object dec
        )
    else:
        data = dict(sname=[], ra=[], dec=[])
    return data


def make_table(data: dict):
    """
    Creates the table and data source components

    Parameters
    ----------
    data
        The dictionary of objects in the database

    Returns
    -------
    cds
        ColumnDataSource, all the data by which the datatable is generated
    dt
        DataTable, the bokeh object to be displayed on page
    """
    cds = ColumnDataSource(data)  # pack into CDS
    cols = [
        TableColumn(field="sname", title="Name"),  # the column appearing on page for object name
        TableColumn(field="ra", title="RA [deg]"),  # the column appearing on page for object ra
        TableColumn(field="dec", title="Dec [deg]")  # the column appearing on page for object dec
    ]
    dt = DataTable(source=cds, columns=cols,  # linking to actual data source and assigning to columns
                   sizing_mode='stretch_width', selectable=True,  # will fit surrounding div and make rows clickable
                   name="datatable")  # id of bokeh object, required for jinja
    return cds, dt


def querying(query_string: str = ''):
    """
    Queries the database for an object

    Parameters
    ----------
    query_string
        The partial string of an object

    Returns
    -------
    The list of tuples of object info
    """
    db = load_db()  # load the database
    results = db.search_object(query_string, format='pandas')
    return results


def main():
    """
    Main module, queries database and returns the resultant table

    Returns
    -------
    cds
        ColumnDataSource, all the data by which the datatable is generated
    dt
        DataTable, the bokeh object to be displayed on page
    """
    results = querying()
    data = results_unpack(results)
    cds, dt = make_table(data)  # create the bokeh elements
    return cds, dt


if __name__ == '__main__' or 'bokeh' in __name__:
    main()
