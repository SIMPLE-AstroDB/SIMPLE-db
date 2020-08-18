#!/usr/bin/env python
"""
Generates the whole database as a table on page

Methods
-------
make_table
    Creates the columndatasource and datatable (bokeh objects) using query of sources
main
    Control module, designed to be called directly, will query simple database return the make_table outputs
"""
# 3rd party libs
from bokeh.models import DataTable, TableColumn, ColumnDataSource, CustomJS
import numpy as np

# local libs
from web_base import load_db


def make_table(results: list):
    """
    Creates the table and data source components

    Parameters
    ----------
    results
        The list of objects in the database

    Returns
    -------
    cds
        ColumnDataSource, all the data by which the datatable is generated
    dt
        DataTable, the bokeh object to be displayed on page
    """
    vals = np.empty((len(results), 3), dtype=object)
    for i, obj in enumerate(results):
        vals[i] = obj[:3]  # only take name, ra, dec
    data = dict(
        sname=vals[:, 0].astype(str),  # object name
        ra=vals[:, 1].astype(float),  # object ra
        dec=vals[:, 2].astype(float)  # object dec
    )
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
    db = load_db()  # load the database
    # TODO: what about when there are tonnes of objects, need to cut up table or have searching implemented
    # TODO: a callback on the dt object to parse the next X sources
    results = db.query(db.Sources).all()  # query all the sources into one list
    cds, dt = make_table(results)  # create the bokeh elements
    return cds, dt


if __name__ == '__main__' or 'bokeh' in __name__:
    main()
