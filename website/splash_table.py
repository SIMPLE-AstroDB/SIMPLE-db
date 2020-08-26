#!/usr/bin/env python
"""
Generates the whole database as a table on page

Methods
-------
Qry
    For the query string check
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


class Qry:
    def __init__(self, instr: str):
        if instr == 'notanobject':
            self.default = True
        else:
            self.default = False


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


def querying(qrystr: str = 'notanobject'):
    """
    Queries the database for an object

    Parameters
    ----------
    qrystr
        The partial string of an object

    Returns
    -------
    The list of tuples of object info
    """
    qry = Qry(qrystr)
    db = load_db()  # load the database
    results = db.search_object(qrystr)
    if qry.default:
        return results
    else:
        results = [obj[:3] for obj in results]
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
    cds, dt = make_table(results)  # create the bokeh elements
    return cds, dt


if __name__ == '__main__' or 'bokeh' in __name__:
    main()
