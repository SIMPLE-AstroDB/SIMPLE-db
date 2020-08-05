#!/usr/bin/env python
"""
Generates the whole database as a table
"""
# 3rd party libs
from bokeh.models import DataTable, TableColumn, ColumnDataSource, CustomJS
import numpy as np

# local libs
from web_base import load_db


def make_table(results: list):
    vals = np.empty((len(results), 3), dtype=object)
    for i, obj in enumerate(results):
        vals[i] = obj[:3]
    data = dict(
        sname=vals[:, 0].astype(str),
        ra=vals[:, 1].astype(float),
        dec=vals[:, 2].astype(float)
    )
    cds = ColumnDataSource(data)
    cols = [
        TableColumn(field="sname", title="Name"),
        TableColumn(field="ra", title="RA [deg]"),
        TableColumn(field="dec", title="Dec [deg]")
    ]
    dt = DataTable(source=cds, columns=cols, sizing_mode='stretch_width', selectable=True, name="datatable")
    return cds, dt


def main():
    db = load_db()
    results = db.query(db.Sources).all()
    cds, dt = make_table(results)
    return cds, dt


if __name__ == '__main__' or 'bokeh' in __name__:
    main()
