"""
Main bokeh serve host
"""
# 3rd party imports
from bokeh.models import CustomJS, ColumnDataSource, PreText, DataTable, TableColumn
from bokeh.plotting import curdoc, figure
import numpy as np
# local imports
import splash_table as st
import gen_info as geninfo
import gen_plot as genspec
import gen_photo as genphot


def info_box():
    infopre = PreText(text='', sizing_mode='stretch_both', name='objectinfo')
    return infopre


def photo_table():
    bands = [*genphot.KNOWN_MAGS.keys(), ]
    data = dict(
        bands=bands,
        vals=[None, ] * len(bands),
        valserr=[None, ] * len(bands)
    )
    cds = ColumnDataSource(data)
    cols = [
        TableColumn(field="bands", title="Band", width=100),
        TableColumn(field="vals", title="Magnitude", width=100),
        TableColumn(field="valserr", title="Error", width=100)
    ]
    photdt = DataTable(source=cds, columns=cols, name='photodt', fit_columns=False, index_position=None,
                       width=300, height=150)
    return photdt, cds


def spectra_fig():
    # TODO: think of a better way of doing multiple spectra on one plot, maybe call for each spectra in total number
    data = dict(
        wave=np.linspace(0.1, 10, 100),
        flux=np.random.randn(100) * 0.5 + 1,
    )
    cds = ColumnDataSource(data)
    tooltips = [('Wavelength', '@wave'), ('Flux', '@flux')]
    specfig = figure(tools='pan,box_zoom,wheel_zoom,reset,save,hover', tooltips=tooltips,
                     sizing_mode='stretch_width', name='spectraplot', height=495,
                     title='Empty Spectra', x_axis_label='Wavelength (um)', y_axis_label='Flux (erg/s/s/um)')
    specfig.line(source=cds, x='wave', y='flux')
    specfig.title.text_font_size = '18pt'
    specfig.xaxis.major_label_text_font_size = '14pt'
    specfig.xaxis.axis_label_text_font_size = '16pt'
    specfig.yaxis.major_label_text_font_size = '14pt'
    specfig.yaxis.axis_label_text_font_size = '16pt'
    return specfig, cds


def main():
    def update(attrname, old, new):
        selected_index = dtcds.selected.indices[0]
        data = dtcds.data
        target = data['sname'][selected_index]

        band, mags, magserr = genphot.main(target)
        photcds.data['bands'] = band
        photcds.data['vals'] = mags
        photcds.data['valserr'] = magserr

        inventory = geninfo.main(target)
        infopre.text = inventory

        specfig.title.text = target
        return
    curdoc().template_variables['title'] = 'Simple Browser'

    photdt, photcds = photo_table()
    curdoc().add_root(photdt)

    infopre = info_box()
    curdoc().add_root(infopre)

    specfig, specfigcds = spectra_fig()
    curdoc().add_root(specfig)

    dtcds, dt = st.main()
    dtcds.selected.on_change('indices', update)
    curdoc().add_root(dt)
    return


if __name__ == '__main__' or 'bokeh' in __name__:
    main()
