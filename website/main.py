"""
Main bokeh serve host

Methods
-------
info_box
    The <pre> box to be filled with the object inventory
photo_table
    The empty photometric table to be populated with object photometry
spectra_fig
    The spectra for an object which will be populated with all object spectra
main
    Generating initial elements on current document and assigns callback to fill them on selection
"""
# inbuilt libs
from string import Template
# 3rd party imports
from bokeh.models import ColumnDataSource, PreText, DataTable, TableColumn, TextInput, CustomJS, Button
from bokeh.plotting import curdoc, figure
import numpy as np
# local imports
import splash_table as st
import gen_info as geninfo
#  import gen_plot as genspec
import gen_photo as genphot

input_string = Template('$typed')


def info_box():
    """
    Generates a <pre> box to be filled with the object inventory

    Returns
    -------
    infopre
        Bokeh PreText object with empty text
    """
    # TODO: fix the object info to be 2 column, fitting to surrounding div not relying on overflow-y
    infopre = PreText(text='', sizing_mode='stretch_both', name='objectinfo')
    return infopre


def photo_table():
    """
    Creates an empty DataTable to be filled with photometric data

    Returns
    -------
    photdt
        Bokeh DataTable object containing columns linked to cds
    cds
        Bokeh ColumnDataSource containing null data
    """
    bands = [*genphot.KNOWN_MAGS.keys(), ]  # the photometric bands to start with
    # TODO: change what bands are shown initially, do not need to be this restrictive list
    data = dict(
        bands=bands,  # photometric bands
        vals=[None, ] * len(bands),  # null list for empty cells
        valserr=[None, ] * len(bands)  # null list for empty cells
    )
    cds = ColumnDataSource(data)  # pass to bokeh columndatasource object
    cols = [
        TableColumn(field="bands", title="Band", width=100),  # the column for photometric bands
        TableColumn(field="vals", title="Magnitude", width=100),  # the column for magnitudes
        TableColumn(field="valserr", title="Error", width=100)  # the column for magnitude errors
    ]
    photdt = DataTable(source=cds, columns=cols, name='photodt',  # the cds to link to, columns and element id
                       fit_columns=False, index_position=None,  # for making small width table and ignore index column
                       width=300, height=150)  # set exact size (pixels)
    return photdt, cds


def spectra_fig():
    """
    Creates the empty figure and relevant ColumnDataSource

    Returns
    -------
    specfig
        Bokeh figure object for showing spectra
    cds
        Bokeh ColumnDataSource object relating to the spectral data
    """
    # TODO: think of a better way of doing multiple spectra on one plot, maybe call for each spectra in total number
    # TODO: could add photometry to this plot, requires knowing mags, their central wavelengths and zero points
    # TODO: decide what we want to see in splash view
    # TODO: decide if we want it normalised or not
    data = dict(
        wave=np.linspace(0.1, 10, 100),  # initial view
        flux=np.random.randn(100) * 0.5 + 1,  # some random flux data
    )
    cds = ColumnDataSource(data)  # pass to bokeh object
    tooltips = [('Wavelength', '@wave'), ('Flux', '@flux')]  # tooltips for hovering
    specfig = figure(tools='pan,box_zoom,wheel_zoom,reset,save,hover', tooltips=tooltips,  # bokeh tools
                     sizing_mode='stretch_width', name='spectraplot', height=495,  # sizing and ids w height in pixels
                     title='Empty Spectra', x_axis_label='Wavelength (um)', y_axis_label='Flux (erg/s/s/um)')  # format
    specfig.line(source=cds, x='wave', y='flux')  # create line plot for spectra
    # font sizing
    specfig.title.text_font_size = '18pt'
    specfig.xaxis.major_label_text_font_size = '14pt'
    specfig.xaxis.axis_label_text_font_size = '16pt'
    specfig.yaxis.major_label_text_font_size = '14pt'
    specfig.yaxis.axis_label_text_font_size = '16pt'
    return specfig, cds


def search_bar():
    """
    Creates the searchbar element text input

    Returns
    -------
    sbar
        The searchbar bokeh object
    """
    sbar = TextInput(value='', title='Object Name Search:', name='searchbar')
    return sbar


def search_button():
    """
    Creates the search button to bring div back

    Returns
    -------
    sbutt
        The search button
    """
    sbutt = Button(label='Search', button_type="success", name='searchbutton')
    return sbutt


def main():
    """
    Generates initial elements in curdoc() and holds callback function

    Methods
    -------
    update
        The callback running on bokeh serve triggered on selection of row
    """
    def update(attrname, old, new):
        """
        Python callback function running in current document on selection of row.
        The parameters shown are not used in shown script but are required in the bokeh callback manager

        Parameters
        ----------
        attrname
            The changing attribute (required under hood)
        old
            The old object (required under hood)
        new
            The new object (required under hood)
        """
        for _ in (attrname, old, new):
            pass  # make pycharm shut up about unused parameters
        try:
            selected_index = dtcds.selected.indices[0]  # the index selected from datatable on page
        except IndexError:
            return
        data = dtcds.data  # the data in table
        target = data['sname'][selected_index]  # pulling out object name

        band, mags, magserr = genphot.main(target)  # grab photometric data fro given object
        photcds.data = dict(bands=band, vals=mags, valserr=magserr)

        inventory = geninfo.main(target)  # pull out full inventory (minus photometry) for given object
        infopre.text = inventory  # update the object info text box

        specfig.title.text = target  # change title on spectra to object name
        # TODO: add spectra in here
        sbar.value = 'bye'
        sbar.value = ''
        return

    def sbar_update(attrname, old, new):
        """
        Python callback function running in current document on search bar keypress
        The parameters shown are not used in shown script but are required in the bokeh callback manager

        Parameters
        ----------
        attrname
            The changing attribute (required under hood)
        old
            The old object (required under hood)
        new
            The new object (required under hood)
        """
        for _ in (attrname, old, new):
            pass  # make pycharm shut up about unused parameters
        query_string = sbar.value_input
        query_string = input_string.substitute(typed=query_string)
        results = st.querying(query_string)
        data = st.results_unpack(results)
        dtcds.data = data
        return

    def clear_page(event):
        """
        Python callback function for clearing the page on button press
        The parameters shown are not used in shown script but are required in the bokeh callback manager

        Parameters
        ----------
        event
            The triggering event object (required under hood)
        """
        for _ in (event, ):
            pass  # make pycharm shut up about unused parameters
        dtcds.selected.indices = []
        sbar.value_input = ''

        km = [*genphot.KNOWN_MAGS.keys(), ]
        vals, valserr = [None, ] * len(km), [None, ] * len(km)
        photcds.data = dict(bands=km, vals=vals, valserr=valserr)

        infopre.text = ''

        specfig.title.text = ''
        return

    # TODO: the animates are not working properly, something weird with js as usual
    # js callbacks
    sbar_callback = CustomJS(code="""
        var AnimationStep = 10; //pixels
        var AnimationInterval = 100; //milliseconds
    
        var oDiv = document.getElementById("searchWrapper");
        if (typeof this.value !== 'undefined') {
            AnimateDown(oDiv, 0); 
            oDiv.style.display = "none";
            document.getElementById("searchButton").style.display = "block"; 
        }
    
        function AnimateDown(element, targetHeight) {
            let curHeight = element.clientHeight;
            if (curHeight <= targetHeight)
                return true;
            let newHeight = curHeight - AnimationStep;
            element.style.height = newHeight.toString() + "px";
            setTimeout(function() {
                AnimateDown(element, targetHeight);
            }, AnimationInterval);
            return false;
        }
    """)
    sbutt_callback = CustomJS(code="""
        var AnimationStep = 10; //pixels
        var AnimationInterval = 10; //milliseconds

        var oDiv = document.getElementById("searchWrapper");
        oDiv.style.display = "block";
        Animate(oDiv, 740);  

        function Animate(element, targetHeight) {
            let curHeight = element.clientHeight;
            if (curHeight >= targetHeight)
                return true;
            let newHeight = curHeight + AnimationStep;
            element.style.height = newHeight.toString() + "px";
            setTimeout(function() {
                Animate(element, targetHeight);
            }, AnimationInterval);
            return false;
        }
        document.getElementById("searchButton").style.display = "none";
        document.getElementsByName("searchbar")[0].focus();
        """)
    # document title
    curdoc().template_variables['title'] = 'Simple Browser'
    curdoc().title = 'Simple Browser'

    photdt, photcds = photo_table()  # create empty photometric table and relating cds object
    curdoc().add_root(photdt)  # embed photometric table in page

    infopre = info_box()  # create empty <pre> box for object info
    curdoc().add_root(infopre)  # embed object info in page

    specfig, specfigcds = spectra_fig()  # create empty bokeh figure and relating cds object
    curdoc().add_root(specfig)  # embed spectra plot in page

    dtcds, dt = st.main()  # the populated table of all sources, ra and dec
    dtcds.selected.on_change('indices', update)  # assign python callback to be on row selection
    curdoc().add_root(dt)  # embed full table in page

    sbar = search_bar()  # the search bar object
    sbar.on_change('value_input', sbar_update)  # called on keypress
    sbar.js_on_change('value', sbar_callback)  # called on enter key or click away
    curdoc().add_root(sbar)

    sbutt = search_button()  # the search button
    sbutt.on_click(clear_page)
    sbutt.js_on_click(sbutt_callback)
    curdoc().add_root(sbutt)
    return


if __name__ == '__main__' or 'bokeh' in __name__:  # nb. the latter condition is true when running in bokeh serve
    main()
