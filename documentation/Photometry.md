# Photometry

The Photometry table contains photometric measurements for sources listed in the Sources table. 
It refers back to the Telescopes and Instruments tables as well.
The combination of *source*, *band*, and *reference* is expected to be unique. 
Columns marked with an asterisk (*) may not be empty.

| Column Name | Description  | Unit  | Data Type | Key Type  |
|---|---|---|---|---|
| *source    | Unique identifier for the source |   | String(100)  | primary and foreign: Sources.source   |
| *band      | Name of the wavelength band |  | String(30)  | primary |
| ucd       | Unified Content Descriptor for band |  | String(100)  |   |
| magnitude | Magnitude value | mag | Float  |   |
| magnitude_error | Magnitude uncertainty | mag | Float  |   |
| telescope | Name of telescope |  | String(30)  | foreign: Telescopes.name |
| instrument | Name of instrument |  | String(30)  | foreign: Instruments.name |
| epoch     | Decimal year | year | Float |   |
| comments  | Free form comments |   | String(1000) |   |
| *reference | Reference |   | String(30) | primary and foreign: Publications.name |

## Notes
- Band names refer to entries in the [Photometry Filters](PhotometryFilters.md) table and are listed at the [SVO filter profile service](http://svo2.cab.inta-csic.es/svo/theory/fps3/index.php?mode=browse&gname=Spitzer&asttype=).
- UCDs are listed in the [IVOA controlled vocabulary](https://www.ivoa.net/documents/UCD1+/20200212/PEN-UCDlist-1.4-20200212.html#tth_sEcB).
  Common ones for cool stars are:
  - `em.opt.R`	Optical band between 600 and 750 nm
  - `em.opt.I`	Optical band between 750 and 1000 nm
  - `em.IR.J`	Infrared between 1.0 and 1.5 micron
  - `em.IR.H`	Infrared between 1.5 and 2 micron 
  - `em.IR.K`	Infrared between 2 and 3 micron 
  - `em.IR.3-4um`	Infrared between 3 and 4 micron
  - `em.IR.4-8um`	Infrared between 4 and 8 micron
  