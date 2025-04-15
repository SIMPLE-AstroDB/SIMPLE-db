# Photometry

The Photometry table contains photometric measurements for sources listed in the Sources table. 
It refers back to the Telescopes and Instruments tables as well.
The combination of *source*, *band*, and *reference* is expected to be unique. 
Columns marked with an asterisk (*) may not be empty.

| Column Name | Description  | Unit  | Data Type | Key Type  |
|---|---|---|---|---|
| *source    | Unique identifier for the source |   | String(100)  | primary and foreign: Sources.source   |
| *band      | Name of the wavelength band |  | String(30)  | primary and foreign: PhotometryFilters.band |
| *magnitude | Magnitude value | mag | Float  |   |
| magnitude_error | Magnitude uncertainty | mag | Float  |   |
| telescope | Name of telescope |  | String(30)  | foreign: Telescopes.telescope |
| instrument | Name of instrument |  | String(30)  | foreign: Instruments.instrument |
| epoch     | Decimal year | year | Float |   |
| comments  | Free form comments |   | String(1000) |   |
| *reference | Reference |   | String(30) | primary and foreign: Publications.reference |

## Notes
- Band names refer to entries in the [Photometry Filters](PhotometryFilters.md) table and are listed at the [SVO filter profile service](http://svo2.cab.inta-csic.es/svo/theory/fps3/index.php?mode=browse&gname=Spitzer&asttype=).
