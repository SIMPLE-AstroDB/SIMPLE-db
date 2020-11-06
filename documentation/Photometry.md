# Photometry

The Photometry table contains photometric measurements for sources listed in the Sources table. 
It refers back to the Telescopes and Instruments tables as well.
The combination of *source*, *band*, and *reference* is expected to be unique.

| Column Name | Description  | Unit  | Data Type | Key Type  |
|---|---|---|---|---|
| source    | Unique identifier for the source |   | String(100)  | primary and foreign: Sources.source   |
| band      | Name of the wavelength band |  | String(30)  | primary |
| ucd       | Unified Content Descriptor for band |  | String(100)  |   |
| magnitude | Magnitude value | mag | Float  |   |
| magnitude_error | Magnitude uncertainty | mag | Float  |   |
| telescope | Name of telescope |  | String(30)  | foreign: Telescopes.name |
| instrument | Name of instrument |  | String(30)  | foreign: Instruments.name |
| epoch     |  |   | String(30) |   |
| comments  | Free form comments |   | String(1000) |   |
| reference | Reference to source |   | String(30) | primary and foreign: Publications.name |

