# SpectralTypes

The SpectralTypes table contains spectral type measurements for sources listed in the Sources table. 
The combination of *source*, *regime*, and *reference* is expected to be unique.
Note that *regime* is a string constrained from a list of enumerated values.

| Column Name | Description  | Unit  | Data Type | Key Type  |
|---|---|---|---|---|
| source    | Unique identifier for the source |   | String(100)  | primary and foreign: Sources.source   |
| spectral_type | Spectral type string |  | String(10)  |   |
| spectral_type_error | Uncertainty of spectral type |  | Float  |   |
| regime | Regime for spectral type value |  | Enumeration  |   |
| adopted    | Flag indicating if this is the adopted measurement |  | Boolean  |   |
| comments  | Free form comments |   | String(1000) |   |
| reference | Reference |   | String(30) | primary and foreign: Publications.name |

Enumerations for regime include:
 - gammaray
 - xray
 - ultraviolet
 - optical
 - infrared
 - millimeter
 - radio
 