# SpectralTypes

The SpectralTypes table contains spectral type measurements for sources listed in the Sources table. 
The combination of *source*, *regime*, and *reference* is expected to be unique.
Note that *regime* is a string constrained from a list of enumerated values.

| Column Name | Description  | Unit  | Data Type | Key Type  |
|---|---|---|---|---|
| source    | Unique identifier for the source |   | String(100)  | primary and foreign: Sources.source   |
| spectral_type_string | Spectral type string |  | String(10)  |   |
| spectral_type_code | Numeric code corresponding to spectral type |  | Float  |   |
| spectral_type_error | Uncertainty of spectral type |  | Float  |   |
| regime | Regime for spectral type value |  | Enumeration  | primary |
| adopted    | Flag indicating if this is the adopted measurement |  | Boolean  |   |
| comments  | Free form comments |   | String(1000) |   |
| reference | Reference |   | String(30) | primary and foreign: Publications.name |

Spectral Type Codes:
0 = M0
9 = M9
10 = L0
20 = T0
30 = Y0

Enumerations for regime include:
 - gammaray
 - xray
 - ultraviolet
 - optical
 - infrared
 - millimeter
 - radio
 