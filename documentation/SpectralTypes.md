# SpectralTypes

The SpectralTypes table contains spectral type measurements for sources listed in the Sources table. 
The combination of *source*, *spectral_type_code*, *regime*, and *reference* is expected to be unique.
Columns marked with an asterisk (*) may not be empty.

| Column Name | Description  | Unit  | Data Type | Key Type  |
|---|---|---|---|---|
| *source    | Unique identifier for the source |   | String(100)  | primary and foreign: Sources.source   |
| spectral_type_string | Spectral type string |  | String(20)  |   |
| *spectral_type_code | Numeric code corresponding to spectral type |  | Float  | primary  |
| spectral_type_error | Uncertainty of spectral type |  | Float  |   |
| *regime | Regime for spectral type value |  |  | primary and foreign:Regimes.regime |
| adopted    | Flag indicating if this is the adopted measurement |  | Boolean  |   |
| photometric    | Flag indicating if this is a photometric spectral type |  | Boolean  |   |
| comments  | Free form comments |   | String(1000) |   |
| *reference | Reference |   | String(30) | primary and foreign: Publications.name |

Spectral Type Codes:
 - 60 = M0 
 - 69 = M9
 - 70 = L0 
 - 80 = T0
 - 90 = Y0

 Regime is required and is constrained by the [Regimes table](/data/Regimes.json). However, `regime = "unknown"` can be used when the regime is unknown. 