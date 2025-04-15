# SpectralTypes
The SpectralTypes table contains spectral type measurements for sources listed in the Sources table. The combination of *source*, *spectral_type_string*, *spectral_type_code*, *regime*, and *reference* is expected to be unique.


Columns marked with an exclamation mark ( :exclamation:) may not be empty.
| Column Name | Description | Datatype | Length | Units  | UCD |
| --- | --- | --- | --- | --- | --- |
| :exclamation:**source** | Unique identifier for a source; links to Sources table | string | 100 |  | meta.id;meta.main  |
| :exclamation:**spectral_type_string** | Spectral type string | string | 20 |  | meta.id;src.spType  |
| :exclamation:**spectral_type_code** | Numeric code corresponding to spectral type. 60 = M0, 69 = M9, 70 = L0, 80 = T0, 90 = Y0 | double |  |  | meta.code;src.spType  |
| spectral_type_error | Uncertainty of the spectral type value | double |  |  | stat.error;src.spType  |
| :exclamation:**regime** | Spectral type regime; links to Regimes table | string | 30 |  | meta.id  |
| adopted | Flag to indicate if this is the adopted entry | boolean |  |  |   |
| photometric | Flag to indicate if this is a photometric spectral type | boolean |  |  |   |
| comments | Free form comments | string | 1000 |  | meta.note  |
| reference | Reference; links to Publications table | string | 30 |  |   |

## Indexes
| Name | Columns | Description |
| --- | --- | --- |
| PK_SpectralTypes | ['#SpectralTypes.source', '#SpectralTypes.spectral_type_string', '#SpectralTypes.spectral_type_code', '#SpectralTypes.regime', '#SpectralTypes.reference'] | Primary key for Spectral Types table |

## Foreign Keys
| Description | Columns | Referenced Columns |
| --- | --- | --- |
| Link SpectralTypes source to Sources table | ['#SpectralTypes.source'] | ['#Sources.source'] |
| Link SpectralTypes reference to Publications table | ['#SpectralTypes.reference'] | ['#Publications.reference'] |
| Link SpectralTypes regime to Regimes table | ['#SpectralTypes.regime'] | ['#Regimes.regime'] |
