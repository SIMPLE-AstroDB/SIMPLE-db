# SpectralTypes
Spectral types for Sources
Columns marked with an exclamation mark ( :exclamation:) may not be empty.
| Column Name | Description | Datatype | Length | Units  | UCD |
| --- | --- | --- | --- | --- | --- |
| :exclamation:**source** | Main identifier for an object; links to Sources table | string | 100 |  | meta.id;meta.main  |
| :exclamation:**spectral_type_string** | Spectral type value for this entry | string | 20 |  | meta.id;src.spType  |
| :exclamation:**spectral_type_code** | Spectral type code for this entry | double |  |  | meta.code;src.spType  |
| spectral_type_error | Uncertainty of the spectral type value | double |  |  | stat.error;src.spType  |
| regime | Spectral type regime; links to Regimes table | string | 30 |  | meta.id  |
| adopted | Flag to indicate if this is the adopted entry | boolean |  |  |   |
| photometric | Flag to indicate if this is a photometric spectral type | boolean |  |  |   |
| comments | Free-form comments for this entry | string | 1000 |  | meta.note  |
| reference | Publication reference; links to Publications table | string | 30 |  |   |

## Indexes
| Name | Columns | Description |
| --- | --- | --- |
| PK_SpectralTypes | ['#SpectralTypes.source', '#SpectralTypes.spectral_type_string', '#SpectralTypes.spectral_type_code', '#SpectralTypes.regime', '#SpectralTypes.reference'] | Primary key for Spectral Types table |

## Constraints
| Type | Description | Columns | Referenced Columns |
| --- | --- | --- | --- |
| ForeignKey | Link SpectralTypes source to Sources table | ['#SpectralTypes.source'] | ['#Sources.source'] |
| ForeignKey | Link SpectralTypes reference to Publications table | ['#SpectralTypes.reference'] | ['#Publications.reference'] |
| ForeignKey | Link SpectralTypes regime to Regimes table | ['#SpectralTypes.regime'] | ['#Regimes.regime'] |

