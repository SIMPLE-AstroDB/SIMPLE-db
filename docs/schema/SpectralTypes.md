## SpectralTypes
### Description
Spectral types for Sources
### Columns
| Column Name | Description | Datatype | Length | Units  | UCD | Nullable |
| --- | --- | --- | --- | --- | --- | --- |
| source | Main identifier for an object; links to Sources table | string | 100 |  | meta.id;meta.main | False |
| spectral_type_string | Spectral type value for this entry | string | 20 |  | meta.id;src.spType | False |
| spectral_type_code | Spectral type code for this entry | double |  |  | meta.code;src.spType | False |
| spectral_type_error | Uncertainty of the spectral type value | double |  |  | stat.error;src.spType | True |
| regime | Spectral type regime; links to Regimes table | string | 30 |  | meta.id | True |
| adopted | Flag to indicate if this is the adopted entry | boolean |  |  |  | True |
| photometric | Flag to indicate if this is a photometric spectral type | boolean |  |  |  | True |
| comments | Free-form comments for this entry | string | 1000 |  | meta.note | True |
| reference | Publication reference; links to Publications table | string | 30 |  |  | True |

### Indexes
| Name | Columns | Description |
| --- | --- | --- |
| PK_SpectralTypes | ['#SpectralTypes.source', '#SpectralTypes.spectral_type_string', '#SpectralTypes.spectral_type_code', '#SpectralTypes.regime', '#SpectralTypes.reference'] | Primary key for Spectral Types table |

### Constraints
| Type | Description | Columns | Referenced Columns |
| --- | --- | --- | --- |
| ForeignKey | Link SpectralTypes source to Sources table | ['#SpectralTypes.source'] | ['#Sources.source'] |
| ForeignKey | Link SpectralTypes reference to Publications table | ['#SpectralTypes.reference'] | ['#Publications.reference'] |
| ForeignKey | Link SpectralTypes regime to Regimes table | ['#SpectralTypes.regime'] | ['#Regimes.regime'] |

