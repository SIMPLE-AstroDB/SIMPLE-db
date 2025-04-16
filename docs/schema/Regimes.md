# Regimes
Regime lookup table. Values used by Spectra and SpectralTypes tables


Columns marked with an exclamation mark ( :exclamation:) may not be empty.
| Column Name | Description | Datatype | Length | Units  | UCD |
| --- | --- | --- | --- | --- | --- |
| :exclamation:<ins>regime</ins> | Regime identifier string | string | 30 |  | meta.id;meta.main  |
| description | Description of regime | string | 1000 |  |   |

## Indexes
| Name | Columns | Description |
| --- | --- | --- |
| PK_Regimes | ['#Regimes.regime'] | Primary key for Regimes table |

