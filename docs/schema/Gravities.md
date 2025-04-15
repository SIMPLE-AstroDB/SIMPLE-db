# Gravities
Surface gravities for Sources


Columns marked with an exclamation mark ( :exclamation:) may not be empty.
| Column Name | Description | Datatype | Length | Units  | UCD |
| --- | --- | --- | --- | --- | --- |
| :exclamation:**source** | Main identifier for an object; links to Sources table | string | 100 |  | meta.id;meta.main  |
| gravity | Gravity from enumerated values (alpha, beta, etc) | string | 20 |  | meta.code;phys.gravity  |
| regime | Gravity regime; links to Regimes table | string | 30 |  | meta.id  |
| comments | Free-form comments for this entry | string | 1000 |  | meta.note  |
| reference | Publication reference; links to Publications table | string | 30 |  |   |

## Indexes
| Name | Columns | Description |
| --- | --- | --- |
| PK_Gravities | ['#Gravities.source', '#Gravities.gravity', '#Gravities.reference'] | Primary key for Gravities table |

## Foreign Keys
| Description | Columns | Referenced Columns |
| --- | --- | --- |
| Link Gravities source to Sources table | ['#Gravities.source'] | ['#Sources.source'] |
| Link Gravities reference to Publications table | ['#Gravities.reference'] | ['#Publications.reference'] |
| Link Gravities regime to Regimes table | ['#Gravities.regime'] | ['#Regimes.regime'] |
