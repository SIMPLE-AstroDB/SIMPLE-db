# Gravities
Surface gravities for Sources
| Column Name | Description | Datatype | Length | Units  | UCD | Nullable |
| --- | --- | --- | --- | --- | --- | --- |
| :exclamation:**source** | Main identifier for an object; links to Sources table | string | 100 |  | meta.id;meta.main | False |
| gravity | Gravity from enumerated values (alpha, beta, etc) | string | 20 |  | meta.code;phys.gravity | True |
| regime | Gravity regime; links to Regimes table | string | 30 |  | meta.id | True |
| comments | Free-form comments for this entry | string | 1000 |  | meta.note | True |
| reference | Publication reference; links to Publications table | string | 30 |  |  | True |

## Indexes
| Name | Columns | Description |
| --- | --- | --- |
| PK_Gravities | ['#Gravities.source', '#Gravities.gravity', '#Gravities.reference'] | Primary key for Gravities table |

## Constraints
| Type | Description | Columns | Referenced Columns |
| --- | --- | --- | --- |
| ForeignKey | Link Gravities source to Sources table | ['#Gravities.source'] | ['#Sources.source'] |
| ForeignKey | Link Gravities reference to Publications table | ['#Gravities.reference'] | ['#Publications.reference'] |
| ForeignKey | Link Gravities regime to Regimes table | ['#Gravities.regime'] | ['#Regimes.regime'] |

