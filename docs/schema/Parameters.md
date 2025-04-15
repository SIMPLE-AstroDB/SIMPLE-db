# Parameters
Parameters lookup table
| Column Name | Description | Datatype | Length | Units  | UCD | Nullable |
| --- | --- | --- | --- | --- | --- | --- |
| :exclamation:**parameter** | Main identifier for a parameter | string | 30 |  | meta.id;meta.main | False |
| description | Description of the parameter | string | 1000 |  | meta.note | True |

## Indexes
| Name | Columns | Description |
| --- | --- | --- |
| PK_Parameters | ['#Parameters.parameter'] | Primary key for Parameters table |

