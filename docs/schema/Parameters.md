# Parameters
Parameters lookup table
Columns marked with an exclamation mark (:exclamation:) may not be empty.
| Column Name | Description | Datatype | Length | Units  | UCD |
| --- | --- | --- | --- | --- | --- |
| :exclamation:**parameter** | Main identifier for a parameter | string | 30 |  | meta.id;meta.main  |
| description | Description of the parameter | string | 1000 |  | meta.note  |

## Indexes
| Name | Columns | Description |
| --- | --- | --- |
| PK_Parameters | ['#Parameters.parameter'] | Primary key for Parameters table |

