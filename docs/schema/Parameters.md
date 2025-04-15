## Parameters
### Description
Parameters lookup table
### Columns
| Column Name | Description | Datatype | Length | Units  | UCD | Nullable |
| --- | --- | --- | --- | --- | --- | --- |
| parameter | Main identifier for a parameter | string | 30 |  | meta.id;meta.main | False |
| description | Description of the parameter | string | 1000 |  | meta.note | True |

### Indexes
| Name | Columns | Description |
| --- | --- | --- |
| PK_Parameters | ['#Parameters.parameter'] | Primary key for Parameters table |

