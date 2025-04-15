# Parameters
The Parameters table is a lookup table that contains names and descriptions for parameters referred to in the ModeledParameters table. The *parameter* name is required to be unique.


Columns marked with an exclamation mark ( :exclamation:) may not be empty.
| Column Name | Description | Datatype | Length | Units  | UCD |
| --- | --- | --- | --- | --- | --- |
| :exclamation:**parameter** | Short name for a parameter | string | 30 |  | meta.id;meta.main  |
| description | Description of the parameter | string | 1000 |  | meta.note  |

## Indexes
| Name | Columns | Description |
| --- | --- | --- |
| PK_Parameters | ['#Parameters.parameter'] | Primary key for Parameters table |

