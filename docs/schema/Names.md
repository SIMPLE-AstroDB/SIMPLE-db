# Names
Additional identifiers for objects in Sources table


Columns marked with an exclamation mark ( :exclamation:) may not be empty.
| Column Name | Description | Datatype | Length | Units  | UCD |
| --- | --- | --- | --- | --- | --- |
| :exclamation:**source** | Main identifier for an object; links to Sources table | string | 100 |  | meta.id;meta.main  |
| :exclamation:**other_name** | Alternate identifier for an object | string | 100 |  | meta.id  |

## Indexes
| Name | Columns | Description |
| --- | --- | --- |
| PK_Names_source | ['#Names.source', '#Names.other_name'] | Primary key for Names table |

## Foreign Keys
| Description | Columns | Referenced Columns |
| --- | --- | --- |
| Link Names primary identifer to Sources table | ['#Names.source'] | ['#Sources.source'] |
