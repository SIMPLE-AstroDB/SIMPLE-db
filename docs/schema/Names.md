# Names
Additional identifiers for objects in Sources table
| Column Name | Description | Datatype | Length | Units  | UCD | Nullable |
| --- | --- | --- | --- | --- | --- | --- |
| :exclamation:**source** | Main identifier for an object; links to Sources table | string | 100 |  | meta.id;meta.main | False |
| :exclamation:**other_name** | Alternate identifier for an object | string | 100 |  | meta.id | False |

## Indexes
| Name | Columns | Description |
| --- | --- | --- |
| PK_Names_source | ['#Names.source', '#Names.other_name'] | Primary key for Names table |

## Constraints
| Type | Description | Columns | Referenced Columns |
| --- | --- | --- | --- |
| ForeignKey | Link Names primary identifer to Sources table | ['#Names.source'] | ['#Sources.source'] |

