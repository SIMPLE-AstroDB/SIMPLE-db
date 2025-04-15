# Telescopes
Telescope, mission, and survey information
| Column Name | Description | Datatype | Length | Units  | UCD | Nullable |
| --- | --- | --- | --- | --- | --- | --- |
| telescope | Telescope, mission, or survey name | string | 30 |  | meta.id;meta.main | False |
| description | Telescope description | string | 1000 |  | meta.note | True |
| reference | Publication reference; links to Publications table | string | 30 |  |  | True |

### Constraints
| Type | Description | Columns | Referenced Columns |
| --- | --- | --- | --- |
| ForeignKey | Link Telescopes reference to Publications table | ['#Telescopes.reference'] | ['#Publications.reference'] |

