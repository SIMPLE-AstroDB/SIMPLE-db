# Telescopes
Telescope, mission, and survey information
Columns marked with an exclamation mark ( :exclamation:) may not be empty.
| Column Name | Description | Datatype | Length | Units  | UCD |
| --- | --- | --- | --- | --- | --- |
| :exclamation:**telescope** | Telescope, mission, or survey name | string | 30 |  | meta.id;meta.main  |
| description | Telescope description | string | 1000 |  | meta.note  |
| reference | Publication reference; links to Publications table | string | 30 |  |   |

## Constraints
| Type | Description | Columns | Referenced Columns |
| --- | --- | --- | --- |
| ForeignKey | Link Telescopes reference to Publications table | ['#Telescopes.reference'] | ['#Publications.reference'] |

