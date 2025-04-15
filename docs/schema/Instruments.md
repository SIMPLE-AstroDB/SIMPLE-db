## Instruments
### Description
Instrument information
### Columns
| Column Name | Description | Datatype | Length | Units  | UCD | Nullable |
| --- | --- | --- | --- | --- | --- | --- |
| instrument | Instrument name | string | 30 |  | instr;meta.main | False |
| mode | Instrument mode | string | 30 |  |  | True |
| telescope | Telescope, mission, or survey name; links to Telescopes table | string | 30 |  |  | True |
| description | Instrument description | string | 1000 |  | meta.note | True |
| reference | Publication reference; links to Publications table | string | 30 |  | meta.ref | True |

### Constraints
| Type | Description | Columns | Referenced Columns |
| --- | --- | --- | --- |
| ForeignKey | Link Instruments reference to Publications table | ['#Instruments.reference'] | ['#Publications.reference'] |
| ForeignKey | Link Instruments telescope to Telescopes table | ['#Instruments.telescope'] | ['#Telescopes.telescope'] |

