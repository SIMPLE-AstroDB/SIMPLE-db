# Instruments
Instrument information
Columns marked with an exclamation mark ( :exclamation:) may not be empty.
| Column Name | Description | Datatype | Length | Units  | UCD |
| --- | --- | --- | --- | --- | --- |
| :exclamation:**instrument** | Instrument name | string | 30 |  | instr;meta.main  |
| mode | Instrument mode | string | 30 |  |   |
| telescope | Telescope, mission, or survey name; links to Telescopes table | string | 30 |  |   |
| description | Instrument description | string | 1000 |  | meta.note  |
| reference | Publication reference; links to Publications table | string | 30 |  | meta.ref  |

## Constraints
| Type | Description | Columns | Referenced Columns |
| --- | --- | --- | --- |
| ForeignKey | Link Instruments reference to Publications table | ['#Instruments.reference'] | ['#Publications.reference'] |
| ForeignKey | Link Instruments telescope to Telescopes table | ['#Instruments.telescope'] | ['#Telescopes.telescope'] |

