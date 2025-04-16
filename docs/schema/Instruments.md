# Instruments
The Instruments table contains names and references for instruments (and their modes) referred to in other tables. The combination of *instrument*, *mode*, and *telescope* is expected to be unique.


Columns marked with an exclamation mark ( :exclamation:) may not be empty.
| Column Name | Description | Datatype | Length | Units  | UCD |
| --- | --- | --- | --- | --- | --- |
| :exclamation:**instrument** | Name of the instrument | string | 30 |  | instr;meta.main  |
| :exclamation:**mode** | Instrument mode | string | 30 |  |   |
| :exclamation:**telescope** | Telescope, mission, or survey name; links to Telescopes table | string | 30 |  |   |
| description | Instrument description | string | 1000 |  | meta.note  |
| reference | Reference for the instrument and/or mode; links to Publications table | string | 30 |  | meta.ref  |

## Indexes
| Name | Columns | Description |
| --- | --- | --- |
| PK_Instruments | ['#Instruments.instrument', '#Instruments.mode', '#Instruments.telescope'] | Primary key for Instruments table |

## Foreign Keys
| Description | Columns | Referenced Columns |
| --- | --- | --- |
| Link Instruments reference to Publications table | ['#Instruments.reference'] | ['#Publications.reference'] |
| Link Instruments telescope to Telescopes table | ['#Instruments.telescope'] | ['#Telescopes.telescope'] |
