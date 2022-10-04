# ModeledParameters

The ModeledParameters table contains a range of derived/inferred parameters for sources listed in the Sources table. 
The combination of *source*, *parameter*, and *reference* is expected to be unique.
Note that *parameter* is a string constrained from a list of enumerated values. 
Columns marked with an asterisk (*) may not be empty.

| Column Name | Description  | Unit  | Data Type | Key Type  |
|---|---|---|---|---|
| *source    | Unique identifier for the source |   | String(100)  | primary and foreign: Sources.source   |
| *parameter | Name of parameter |  | Enumeration  | primary |
| *value      | Value for parameter | | Float    |     |
| comments  | Free form comments |   | String(1000) |   |
| *reference | Reference |   | String(30) | primary and foreign: Publications.name |

Enumeraions for parameter include:
 - mass
 - radius
 - log_g
 - T_eff
 - metallicity
 - CO
