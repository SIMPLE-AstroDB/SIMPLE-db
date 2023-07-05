# CompanionRelationships

The CompanionRelationships table contains companions to sources listed in the Sources table. 
The combination of *source* and *companion_name* is expected to be unique.
Columns marked with an asterisk (*) may not be empty.

| Column Name | Description  | Unit  | Data Type | Key Type  |
|---|---|---|---|---|
| *source        | Unique identifier for the source |   | String(100)  | primary and foreign: Sources.source   |
| *companion_name         | SIMBAD resovable name of companion object | | String(100)  | primary  |
| projected_separation_arcsec        | Projected separation between the source and companion | arcsec | Float  |   |
| projected_separation_error   | Uncertainty of projected separation | arcsec | Float  |   |
| relationship    | Relationship of source to companion. See Notes below. |  | String(100)  |   |
| comments      | Free form comments |   | String(1000) |   |
| reference     | Reference |   | String(30) | foreign: Publications.name |

## Notes
Relationships are not currently constrained but should be one of the following:
- *Child*: The source is lower mass/fainter than the companion
- *Sibling*: The source is similar to the companion
- *Parent*: The source is higher mass/brighter than the companion
- *Unresolved Parent*: The source is the unresolved, combined light source of an unresolved multiple system which includes the companion