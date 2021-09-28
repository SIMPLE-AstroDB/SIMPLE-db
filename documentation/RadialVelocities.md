# RadialVelocities

The RadialVelocities table contains radial velocity measurements in km/s for sources listed in the Sources table. 
The combination of *source* and *reference* is expected to be unique.
Columns marked with an asterisk (*) may not be empty.

| Column Name | Description  | Unit  | Data Type | Key Type  |
|---|---|---|---|---|
| *source        | Unique identifier for the source |   | String(100)  | primary and foreign: Sources.source   |
| *radial_velocity         | Radial velocity | km/yr | Float  |   |
| radial_velocity_error   | Uncertainty of radial velocity | km/yr | Float  |   |
| adopted    | Flag indicating if this is the adopted measurement |  | Boolean  |   |
| comments      | Free form comments |   | String(1000) |   |
| *reference     | Reference |   | String(30) | primary and foreign: Publications.name |
