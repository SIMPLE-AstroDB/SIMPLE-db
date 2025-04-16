# RadialVelocities
The RadialVelocities table contains radial velocity measurements for sources listed in the Sources table. The combination of *source* and *reference* is expected to be unique.


Columns marked with an exclamation mark ( :exclamation:) may not be empty.
| Column Name | Description | Datatype | Length | Units  | UCD |
| --- | --- | --- | --- | --- | --- |
| :exclamation:<u>source</u> | Unique identifier for a source; links to Sources table | string | 100 |  | meta.id;meta.main  |
| :exclamation:radial_velocity_km_s | Radial velocity measurement for the source | double |  | km/s | spect.dopplerVeloc  |
| radial_velocity_error_km_s | Uncertainty of the radial velocity value | double |  | km/s | stat.error;spect.dopplerVeloc  |
| adopted | Flag to indicate if this is the adopted entry | boolean |  |  |   |
| comments | Free form comments | string | 1000 |  | meta.note  |
| :exclamation:<u>reference</u> | Reference; links to Publications table | string | 30 |  | meta.ref  |

## Indexes
| Name | Columns | Description |
| --- | --- | --- |
| PK_RadialVelocities | ['#RadialVelocities.source', '#RadialVelocities.reference'] | Primary key for Radial Velocities table |

## Foreign Keys
| Description | Columns | Referenced Columns |
| --- | --- | --- |
| Link RadialVelocities source to Sources table | ['#RadialVelocities.source'] | ['#Sources.source'] |
| Link RadialVelocities reference to Publications table | ['#RadialVelocities.reference'] | ['#Publications.reference'] |
## Checks
| Description | Expression |
| --- | --- |
| Validate radial velocity error | radial_velocity_error_km_s >= 0 |
