## RadialVelocities
### Description
Radial Velocities of Sources
### Columns
| Column Name | Description | Datatype | Length | Units  | UCD | Nullable |
| --- | --- | --- | --- | --- | --- | --- |
| source | Main identifier for an object; links to Sources table | string | 50 |  | meta.id;meta.main | False |
| radial_velocity_km_s | Radial velocity value for this entry | double |  | km/s | spect.dopplerVeloc | True |
| radial_velocity_error_km_s | Uncertainty of the radial velocity value | double |  | km/s | stat.error;spect.dopplerVeloc | True |
| adopted | Flag to indicate if this is the adopted entry | boolean |  |  | meta.code | True |
| comments | Free-form comments for this entry | string | 1000 |  | meta.note | True |
| reference | Publication reference; links to Publications table | string | 30 |  | meta.ref | False |

### Indexes
| Name | Columns | Description |
| --- | --- | --- |
| PK_RadialVelocities | ['#RadialVelocities.source', '#RadialVelocities.reference'] | Primary key for Radial Velocities table |

### Constraints
| Type | Description | Columns | Referenced Columns |
| --- | --- | --- | --- |
| ForeignKey | Link RadialVelocities source to Sources table | ['#RadialVelocities.source'] | ['#Sources.source'] |
| ForeignKey | Link RadialVelocities reference to Publications table | ['#RadialVelocities.reference'] | ['#Publications.reference'] |
| Check | Validate radial velocity error |  |  |

