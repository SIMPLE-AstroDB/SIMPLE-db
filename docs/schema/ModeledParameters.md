# ModeledParameters
Derived/modeled parameters for sources
Columns marked with an exclamation mark ( :exclamation:) may not be empty.
| Column Name | Description | Datatype | Length | Units  | UCD |
| --- | --- | --- | --- | --- | --- |
| :exclamation:**source** | Main identifier for an object; links to Sources table | string | 100 |  | meta.id;meta.main  |
| :exclamation:**parameter** | Parameter name | string | 30 |  | meta.id  |
| value | Value of the parameter | double |  |  |   |
| value_error | Uncertainty of the parameter value | double |  |  |   |
| unit | Unit of the parameter value. Should be astropy units compatible. | string | 20 |  |   |
| comments | Free-form comments for this entry | string | 1000 |  | meta.note  |
| :exclamation:**reference** | Publication reference; links to Publications table | string | 30 |  | meta.ref  |

## Indexes
| Name | Columns | Description |
| --- | --- | --- |
| PK_ModeledParameters | ['#ModeledParameters.source', '#ModeledParameters.parameter', '#ModeledParameters.reference'] | Primary key for ModeledParameters table |

## Constraints
| Type | Description | Columns | Referenced Columns |
| --- | --- | --- | --- |
| ForeignKey | Link ModeledParameters source to Sources table | ['#ModeledParameters.source'] | ['#Sources.source'] |
| ForeignKey | Link ModeledParameters reference to Publications table | ['#ModeledParameters.reference'] | ['#Publications.reference'] |
| ForeignKey | Link ModeledParameters parameter to Parameters table | ['#ModeledParameters.parameter'] | ['#Parameters.parameter'] |

