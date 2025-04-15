# ModeledParameters
The ModeledParameters table contains a range of derived/inferred parameters for sources listed in the Sources table. The combination of *source*, *parameter*, and *reference* is expected to be unique. Note that *parameter* is linked to the Parameters table. 


Columns marked with an exclamation mark ( :exclamation:) may not be empty.
| Column Name | Description | Datatype | Length | Units  | UCD |
| --- | --- | --- | --- | --- | --- |
| :exclamation:**source** | Unique identifier for the source; links to Sources table | string | 100 |  | meta.id;meta.main  |
| :exclamation:**parameter** | Parameter name; links to Parameters table | string | 30 |  | meta.id  |
| :exclamation:**value** | Value of the parameter | double |  |  | meta.modelled  |
| value_error | Uncertainty of the parameter value | double |  |  | stat.error;meta.modelled  |
| :exclamation:**unit** | Unit of the parameter value. Should be astropy units compatible. | string | 20 |  | meta.unit  |
| comments | Free form comments | string | 1000 |  | meta.note  |
| :exclamation:**reference** | Reference; links to Publications table | string | 30 |  | meta.ref  |

## Indexes
| Name | Columns | Description |
| --- | --- | --- |
| PK_ModeledParameters | ['#ModeledParameters.source', '#ModeledParameters.parameter', '#ModeledParameters.reference'] | Primary key for ModeledParameters table |

## Foreign Keys
| Description | Columns | Referenced Columns |
| --- | --- | --- |
| Link ModeledParameters source to Sources table | ['#ModeledParameters.source'] | ['#Sources.source'] |
| Link ModeledParameters reference to Publications table | ['#ModeledParameters.reference'] | ['#Publications.reference'] |
| Link ModeledParameters parameter to Parameters table | ['#ModeledParameters.parameter'] | ['#Parameters.parameter'] |
