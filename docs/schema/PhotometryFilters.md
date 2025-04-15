## PhotometryFilters
### Description
Photometry filter information. This stores relationships between filters and instruments, telescopes, as well as wavelength and width
### Columns
| Column Name | Description | Datatype | Length | Units  | UCD | Nullable |
| --- | --- | --- | --- | --- | --- | --- |
| band | Band name. Must be of the form instrument.filter | string | 30 |  | instr.bandpass;meta.main | False |
| ucd | Unified Content Descriptor of the photometry filter | string | 100 |  |  | True |
| effective_wavelength | Effective wavelength of the photometry filter in Angstroms | double |  | Angstrom |  | False |
| width | Width of the ephotometry filter in Angstroms | double |  | Angstrom | instr.bandwidth | True |

