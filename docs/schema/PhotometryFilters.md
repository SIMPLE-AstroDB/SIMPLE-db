# PhotometryFilters
Photometry filter information. This stores relationships between filters and instruments, telescopes, as well as wavelength and width
Columns marked with an exclamation mark (:exclamation:) may not be empty.
| Column Name | Description | Datatype | Length | Units  | UCD |
| --- | --- | --- | --- | --- | --- |
| :exclamation:**band** | Band name. Must be of the form instrument.filter | string | 30 |  | instr.bandpass;meta.main  |
| ucd | Unified Content Descriptor of the photometry filter | string | 100 |  |   |
| :exclamation:**effective_wavelength** | Effective wavelength of the photometry filter in Angstroms | double |  | Angstrom |   |
| width | Width of the ephotometry filter in Angstroms | double |  | Angstrom | instr.bandwidth  |

