# PhotometryFilters
The Photometry Filters table contains the names of the filters used by the Photometry table. The combination of *source*, *band*, and *reference* is expected to be unique.


Columns marked with an exclamation mark ( :exclamation:) may not be empty.
| Column Name | Description | Datatype | Length | Units  | UCD |
| --- | --- | --- | --- | --- | --- |
| :exclamation:<u>band</u> | Band name. Must be of the form instrument.filter | string | 30 |  | instr.bandpass;meta.main  |
| ucd | Unified Content Descriptor of the photometry filter | string | 100 |  | meta.ucd  |
| :exclamation:effective_wavelength | Effective wavelength of the photometry filter in Angstroms | double |  | Angstrom |   |
| width | Width of the ephotometry filter in Angstroms | double |  | Angstrom | instr.bandwidth  |

## Checks
| Description | Expression |
| --- | --- |
| Validate width is not negative | width >= 0 |
