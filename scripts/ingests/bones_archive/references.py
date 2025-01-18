from astrodb_utils import ingest_publication, AstroDBError
from astropy.io import ascii


# Load Ultracool sheet refrences
doc_id = "1i98ft8g5mzPp2DNno0kcz4B9nzMxdpyz5UquAVhz-U8"  # Last update: 2024-02-04 23:29:26 (UTC)

sheet_id = "453417780"
##link = (
    ##f"https://docs.google.com/spreadsheets/d/{doc_id}/export?format=csv&gid={sheet_id}"
##)
link = (
    "scripts/bones_archive/bones_archive_properties.csv"
)

# read the csv data into an astropy table
bones_reference_table = ascii.read(
    link,
    format="csv",
    data_start=1,
    header_start=0,
    guess=False,
    fast_reader=False,
    delimiter=",",
)

bones_ref_to_ADS = {}
for ref in bones_reference_table:
    bones_ref_to_ADS[ref["code_ref"]] = ref["ADSkey_ref"]


def bones_ref_to_simple_ref(db, ref):
    name = ref
    t = (
        db.query(db.Publications)
        .filter(db.Publications.c.bibcode == bones_ref_to_ADS[ref])
        .astropy()
    )
    if len(t) == 0:
        ingest_publication(db, bibcode=bones_ref_to_ADS[ref], reference=name)
        return name
    elif len(t) == 1:
        return t["reference"][0]
    else:
        msg = "Multiple reference match"
        raise AstroDBError(msg)
