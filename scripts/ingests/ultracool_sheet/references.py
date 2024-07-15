from astrodb_utils import ingest_publication, AstroDBError
from astropy.io import ascii


# Load Ultracool sheet refrences
doc_id = "1i98ft8g5mzPp2DNno0kcz4B9nzMxdpyz5UquAVhz-U8"  # Last update: 2024-02-04 23:29:26 (UTC)

sheet_id = "453417780"
link = (
    f"https://docs.google.com/spreadsheets/d/{doc_id}/export?format=csv&gid={sheet_id}"
)

# read the csv data into an astropy table
uc_reference_table = ascii.read(
    link,
    format="csv",
    data_start=1,
    header_start=0,
    guess=False,
    fast_reader=False,
    delimiter=",",
)

uc_ref_to_ADS = {}
for ref in uc_reference_table:
    uc_ref_to_ADS[ref["code_ref"]] = ref["ADSkey_ref"]


def uc_ref_to_simple_ref(db, ref):
    if ref == "Harr15":
        return ref
    if uc_ref_to_ADS[ref][0:5] == "noADS":
        msg = "Reference match failed due to bad publication"
        raise AstroDBError(msg)
    name = ref
    if ref == "Alle06b":
        name = "Alle06PhD"
    if ref == "Schn23b":
        name = "Schn23.ace9"
    if ref == "Alle16b":
        name = "Alle16.PhDT"
    if ref == "Stol20b":
        name = "Stol20"
    t = (
        db.query(db.Publications)
        .filter(db.Publications.c.bibcode == uc_ref_to_ADS[ref])
        .astropy()
    )
    if len(t) == 0:
        ingest_publication(db, bibcode=uc_ref_to_ADS[ref], reference=name)
        return name
    elif len(t) == 1:
        return t["reference"][0]
    else:
        msg = "Multiple reference match"
        raise AstroDBError(msg)
