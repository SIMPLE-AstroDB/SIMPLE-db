import pytest
from astrodb_utils.utils import (
    AstroDBError,
)
from simple.utils.spectral_types import (
    convert_spt_string_to_code,
    convert_spt_code_to_string,
    ingest_spectral_type,
)


def test_convert_spt_string_to_code():
    # Test conversion of spectral types into numeric values
    assert convert_spt_string_to_code("M5.6") == 65.6
    assert convert_spt_string_to_code("T0.1") == 80.1
    assert convert_spt_string_to_code("Y2pec") == 92


def test_convert_spt_code_to_string():
    # Test conversion of spectral types into numeric values
    assert convert_spt_code_to_string(65.6) == "M5.6"
    assert convert_spt_code_to_string(80.1) == "T0.1"
    assert convert_spt_code_to_string(92, decimals=0) == "Y2"


def test_ingest_spectral_type(temp_db):
    spt_data1 = {
        "source": "Fake 1",
        "spectral_type": "M5.6",
        "regime": "nir",
        "reference": "Ref 1",
    }
    spt_data2 = {
        "source": "Fake 2",
        "spectral_type": "T0.1",
        "regime": "nir",
        "reference": "Ref 1",
    }
    spt_data3 = {
        "source": "Fake 3",
        "spectral_type": "Y2pec",
        "regime": "nir",
        "reference": "Ref 2",
    }
    for spt_data in [spt_data1, spt_data2, spt_data3]:
        ingest_spectral_type(
            temp_db,
            source=spt_data["source"],
            spectral_type_string=spt_data["spectral_type"],
            spectral_type_error=1.0,
            reference=spt_data["reference"],
            regime=spt_data["regime"],
        )
        results = (
            temp_db.query(temp_db.SpectralTypes)
            .filter(temp_db.SpectralTypes.c.source == spt_data["source"])
            .table()
        )
        assert len(results) == 1, f"Expecting this data: {spt_data} in \n {results}"
        assert results["adopted"][0] == True  # noqa: E712

    assert (
        temp_db.query(temp_db.SpectralTypes)
        .filter(temp_db.SpectralTypes.c.reference == "Ref 1")
        .count()
        == 2
    )
    results = (
        temp_db.query(temp_db.SpectralTypes)
        .filter(temp_db.SpectralTypes.c.reference == "Ref 2")
        .table()
    )
    assert len(results) == 1
    assert results["source"][0] == "Fake 3"
    assert results["spectral_type_string"][0] == "Y2pec"
    assert results["spectral_type_code"][0] == [92]


def test_ingest_spectral_type_adopted(temp_db):
    spt_data = {
        "source": "Fake 1",
        "spectral_type": "M5.0",
        "spectral_type_error": 0.5,
        "regime": "optical",
        "reference": "Ref 1",
    }
    ingest_spectral_type(
        temp_db,
        source=spt_data["source"],
        spectral_type_string=spt_data["spectral_type"],
        spectral_type_error=spt_data["spectral_type_error"],
        reference=spt_data["reference"],
        regime=spt_data["regime"],
    )
    results = (
        temp_db.query(temp_db.SpectralTypes)
        .filter(temp_db.SpectralTypes.c.source == spt_data["source"])
        .table()
    )
    assert len(results) == 2
    results = (
        temp_db.query(temp_db.SpectralTypes)
        .filter(
            temp_db.SpectralTypes.c.source == spt_data["source"],
            temp_db.SpectralTypes.c.spectral_type_error == 0.5,
        )
        .table()
    )
    assert results["adopted"][0] == True  # noqa: E712
    results = (
        temp_db.query(temp_db.SpectralTypes)
        .filter(
            temp_db.SpectralTypes.c.source == spt_data["source"],
            temp_db.SpectralTypes.c.spectral_type_error == 1.0,
        )
        .table()
    )
    assert results["adopted"][0] == False  # noqa: E712


@pytest.mark.filterwarnings(
    "ignore", message=".*identifier has incorrect format for catalog*"
)
def test_ingest_spectral_type_errors(temp_db):
    # testing for publication error
    spt_data4 = {
        "source": "Fake 1",
        "spectral_type": "M5.6",
        "regime": "nir",
        "reference": "Ref 1",
    }
    # spt_data5 = {
    #     "source": "Fake 2",
    #     "spectral_type": "T0.1",
    #     "regime": "nir",
    #     "reference": "Ref 1",
    # }
    # spt_data6 = {
    #     "source": "Fake 3",
    #     "spectral_type": "Y2pec",
    #     "regime": "nir",
    #     "reference": "Ref 4",
    # }

    with pytest.raises(AstroDBError) as error_message:
        ingest_spectral_type(
            temp_db,
            source="not a source",
            spectral_type_string=spt_data4["spectral_type"],
            reference=spt_data4["reference"],
            regime=spt_data4["regime"],
        )
    assert "No unique source match" in str(error_message.value)

    with pytest.raises(AstroDBError) as error_message:
        ingest_spectral_type(
            temp_db,
            spt_data4["source"],
            spectral_type_string=spt_data4["spectral_type"],
            reference=spt_data4["reference"],
            regime=spt_data4["regime"],
        )
    assert "Spectral type already in the database" in str(error_message.value)

    with pytest.raises(AstroDBError) as error_message:
        ingest_spectral_type(
            temp_db,
            spt_data4["source"],
            spectral_type_string="M6",
            reference="not a reference",
            regime=spt_data4["regime"],
        )
    assert "The publication does not exist in the database" in str(error_message.value)


def test_ingest_spectral_type_logger(temp_db):
    # https://stackoverflow.com/questions/59875983/why-is-caplog-text-empty-even-though-the-function-im-testing-is-logging
    pass
