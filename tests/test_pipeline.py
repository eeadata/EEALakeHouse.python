import pytest

from eea_datalakehouse_ingestion import IngestionPipeline, IngestionValidationError


def test_hello_world_returns_greeting():
    assert IngestionPipeline().hello_world() == "Hello, world!"


def test_prepare_normalizes_records_into_plain_dicts():
    pipeline = IngestionPipeline()
    source = ({"id": 1, "value": "a"}, {"id": 2, "value": "b"})

    result = pipeline.prepare(source)

    assert result == [{"id": 1, "value": "a"}, {"id": 2, "value": "b"}]
    assert all(type(record) is dict for record in result)


def test_validate_passes_when_required_fields_present():
    pipeline = IngestionPipeline(required_fields=["id", "value"])
    records = pipeline.prepare([{"id": 1, "value": "a"}])

    assert pipeline.validate(records) is True


def test_validate_raises_on_missing_required_field():
    pipeline = IngestionPipeline(required_fields=["id", "value"])
    records = pipeline.prepare([{"id": 1}])

    with pytest.raises(IngestionValidationError, match="record 0"):
        pipeline.validate(records)
