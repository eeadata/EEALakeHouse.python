from pathlib import Path

import pytest

from eea_datalakehouse.data_preparation import DataValidationError, ParquetTransformer


class _StubTransformer(ParquetTransformer):
    def to_parquet(self, records, destination: Path) -> Path:
        return destination


def test_prepare_normalizes_records_into_plain_dicts():
    transformer = _StubTransformer()
    source = ({"id": 1, "value": "a"}, {"id": 2, "value": "b"})

    result = transformer.prepare(source)

    assert result == [{"id": 1, "value": "a"}, {"id": 2, "value": "b"}]
    assert all(type(record) is dict for record in result)


def test_validate_passes_when_required_fields_present():
    transformer = _StubTransformer(required_fields=["id", "value"])
    records = transformer.prepare([{"id": 1, "value": "a"}])

    assert transformer.validate(records) is True


def test_validate_raises_on_missing_required_field():
    transformer = _StubTransformer(required_fields=["id", "value"])
    records = transformer.prepare([{"id": 1}])

    with pytest.raises(DataValidationError, match="record 0"):
        transformer.validate(records)


def test_parquet_transformer_cannot_be_instantiated_directly():
    with pytest.raises(TypeError):
        ParquetTransformer()
