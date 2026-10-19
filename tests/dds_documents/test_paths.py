from __future__ import annotations

import pytest

from eea_datalakehouse.dds_documents import to_catalog_path, to_storage_path


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("catalog.water.bwd", "catalog/water/bwd"),
        (" catalog.water.bwd. ", "catalog/water/bwd"),
        ("catalog/water/bwd", "catalog/water/bwd"),
        ("catalog/water/v1.0", "catalog/water/v1.0"),  # has '/': storage format, dots kept
        ("bwd", "bwd"),
        ('catalog."water_management_resources".bwd', "catalog/water_management_resources/bwd"),
        ('catalog."bathing water"."v1.0"', "catalog/bathing water/v1.0"),
        (
            'nossl_s3."datahub-pre-01".datasets."[EU SDG 14_40] Bathing waters."',
            "nossl_s3/datahub-pre-01/datasets/[EU SDG 14_40] Bathing waters.",
        ),
        ('catalog."say ""hi""".x', 'catalog/say "hi"/x'),  # "" escapes a quote
        ('"a/b".c', "a/b/c"),  # '/' only inside quotes: still catalog format
        ('catalog/"v1.0"/x', "catalog/v1.0/x"),  # storage format, quotes removed too
    ],
)
def test_to_storage_path(path: str, expected: str) -> None:
    assert to_storage_path(path) == expected


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("catalog/water_management_resources/bwd", "catalog.water_management_resources.bwd"),
        ("catalog.water.bwd", "catalog.water.bwd"),
        ("catalog/bathing water/v1.0", 'catalog."bathing water"."v1.0"'),
        ("nossl_s3/datahub-pre-01/x", 'nossl_s3."datahub-pre-01".x'),
        ('catalog."say ""hi"""', 'catalog."say ""hi"""'),  # escaped quote kept escaped
    ],
)
def test_to_catalog_path(path: str, expected: str) -> None:
    assert to_catalog_path(path) == expected
    assert to_storage_path(expected) == to_storage_path(path)  # round trip


def test_to_storage_path_rejects_an_unclosed_quote() -> None:
    with pytest.raises(ValueError, match="unclosed quote"):
        to_storage_path('catalog."water.bwd')
