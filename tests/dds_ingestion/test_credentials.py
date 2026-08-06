from __future__ import annotations

import pytest
from eea_datalakehouse.dds_ingestion.credentials import (
    ENV_BASE_URL,
    ENV_PWD,
    ENV_USER,
    DremioCreds,
    MissingCredentialsError,
    load_base_url,
    load_creds,
)


def test_load_creds_from_env() -> None:
    creds = load_creds({ENV_USER: "bob", ENV_PWD: "pw"})
    assert creds.username == "bob"
    # _DREMIO_PWD is the PAT, sent as the Bearer token.
    assert creds.password == "pw"


def test_load_creds_missing_raises() -> None:
    with pytest.raises(MissingCredentialsError):
        load_creds({ENV_USER: "bob"})


def test_load_base_url_strips_trailing_slash() -> None:
    assert load_base_url({ENV_BASE_URL: "https://dds.test/"}) == "https://dds.test"


def test_load_base_url_missing_raises() -> None:
    with pytest.raises(MissingCredentialsError):
        load_base_url({})


def test_password_redacted_in_repr_and_str() -> None:
    creds = DremioCreds(username="alice", _password="TOP-SECRET")
    assert "TOP-SECRET" not in repr(creds)
    assert "TOP-SECRET" not in str(creds)
    assert "***" in repr(creds)
    # the secret is still retrievable via the explicit accessor
    assert creds.password == "TOP-SECRET"
