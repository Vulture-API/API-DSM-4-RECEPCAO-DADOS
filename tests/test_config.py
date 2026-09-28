import pytest
from pydantic import ValidationError

from station_ingest.config import Settings


def test_default_settings() -> None:
    settings = Settings(_env_file=None)

    assert settings.mqtt_topic == "estacoes/+/dados"
    assert settings.redis_stream == "telemetry:ingest"
    assert settings.batch_size == 500
    assert settings.retention_days == 7


def test_environment_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("INGEST_BATCH_SIZE", "100")
    monkeypatch.setenv("INGEST_REDIS_URL", "rediss://example.test:6380/1")

    settings = Settings(_env_file=None)

    assert settings.batch_size == 100
    assert settings.redis_url == "rediss://example.test:6380/1"


def test_batch_cannot_exceed_queue() -> None:
    with pytest.raises(ValidationError, match="batch_size cannot exceed"):
        Settings(_env_file=None, batch_size=11, queue_max_size=10)


def test_tls_cert_and_key_are_a_pair() -> None:
    with pytest.raises(ValidationError, match="must be set together"):
        Settings(_env_file=None, mqtt_tls_cert_file="client.crt")

