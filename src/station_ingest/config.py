from __future__ import annotations

from pathlib import Path

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application configuration loaded from INGEST_* environment variables."""

    model_config = SettingsConfigDict(
        env_prefix="INGEST_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    mqtt_host: str = "localhost"
    mqtt_port: int = Field(default=1883, ge=1, le=65535)
    mqtt_username: str | None = None
    mqtt_password: SecretStr | None = None
    mqtt_topic: str = "estacoes/+/dados"
    mqtt_client_id: str = "station-ingest-01"
    mqtt_keepalive: int = Field(default=60, ge=1)
    mqtt_max_queued_incoming_messages: int = Field(default=50_000, ge=1)
    mqtt_tls: bool = False
    mqtt_tls_ca_file: Path | None = None
    mqtt_tls_cert_file: Path | None = None
    mqtt_tls_key_file: Path | None = None
    mqtt_tls_insecure: bool = False

    redis_url: str = "redis://localhost:6379/0"
    redis_stream: str = "telemetry:ingest"
    redis_max_connections: int = Field(default=20, ge=1)
    redis_socket_timeout_seconds: float = Field(default=5.0, gt=0)

    queue_max_size: int = Field(default=50_000, ge=1)
    batch_size: int = Field(default=500, ge=1)
    flush_interval_ms: int = Field(default=50, ge=1)
    max_payload_bytes: int = Field(default=256 * 1024, ge=1)
    retention_days: int = Field(default=7, ge=1)
    retention_interval_seconds: float = Field(default=60.0, gt=0)
    retry_initial_seconds: float = Field(default=0.25, gt=0)
    retry_max_seconds: float = Field(default=30.0, gt=0)
    shutdown_timeout_seconds: float = Field(default=30.0, gt=0)

    # Consumidor Redis -> PostgreSQL (python -m station_ingest persist)
    database_url: str | None = None
    persist_group: str = "postgres-persister"
    persist_consumer: str = "persister-01"
    persist_batch_size: int = Field(default=500, ge=1)
    persist_block_ms: int = Field(default=1000, ge=1)

    log_level: str = "INFO"
    metrics_interval_seconds: float = Field(default=30.0, gt=0)

    @model_validator(mode="after")
    def validate_related_values(self) -> Settings:
        if self.batch_size > self.queue_max_size:
            raise ValueError("batch_size cannot exceed queue_max_size")
        if self.retry_initial_seconds > self.retry_max_seconds:
            raise ValueError(
                "retry_initial_seconds cannot exceed retry_max_seconds"
            )
        if bool(self.mqtt_tls_cert_file) != bool(self.mqtt_tls_key_file):
            raise ValueError(
                "mqtt_tls_cert_file and mqtt_tls_key_file must be set together"
            )
        return self
