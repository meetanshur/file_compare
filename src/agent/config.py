"""Validated LLM provider settings and environment-based secret resolution."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from enum import StrEnum
from pathlib import Path
from typing import Self

import yaml
from pydantic import BaseModel, ConfigDict, Field, SecretStr, ValidationError, model_validator


class LLMProvider(StrEnum):
    """Supported provider identifiers; adapters may be added independently."""

    OPENAI = "openai"
    AZURE_OPENAI = "azure_openai"
    ANTHROPIC = "anthropic"
    LOCAL = "local"


class AgentConfig(BaseModel):
    """Provider-neutral, validated settings for the LLM orchestration layer."""

    model_config = ConfigDict(extra="forbid")

    provider: LLMProvider = LLMProvider.OPENAI
    model: str = Field(default="gpt-4o-mini", min_length=1)
    api_key_env: str = Field(default="OPENAI_API_KEY", min_length=1)
    base_url: str | None = None
    timeout_seconds: float = Field(default=30.0, gt=0, le=600)
    max_retries: int = Field(default=2, ge=0, le=10)
    max_output_tokens: int = Field(default=2048, gt=0, le=100_000)
    max_request_text_chars: int = Field(default=12_000, gt=0, le=1_000_000)
    max_explanation_findings: int = Field(default=20, ge=0, le=1_000)
    max_explanation_chars: int = Field(default=8_000, ge=512, le=100_000)
    max_sample_records: int = Field(default=5, ge=0, le=100)
    max_sample_chars: int = Field(default=2_000, ge=0, le=20_000)
    mask_sample_digits: bool = True
    enable_config_proposals: bool = True

    @model_validator(mode="before")
    @classmethod
    def reject_inline_api_key(cls, data: object) -> object:
        if isinstance(data, Mapping) and "api_key" in data:
            environment_name = data.get("api_key_env", "OPENAI_API_KEY")
            if not isinstance(environment_name, str) or not environment_name:
                environment_name = "api_key_env"
            raise ValueError(
                f"api_key is not allowed in config; set the {environment_name} environment "
                "variable instead"
            )
        return data

    @model_validator(mode="after")
    def validate_provider_options(self) -> Self:
        if self.provider is LLMProvider.OPENAI and self.base_url is not None:
            if not self.base_url.startswith(("https://", "http://")):
                raise ValueError("base_url must be an HTTP(S) URL")
        return self

    def resolve(self, environ: Mapping[str, str] | None = None) -> ResolvedAgentConfig:
        """Resolve credentials only from the configured environment variable."""
        environment = os.environ if environ is None else environ
        return ResolvedAgentConfig(config=self, api_key=environment.get(self.api_key_env))


class ResolvedAgentConfig(BaseModel):
    """Runtime settings with a redacted API key resolved from configuration or env."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    config: AgentConfig
    api_key: SecretStr | None = Field(default=None, repr=False)

    def __init__(self, *, config: AgentConfig, api_key: str | None) -> None:
        super().__init__(
            config=config,
            api_key=SecretStr(api_key) if api_key is not None else None,
        )

    def require_api_key(self) -> str:
        """Return the credential or raise an explicit provider configuration error."""
        if self.api_key is None or not self.api_key.get_secret_value():
            raise ValueError(
                f"Missing API key: set the environment variable {self.config.api_key_env}"
            )
        return self.api_key.get_secret_value()


def validate_agent_config(data: Mapping[str, object]) -> AgentConfig:
    """Validate a mapping and report Pydantic locations in a simple config error."""
    try:
        return AgentConfig.model_validate(data)
    except ValidationError as exc:
        details = "; ".join(
            f"{'.'.join(str(part) for part in error['loc']) or '<root>'}: {error['msg']}"
            for error in exc.errors()
        )
        raise ValueError(f"Invalid agent config: {details}") from exc


def load_agent_config(path: str | Path) -> AgentConfig:
    """Load a provider config from JSON or YAML without reading credentials from disk."""
    config_path = Path(path)
    suffix = config_path.suffix.lower()
    text = config_path.read_text(encoding="utf-8")
    if suffix == ".json":
        data = json.loads(text)
    elif suffix in {".yaml", ".yml"}:
        data = yaml.safe_load(text)
    else:
        raise ValueError(f"Unsupported agent config extension: {suffix or '(none)'}")
    if not isinstance(data, Mapping):
        raise ValueError("Invalid agent config: <root>: expected a mapping")
    return validate_agent_config(data)
