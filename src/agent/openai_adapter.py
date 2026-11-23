"""OpenAI Chat Completions adapter with injectable client support."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from openai import OpenAIError

from agent.config import AgentConfig, LLMProvider


class AgentProviderError(RuntimeError):
    """Provider configuration or response failed explicitly."""


@dataclass(frozen=True)
class LLMMessage:
    """One role-tagged message sent to the language model."""

    role: str
    content: str


@dataclass(frozen=True)
class LLMResponse:
    """Text returned by an LLM provider together with provider/model metadata."""

    content: str
    provider: LLMProvider
    model: str


class _ChatCompletions(Protocol):
    def create(self, **kwargs: Any) -> Any: ...


class _OpenAIClient(Protocol):
    chat: Any


class OpenAIAdapter:
    """Call OpenAI's Chat Completions API; deterministic comparisons stay in local tools."""

    def __init__(
        self,
        config: AgentConfig,
        *,
        client: _OpenAIClient | None = None,
        environ: Mapping[str, str] | None = None,
    ) -> None:
        if config.provider is not LLMProvider.OPENAI:
            raise AgentProviderError(
                f"OpenAIAdapter cannot serve provider {config.provider.value!r}"
            )
        self.config = config
        resolved = config.resolve(environ)
        try:
            self.api_key = resolved.require_api_key()
        except ValueError as exc:
            raise AgentProviderError(str(exc)) from exc
        self._client = client if client is not None else self._create_client()

    def _create_client(self) -> _OpenAIClient:
        try:
            from openai import OpenAI
        except ImportError as exc:
            raise AgentProviderError(
                "The OpenAI adapter requires the optional 'openai' package; install the "
                "project dependencies."
            ) from exc

        options: dict[str, object] = {
            "api_key": self.api_key,
            "timeout": self.config.timeout_seconds,
            "max_retries": self.config.max_retries,
        }
        if self.config.base_url is not None:
            options["base_url"] = self.config.base_url
        return OpenAI(**options)

    def complete(self, messages: Sequence[LLMMessage]) -> LLMResponse:
        """Request one assistant response and fail clearly for malformed responses."""
        if not messages:
            raise ValueError("At least one message is required")
        payload = [{"role": message.role, "content": message.content} for message in messages]
        try:
            response = self._client.chat.completions.create(
                model=self.config.model,
                messages=payload,
                max_tokens=self.config.max_output_tokens,
            )
        except OpenAIError as exc:
            raise AgentProviderError(f"OpenAI request failed: {exc}") from exc

        choices = getattr(response, "choices", None)
        if not choices:
            raise AgentProviderError("OpenAI returned no completion choices")
        message = getattr(choices[0], "message", None)
        content = getattr(message, "content", None)
        if not isinstance(content, str):
            raise AgentProviderError("OpenAI returned an empty or non-text response")
        return LLMResponse(content=content, provider=LLMProvider.OPENAI, model=self.config.model)


def create_llm_adapter(
    config: AgentConfig,
    *,
    client: _OpenAIClient | None = None,
    environ: Mapping[str, str] | None = None,
) -> OpenAIAdapter:
    """Create the implemented provider adapter; unsupported providers fail explicitly."""
    if config.provider is LLMProvider.OPENAI:
        return OpenAIAdapter(config, client=client, environ=environ)
    raise AgentProviderError(f"No adapter is implemented for provider {config.provider.value!r}")
