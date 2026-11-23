"""OpenAI Chat Completions adapter with injectable client support."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from openai import APIConnectionError, APIStatusError, APITimeoutError, OpenAI, OpenAIError

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


class LLMProviderAdapter(Protocol):
    """Provider-independent completion interface for agent orchestration."""

    def complete(self, messages: Sequence[LLMMessage]) -> LLMResponse: ...


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
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if config.provider is not LLMProvider.OPENAI:
            raise AgentProviderError(
                f"OpenAIAdapter cannot serve provider {config.provider.value!r}"
            )
        self.config = config
        self._sleep = sleep
        resolved = config.resolve(environ)
        try:
            self._api_key = resolved.require_api_key()
        except ValueError as exc:
            raise AgentProviderError(str(exc)) from exc
        self._client = client if client is not None else self._create_client()

    def _create_client(self) -> _OpenAIClient:
        options: dict[str, object] = {
            "api_key": self._api_key,
            "timeout": self.config.timeout_seconds,
            "max_retries": 0,
        }
        if self.config.base_url is not None:
            options["base_url"] = self.config.base_url
        return OpenAI(**options)

    def complete(self, messages: Sequence[LLMMessage]) -> LLMResponse:
        """Request one assistant response and fail clearly for malformed responses."""
        if not messages:
            raise ValueError("At least one message is required")
        text_characters = sum(len(message.content) for message in messages)
        if text_characters > self.config.max_request_text_chars:
            raise AgentProviderError(
                f"Request text is {text_characters} characters; configured limit is "
                f"{self.config.max_request_text_chars}"
            )
        payload = [{"role": message.role, "content": message.content} for message in messages]
        response = self._request_with_retries(payload)

        choices = getattr(response, "choices", None)
        if not choices:
            raise AgentProviderError("OpenAI returned no completion choices")
        message = getattr(choices[0], "message", None)
        content = getattr(message, "content", None)
        if not isinstance(content, str):
            raise AgentProviderError("OpenAI returned an empty or non-text response")
        return LLMResponse(content=content, provider=LLMProvider.OPENAI, model=self.config.model)

    def _request_with_retries(self, payload: list[dict[str, str]]) -> Any:
        for attempt in range(self.config.max_retries + 1):
            try:
                return self._client.chat.completions.create(
                    model=self.config.model,
                    messages=payload,
                    max_tokens=self.config.max_output_tokens,
                )
            except (OpenAIError, TimeoutError, ConnectionError) as exc:
                if _is_invalid_credential(exc):
                    raise AgentProviderError(
                        "OpenAI rejected the configured credentials; verify the API key."
                    ) from None
                if attempt >= self.config.max_retries or not _is_retryable(exc):
                    raise AgentProviderError(
                        "OpenAI request failed after configured retries; provider details "
                        "were redacted."
                    ) from None
                self._sleep(min(0.5 * (2**attempt), 8.0))
        raise AgentProviderError("OpenAI request failed; provider details were redacted.")


def create_llm_adapter(
    config: AgentConfig,
    *,
    client: _OpenAIClient | None = None,
    environ: Mapping[str, str] | None = None,
) -> LLMProviderAdapter:
    """Create the implemented provider adapter; unsupported providers fail explicitly."""
    if config.provider is LLMProvider.OPENAI:
        return OpenAIAdapter(config, client=client, environ=environ)
    raise AgentProviderError(f"No adapter is implemented for provider {config.provider.value!r}")


def _status_code(error: BaseException) -> int | None:
    status_code = getattr(error, "status_code", None)
    return status_code if isinstance(status_code, int) else None


def _is_invalid_credential(error: BaseException) -> bool:
    return _status_code(error) in {401, 403}


def _is_retryable(error: BaseException) -> bool:
    if isinstance(error, (APIConnectionError, APITimeoutError, TimeoutError, ConnectionError)):
        return True
    status_code = _status_code(error)
    if isinstance(error, APIStatusError):
        return status_code in {408, 409, 429} or status_code is not None and status_code >= 500
    return status_code in {408, 409, 429} or status_code is not None and status_code >= 500
