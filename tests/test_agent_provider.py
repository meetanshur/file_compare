from __future__ import annotations

import json
import logging
from types import SimpleNamespace

import pytest
from openai import OpenAIError
from pydantic import ValidationError

from agent.config import (
    AgentConfig,
    LLMProvider,
    load_agent_config,
    validate_agent_config,
)
from agent.openai_adapter import (
    AgentProviderError,
    LLMMessage,
    LLMProviderAdapter,
    OpenAIAdapter,
    create_llm_adapter,
)


class _FakeOpenAIClient:
    def __init__(self, response) -> None:
        self.response = response
        self.call = None
        self.chat = SimpleNamespace(
            completions=SimpleNamespace(create=self._create_completion),
        )

    def _create_completion(self, **kwargs):
        self.call = kwargs
        return self.response


class _FakeOpenAIError(OpenAIError):
    def __init__(self, message: str, status_code: int) -> None:
        super().__init__(message)
        self.status_code = status_code


class _SequencedOpenAIClient:
    def __init__(self, outcomes) -> None:
        self.outcomes = list(outcomes)
        self.calls = 0
        self.chat = SimpleNamespace(
            completions=SimpleNamespace(create=self._create_completion),
        )

    def _create_completion(self, **kwargs):
        self.calls += 1
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


def test_agent_config_loads_yaml_and_resolves_named_environment_secret(tmp_path) -> None:
    config_file = tmp_path / "agent.yaml"
    config_file.write_text(
        "provider: openai\nmodel: gpt-test\napi_key_env: TEST_LLM_KEY\ntimeout_seconds: 12\n",
        encoding="utf-8",
    )

    config = load_agent_config(config_file)
    resolved = config.resolve({"TEST_LLM_KEY": "secret-from-env"})

    assert config.provider is LLMProvider.OPENAI
    assert config.model == "gpt-test"
    assert config.timeout_seconds == 12
    assert resolved.require_api_key() == "secret-from-env"
    assert "secret-from-env" not in repr(resolved)
    assert "secret-from-env" not in json.dumps(config.model_dump(mode="json"))


def test_agent_config_supports_inline_secret_without_repr_leak() -> None:
    config = AgentConfig(api_key="sensitive-value")

    assert config.resolve({}).require_api_key() == "sensitive-value"
    assert "sensitive-value" not in repr(config)


def test_agent_config_reports_invalid_provider_fields() -> None:
    with pytest.raises(ValueError, match="unexpected"):
        validate_agent_config({"unexpected": True})
    with pytest.raises(ValueError, match="max_retries"):
        validate_agent_config({"max_retries": "many"})
    with pytest.raises(ValidationError):
        AgentConfig(timeout_seconds=0)


def test_agent_config_loads_json_and_rejects_bad_extension(tmp_path) -> None:
    config_file = tmp_path / "agent.json"
    config_file.write_text(
        json.dumps({"provider": "openai", "base_url": "https://api.example.test/v1"}),
        encoding="utf-8",
    )
    assert load_agent_config(config_file).base_url == "https://api.example.test/v1"

    unsupported = tmp_path / "agent.toml"
    unsupported.write_text("[agent]\n", encoding="utf-8")
    with pytest.raises(ValueError, match="extension"):
        load_agent_config(unsupported)


def test_openai_adapter_sends_validated_chat_completion_request() -> None:
    fake = _FakeOpenAIClient(
        SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content="Summary"))])
    )
    adapter = OpenAIAdapter(
        AgentConfig(model="gpt-test", max_output_tokens=128),
        client=fake,
        environ={"OPENAI_API_KEY": "test-key"},
    )

    response = adapter.complete([LLMMessage(role="user", content="Explain these findings")])

    assert response.content == "Summary"
    assert response.provider is LLMProvider.OPENAI
    assert response.model == "gpt-test"
    assert fake.call == {
        "model": "gpt-test",
        "messages": [{"role": "user", "content": "Explain these findings"}],
        "max_tokens": 128,
    }


def test_openai_adapter_fails_explicitly_for_missing_credentials() -> None:
    with pytest.raises(AgentProviderError, match="OPENAI_API_KEY"):
        OpenAIAdapter(AgentConfig(), environ={})


def test_openai_adapter_validates_messages_and_provider() -> None:
    fake = _FakeOpenAIClient(
        SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content="ok"))])
    )
    adapter = OpenAIAdapter(
        AgentConfig(),
        client=fake,
        environ={"OPENAI_API_KEY": "test-key"},
    )
    with pytest.raises(ValueError, match="At least one message"):
        adapter.complete([])

    with pytest.raises(AgentProviderError, match="cannot serve"):
        OpenAIAdapter(
            AgentConfig(provider=LLMProvider.ANTHROPIC),
            client=fake,
            environ={"OPENAI_API_KEY": "test-key"},
        )
    with pytest.raises(AgentProviderError, match="No adapter is implemented"):
        create_llm_adapter(AgentConfig(provider=LLMProvider.LOCAL))


def test_provider_interface_is_vendor_independent() -> None:
    fake = _FakeOpenAIClient(
        SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content="ok"))])
    )
    adapter: LLMProviderAdapter = create_llm_adapter(
        AgentConfig(),
        client=fake,
        environ={"OPENAI_API_KEY": "test-key"},
    )

    assert adapter.complete([LLMMessage(role="user", content="small summary")]).content == "ok"


def test_provider_rejects_text_over_configured_limit_without_calling_client() -> None:
    fake = _FakeOpenAIClient(
        SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content="unused"))])
    )
    adapter = OpenAIAdapter(
        AgentConfig(max_request_text_chars=5),
        client=fake,
        environ={"OPENAI_API_KEY": "test-key"},
    )

    with pytest.raises(AgentProviderError, match="Request text"):
        adapter.complete([LLMMessage(role="user", content="six chars")])

    assert fake.call is None


def test_provider_accepts_request_at_configured_text_limit() -> None:
    fake = _FakeOpenAIClient(
        SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content="ok"))])
    )
    adapter = OpenAIAdapter(
        AgentConfig(max_request_text_chars=5),
        client=fake,
        environ={"OPENAI_API_KEY": "test-key"},
    )

    assert adapter.complete([LLMMessage(role="user", content="12345")]).content == "ok"


def test_openai_client_receives_configured_timeout_and_disables_sdk_retries(
    monkeypatch,
) -> None:
    fake = _FakeOpenAIClient(
        SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content="ok"))])
    )
    client_options = {}

    def create_client(**kwargs):
        client_options.update(kwargs)
        return fake

    monkeypatch.setattr("agent.openai_adapter.OpenAI", create_client)
    OpenAIAdapter(
        AgentConfig(timeout_seconds=12.5),
        environ={"OPENAI_API_KEY": "test-key"},
    )

    assert client_options["timeout"] == 12.5
    assert client_options["max_retries"] == 0


def test_openai_adapter_retries_rate_limit_and_timeout_with_backoff() -> None:
    success = SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content="ok"))])
    fake = _SequencedOpenAIClient(
        [
            _FakeOpenAIError("rate limit", 429),
            TimeoutError("transient timeout"),
            success,
        ]
    )
    delays = []
    adapter = OpenAIAdapter(
        AgentConfig(max_retries=2),
        client=fake,
        environ={"OPENAI_API_KEY": "test-key"},
        sleep=delays.append,
    )

    response = adapter.complete([LLMMessage(role="user", content="hello")])

    assert response.content == "ok"
    assert fake.calls == 3
    assert delays == [0.5, 1.0]


def test_provider_errors_redact_api_key_from_exception_and_logs(caplog) -> None:
    secret = "sk-test-never-log-this"
    fake = _SequencedOpenAIClient([_FakeOpenAIError(f"request contained {secret}", 401)])
    adapter = OpenAIAdapter(
        AgentConfig(),
        client=fake,
        environ={"OPENAI_API_KEY": secret},
        sleep=lambda _delay: None,
    )

    with caplog.at_level(logging.DEBUG):
        with pytest.raises(AgentProviderError, match="rejected") as error:
            adapter.complete([LLMMessage(role="user", content="hello")])

    assert secret not in str(error.value)
    assert secret not in repr(adapter.config)
    assert secret not in caplog.text


def test_transient_provider_error_message_is_redacted() -> None:
    secret = "sk-test-never-leak"
    fake = _SequencedOpenAIClient([_FakeOpenAIError(f"transport echoed {secret}", 503)])
    adapter = OpenAIAdapter(
        AgentConfig(max_retries=0),
        client=fake,
        environ={"OPENAI_API_KEY": secret},
    )

    with pytest.raises(AgentProviderError, match="details were redacted") as error:
        adapter.complete([LLMMessage(role="user", content="hello")])

    assert secret not in str(error.value)


@pytest.mark.parametrize(
    "response",
    [
        SimpleNamespace(choices=[]),
        SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=None))]),
    ],
)
def test_openai_adapter_rejects_empty_response_shapes(response) -> None:
    fake = _FakeOpenAIClient(response)
    adapter = OpenAIAdapter(
        AgentConfig(),
        client=fake,
        environ={"OPENAI_API_KEY": "test-key"},
    )

    with pytest.raises(AgentProviderError, match="OpenAI returned"):
        adapter.complete([LLMMessage(role="user", content="hello")])
