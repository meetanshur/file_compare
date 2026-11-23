from __future__ import annotations

import json
from dataclasses import dataclass
from io import BytesIO

import pytest

from agent.comparison import (
    ComparisonAgent,
    ComparisonSource,
    ExplanationStatus,
    ProposalStatus,
    apply_config_proposal,
)
from agent.config import AgentConfig, LLMProvider
from agent.openai_adapter import AgentProviderError, LLMMessage, LLMResponse
from common.comparison_config import (
    ComparisonConfig,
    ComparisonLevels,
    FinancialRules,
    ReconciliationRules,
)
from connectors.base import SourceMetadata
from jobs.runner import ComparisonJobRunner, JobStatus
from parsers.detection import FileFormat, detect_file
from storage.duckdb_store import DuckDBStore


class _FakeProvider:
    def __init__(self, *responses: str) -> None:
        self.responses = list(responses)
        self.messages: list[LLMMessage] = []

    def complete(self, messages):
        self.messages.extend(messages)
        content = self.responses.pop(0)
        return LLMResponse(
            content=content,
            provider=LLMProvider.OPENAI,
            model="fake-model",
        )


class _FailingProvider:
    def complete(self, messages):
        raise AgentProviderError("fake provider failure")


@dataclass
class _JobContext:
    config: ComparisonConfig
    store: DuckDBStore

    @staticmethod
    def raise_if_cancelled() -> None:
        return None


def _source(uri: str, name: str, payload: bytes) -> ComparisonSource:
    metadata = SourceMetadata(
        uri=uri,
        source_type="test",
        name=name,
        size_bytes=len(payload),
    )
    return ComparisonSource(metadata, lambda: BytesIO(payload))


def _levels(**enabled: bool) -> ComparisonLevels:
    values = {
        "raw": False,
        "schema": False,
        "metadata": False,
        "records": False,
        "fields": False,
        "financial": False,
        "reconciliation": False,
    }
    values.update(enabled)
    return ComparisonLevels.model_validate(values)


def _explanation(*, finding_ids: list[int] | None = None) -> str:
    return json.dumps(
        {
            "summary": "The deterministic comparison found differences.",
            "severity": "warning",
            "likely_root_cause": "A source value changed.",
            "cited_finding_ids": finding_ids or [],
        }
    )


def test_tool_selection_is_deterministic_from_levels_and_detected_formats() -> None:
    sample = b"id,currency,amount\n1,USD,1.00\n"
    detected_a = detect_file(sample, "a.csv")
    detected_b = detect_file(sample, "b.csv")
    config = ComparisonConfig(
        levels=_levels(
            raw=True,
            schema=True,
            metadata=True,
            records=True,
            fields=True,
            financial=True,
            reconciliation=True,
        ),
        record_key=["id"],
        financial_rules=FinancialRules(currency_field="currency", amount_field="amount"),
        reconciliation_rules=ReconciliationRules(
            currency_field="currency",
            amount_field="amount",
        ),
    )

    tools = ComparisonAgent.selected_tools(config, detected_a, detected_b)

    assert detected_a.file_format is FileFormat.CSV
    assert tools == (
        "detect_format",
        "sample_file",
        "run_raw_comparison",
        "run_metadata_comparison",
        "run_record_comparison",
        "run_field_comparison",
        "run_financial_comparison",
        "run_reconciliation_comparison",
        "read_findings",
    )


def test_comparison_proposal_is_pending_until_explicit_approval(tmp_path) -> None:
    provider = _FakeProvider(
        _explanation(finding_ids=[1]),
        json.dumps(
            {
                "record_key": ["id"],
                "financial_rules": None,
                "reconciliation_rules": None,
                "ignore_fields": ["timestamp"],
            }
        ),
    )
    agent = ComparisonAgent(
        provider,
        config=AgentConfig(enable_config_proposals=True),
    )
    comparison_config = ComparisonConfig(
        levels=_levels(raw=True),
        max_total_differences=100,
    )
    source_a = _source("file:///a.csv", "a.csv", b"id,value\n1,old\n")
    source_b = _source("file:///b.csv", "b.csv", b"id,value\n1,new\n")

    with DuckDBStore(tmp_path / "agent.duckdb", max_total_differences=100) as store:
        result = agent.compare(_JobContext(comparison_config, store), source_a, source_b)

    assert result.explanation_status is ExplanationStatus.AVAILABLE
    assert result.explanation is not None
    assert result.explanation.label == "AI-generated"
    assert result.explanation.cited_finding_ids == [1]
    assert result.config_proposal is not None
    assert result.proposal_status is ProposalStatus.PENDING_APPROVAL
    assert "propose_config" in result.tool_names
    assert comparison_config.record_key == []
    with pytest.raises(PermissionError, match="explicit user approval"):
        apply_config_proposal(comparison_config, result.config_proposal)

    approved = apply_config_proposal(
        comparison_config,
        result.config_proposal,
        approved=True,
    )
    assert approved.record_key == ["id"]
    assert approved.ignore_fields == ["timestamp"]
    assert comparison_config.record_key == []


def test_llm_failure_keeps_deterministic_comparison_result(tmp_path) -> None:
    agent = ComparisonAgent(
        _FailingProvider(),
        config=AgentConfig(enable_config_proposals=False),
    )
    comparison_config = ComparisonConfig(levels=_levels(raw=True))
    source_a = _source("file:///a.txt", "a.txt", b"before")
    source_b = _source("file:///b.txt", "b.txt", b"after")

    with DuckDBStore(tmp_path / "fallback.duckdb") as store:
        result = agent.compare(_JobContext(comparison_config, store), source_a, source_b)
        findings = list(store.iter_differences_with_ids())

    assert result.summary["total_findings"] == 1
    assert len(findings) == 1
    assert result.explanation is None
    assert result.explanation_status is ExplanationStatus.UNAVAILABLE
    assert result.explanation_error == "provider_AgentProviderError"


def test_invalid_or_unverifiable_citations_make_explanation_unavailable(tmp_path) -> None:
    provider = _FakeProvider(_explanation(finding_ids=[999]))
    agent = ComparisonAgent(provider, config=AgentConfig(enable_config_proposals=False))
    comparison_config = ComparisonConfig(levels=_levels(raw=True))
    source_a = _source("file:///a.txt", "a.txt", b"one")
    source_b = _source("file:///b.txt", "b.txt", b"two")

    with DuckDBStore(tmp_path / "citation.duckdb") as store:
        result = agent.compare(_JobContext(comparison_config, store), source_a, source_b)

    assert result.summary["total_findings"] > 0
    assert result.explanation_status is ExplanationStatus.UNAVAILABLE
    assert result.explanation is None


def test_model_input_samples_are_bounded_and_digit_masked(tmp_path) -> None:
    sample_payload = b"account,amount\n123456789,98765.43\n"
    provider = _FakeProvider(
        _explanation(),
        json.dumps({"record_key": ["account"], "ignore_fields": []}),
    )
    agent = ComparisonAgent(
        provider,
        config=AgentConfig(
            enable_config_proposals=True,
            max_sample_records=1,
            max_sample_chars=120,
            max_request_text_chars=2_000,
            max_explanation_chars=512,
        ),
    )
    detected = detect_file(sample_payload, "sample.csv")

    sample = agent.sample_file(
        _source("file:///sample.csv", "sample.csv", sample_payload),
        detected,
        max_records=1,
        max_characters=120,
    )

    assert len(sample) == 1
    assert len(json.dumps(sample, separators=(",", ":"))) <= 120
    assert "123456789" not in json.dumps(sample)
    assert "98765.43" not in json.dumps(sample)


def test_provider_prompt_limits_config_proposal_sample_size(tmp_path) -> None:
    sample_payload = b"id,account,amount\n123456789,987654321,999999999999999999999999\n"
    provider = _FakeProvider(
        _explanation(),
        json.dumps({"record_key": ["id"], "ignore_fields": []}),
    )
    agent = ComparisonAgent(
        provider,
        config=AgentConfig(
            enable_config_proposals=True,
            max_sample_records=1,
            max_sample_chars=100,
            max_explanation_chars=512,
            max_request_text_chars=2_000,
        ),
    )
    comparison_config = ComparisonConfig(levels=_levels())

    with DuckDBStore(tmp_path / "sample-size.duckdb") as store:
        agent.compare(
            _JobContext(comparison_config, store),
            _source("file:///a.csv", "a.csv", sample_payload),
            _source("file:///b.csv", "b.csv", sample_payload),
        )

    proposal_payload = json.loads(provider.messages[1].content.split("\n", 1)[1])
    assert len(json.dumps(proposal_payload["sample_records"], separators=(",", ":"))) <= 100
    assert "123456789" not in provider.messages[1].content
    assert "999999999999999999999999" not in provider.messages[1].content


def test_explanation_context_is_bounded_and_masks_finding_values(tmp_path) -> None:
    provider = _FakeProvider(_explanation(finding_ids=[1]))
    agent = ComparisonAgent(
        provider,
        config=AgentConfig(
            enable_config_proposals=False,
            max_explanation_findings=1,
            max_explanation_chars=512,
            max_request_text_chars=2_000,
        ),
    )
    comparison_config = ComparisonConfig(levels=_levels(raw=True))

    with DuckDBStore(tmp_path / "explanation-size.duckdb") as store:
        result = agent.compare(
            _JobContext(comparison_config, store),
            _source("file:///a.txt", "a.txt", b"account 123456"),
            _source("file:///b.txt", "b.txt", b"account 654321"),
        )

    message = provider.messages[0].content
    payload = json.loads(message.split("\n", 1)[1])
    assert result.explanation_status is ExplanationStatus.AVAILABLE
    assert len(message) <= 512
    assert len(json.dumps(payload, separators=(",", ":"))) <= 512
    assert "123456" not in message
    assert "654321" not in message


def test_explanation_is_unavailable_when_no_finding_ids_fit_the_bound(tmp_path) -> None:
    provider = _FakeProvider(_explanation())
    agent = ComparisonAgent(
        provider,
        config=AgentConfig(
            enable_config_proposals=False,
            max_explanation_findings=0,
        ),
    )
    comparison_config = ComparisonConfig(levels=_levels(raw=True))

    with DuckDBStore(tmp_path / "uncitable.duckdb") as store:
        result = agent.compare(
            _JobContext(comparison_config, store),
            _source("file:///a.txt", "a.txt", b"alpha"),
            _source("file:///b.txt", "b.txt", b"bravo"),
        )

    assert result.summary["total_findings"] > 0
    assert result.explanation is None
    assert result.explanation_status is ExplanationStatus.UNAVAILABLE
    assert result.explanation_error == "no_citable_findings"
    assert provider.messages == []


def test_minimum_explanation_character_limit_can_send_a_summary(tmp_path) -> None:
    provider = _FakeProvider(_explanation())
    agent = ComparisonAgent(
        provider,
        config=AgentConfig(
            enable_config_proposals=False,
            max_explanation_chars=256,
            max_request_text_chars=1_000,
        ),
    )
    comparison_config = ComparisonConfig(levels=_levels(raw=True))

    with DuckDBStore(tmp_path / "minimum-explanation.duckdb") as store:
        result = agent.compare(
            _JobContext(comparison_config, store),
            _source("file:///a.txt", "a.txt", b"identical"),
            _source("file:///b.txt", "b.txt", b"identical"),
        )

    assert result.explanation_status is ExplanationStatus.AVAILABLE
    assert len(provider.messages[0].content) <= 256


def test_job_runner_preserves_truncated_comparison_result(tmp_path) -> None:
    provider = _FakeProvider(_explanation(finding_ids=[1]))
    agent = ComparisonAgent(
        provider,
        config=AgentConfig(enable_config_proposals=False),
    )
    comparison_config = ComparisonConfig(
        levels=_levels(raw=True),
        max_total_differences=1,
    )

    with ComparisonJobRunner(tmp_path / "jobs", max_workers=1, max_active_jobs=1) as runner:
        job_id = agent.submit(
            runner,
            _source("file:///a.txt", "a.txt", b"before"),
            _source("file:///b.txt", "b.txt", b"after"),
            comparison_config=comparison_config,
        )
        snapshot = runner.wait(job_id, timeout=20)

    assert snapshot.status is JobStatus.TRUNCATED
    assert snapshot.result.truncated is True
    assert snapshot.result.summary["total_findings"] == 1
