from __future__ import annotations

from common.comparison_config import Strictness
from common.comparison_result import ComparisonType, Difference
from jobs.runner import ComparisonJobRunner, JobStatus
from ui.app import (
    _COMPARISON_LEVEL_OPTIONS,
    _comparison_config,
    _export_callable,
    _record_summary_value,
)


def test_ui_config_applies_selected_levels_and_preset() -> None:
    config = _comparison_config(
        ["Raw bytes", "Records", "Reconciliation"],
        "lenient",
        record_key=" id, account ",
        currency_field="currency",
        amount_field="amount",
    )

    assert config.levels.raw is True
    assert config.levels.records is True
    assert config.levels.schema_comparison is False
    assert config.levels.metadata is False
    assert config.strictness is Strictness.LENIENT
    assert config.null_equals_empty is True
    assert config.record_key == ["id", "account"]
    assert config.reconciliation_rules.currency_field == "currency"
    assert config.reconciliation_rules.amount_field == "amount"


def test_ui_exposes_all_comparison_levels_and_record_keys_as_xml_attributes() -> None:
    assert _COMPARISON_LEVEL_OPTIONS == (
        "Raw bytes",
        "Metadata",
        "XML structure",
        "Records",
        "Fields",
        "Financial",
        "Reconciliation",
    )

    config = _comparison_config(["Records", "Fields"], "strict", record_key="id")
    assert config.levels.records is True
    assert config.levels.fields is True
    assert config.record_key == ["id"]


def test_record_summary_says_not_run_when_record_level_is_disabled() -> None:
    summary = {
        "records_selected": False,
        "inserted": 0,
        "updated": 0,
        "deleted": 0,
    }

    assert _record_summary_value(summary, "inserted") == "not run"
    assert _record_summary_value(summary, "updated") == "not run"
    assert _record_summary_value(summary, "deleted") == "not run"


def test_deferred_exports_work_from_a_finished_job_after_rerun(tmp_path, monkeypatch) -> None:
    runner = ComparisonJobRunner(tmp_path / "jobs", max_workers=1)
    monkeypatch.setattr("ui.app._runner", lambda: runner)
    job_id = runner.submit(
        lambda context: context.store.insert_differences(
            [
                Difference(
                    comparison_type=ComparisonType.RAW,
                    location="byte=1",
                    message="changed",
                )
            ]
        )
    )
    try:
        snapshot = runner.wait(job_id, timeout=5)
        assert snapshot.status is JobStatus.COMPLETED

        csv_data = _export_callable(job_id, "csv", None)()
        jsonl_data = _export_callable(job_id, "jsonl", "AI-generated")()

        assert isinstance(csv_data, str)
        assert "finding_id,comparison_type" in csv_data
        assert "changed" in csv_data
        assert isinstance(jsonl_data, str)
        assert '"record_type":"comparison"' in jsonl_data
        assert '"explanation_label":"AI-generated"' in jsonl_data
        assert '"message":"changed"' in jsonl_data
    finally:
        runner.shutdown()
