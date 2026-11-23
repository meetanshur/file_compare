"""Minimal Streamlit interface for background file comparisons."""

from __future__ import annotations

import os
import tempfile
import time
from collections.abc import Callable
from pathlib import Path
from uuid import UUID

import streamlit as st

from agent import AgentConfig, ComparisonAgent, ComparisonSource, create_llm_adapter
from agent.openai_adapter import AgentProviderError, LLMMessage, LLMResponse
from common.comparison_config import (
    ComparisonConfig,
    ComparisonLevels,
    FinancialRules,
    ReconciliationRules,
    Strictness,
)
from common.comparison_result import ComparisonType
from connectors.local import LocalFileConnector
from connectors.s3 import S3Connector
from jobs.runner import ComparisonJobRunner, JobStatus
from reports.exports import iter_findings_csv, iter_findings_jsonl
from storage.duckdb_store import DuckDBStore

_PAGE_SIZE = 50
_COMPARISON_LEVEL_OPTIONS = (
    "Raw bytes",
    "Metadata",
    "XML structure",
    "Records",
    "Fields",
    "Financial",
    "Reconciliation",
)


class _UnavailableProvider:
    def complete(self, messages: list[LLMMessage]) -> LLMResponse:
        raise AgentProviderError("LLM explanation unavailable: API key is not configured.")


@st.cache_resource
def _runner() -> ComparisonJobRunner:
    database_directory = Path.home() / ".file_compare" / "jobs"
    return ComparisonJobRunner(database_directory)


def _provider():
    config = AgentConfig()
    if not os.environ.get(config.api_key_env):
        return _UnavailableProvider()
    return create_llm_adapter(config)


def _source(uri: str) -> ComparisonSource:
    connector = S3Connector() if uri.lower().startswith("s3://") else LocalFileConnector()
    return ComparisonSource.from_connector(connector, uri)


def _comparison_config(
    level_names: list[str],
    preset: str,
    *,
    record_key: str = "",
    currency_field: str = "",
    amount_field: str = "",
    debit_field: str = "",
    credit_field: str = "",
) -> ComparisonConfig:
    levels = ComparisonLevels(
        raw="Raw bytes" in level_names,
        schema="XML structure" in level_names,
        metadata="Metadata" in level_names,
        records="Records" in level_names,
        fields="Fields" in level_names,
        financial="Financial" in level_names,
        reconciliation="Reconciliation" in level_names,
    )
    currency = currency_field.strip() or None
    amount = amount_field.strip() or None
    debit = debit_field.strip() or None
    credit = credit_field.strip() or None
    reconciliation = (
        ReconciliationRules(
            currency_field=currency,
            amount_field=amount,
            debit_field=debit,
            credit_field=credit,
        )
        if currency and any((amount, debit, credit))
        else ReconciliationRules()
    )
    return ComparisonConfig(
        levels=levels,
        strictness=Strictness(preset),
        record_key=[field.strip() for field in record_key.split(",") if field.strip()],
        financial_rules=FinancialRules(
            currency_field=currency,
            amount_field=amount,
        ),
        reconciliation_rules=reconciliation,
    )


def _database_path(job_id: UUID) -> Path:
    return _runner().database_directory / f"{job_id}.duckdb"


def _record_summary_value(summary: dict[str, int | bool], key: str) -> int | str:
    return summary.get(key, 0) if summary.get("records_selected", False) else "not run"


def _export_data(
    job_id: UUID,
    export_format: str,
    explanation_label: str | None,
) -> str:
    with tempfile.SpooledTemporaryFile(
        max_size=1024 * 1024,
        mode="w+t",
        encoding="utf-8",
        newline="",
    ) as output:
        with DuckDBStore(_database_path(job_id)) as store:
            rows = (
                iter_findings_csv(store, explanation_label=explanation_label)
                if export_format == "csv"
                else iter_findings_jsonl(store, explanation_label=explanation_label)
            )
            for row in rows:
                output.write(row)
        output.seek(0)
        return output.read()


def _export_callable(
    job_id: UUID,
    export_format: str,
    explanation_label: str | None,
) -> Callable[[], str]:
    return lambda: _export_data(job_id, export_format, explanation_label)


def _reset_findings_page() -> None:
    st.session_state["finding_page"] = 1


def _render_results(job_id: UUID, snapshot) -> None:
    result = snapshot.result
    if result is None:
        st.error("The job completed without a comparison result.")
        return

    st.subheader("Summary")
    columns = st.columns(4)
    columns[0].metric("Findings", result.summary.get("total_findings", 0))
    columns[1].metric("Inserted", _record_summary_value(result.summary, "inserted"))
    columns[2].metric("Updated", _record_summary_value(result.summary, "updated"))
    columns[3].metric("Deleted", _record_summary_value(result.summary, "deleted"))
    if result.truncated:
        st.warning("Comparison was truncated at the configured finding limit.")

    if result.explanation is not None:
        st.subheader("AI-generated explanation")
        st.caption("AI-generated")
        st.write(result.explanation.summary)
        st.write(f"Severity: {result.explanation.severity.value}")
        st.write(f"Likely root cause: {result.explanation.likely_root_cause}")
        st.write(f"Cited finding IDs: {result.explanation.cited_finding_ids}")
    elif result.explanation_status.value == "unavailable":
        st.info("AI-generated explanation is unavailable; deterministic results are available.")

    for warning in result.warnings:
        st.warning(warning)

    with DuckDBStore(_database_path(job_id)) as store:
        type_names = ["All types", *(item.value for item in ComparisonType)]
        selected_type = st.selectbox(
            "Filter by comparison type",
            type_names,
            key="finding_type",
            on_change=_reset_findings_page,
        )
        comparison_type = None if selected_type == "All types" else ComparisonType(selected_type)
        total = store.count_differences(comparison_type)
        page_count = max(1, (total + _PAGE_SIZE - 1) // _PAGE_SIZE)
        if st.session_state.get("finding_page", 1) > page_count:
            st.session_state["finding_page"] = page_count
        page = st.number_input(
            "Findings page",
            min_value=1,
            max_value=page_count,
            value=1,
            step=1,
            key="finding_page",
        )
        findings = list(
            store.iter_differences_with_ids(
                comparison_type=comparison_type,
                offset=(page - 1) * _PAGE_SIZE,
                limit=_PAGE_SIZE,
            )
        )
        st.caption(f"Showing {len(findings)} of {total} findings")
        if findings:
            st.dataframe(
                [
                    {
                        "ID": finding_id,
                        "Type": finding.comparison_type.value,
                        "Severity": finding.severity.value,
                        "Location": finding.location,
                        "Finding": finding.message,
                        "A": finding.value_a,
                        "B": finding.value_b,
                    }
                    for finding_id, finding in findings
                ],
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("No findings match this filter.")

        explanation_label = result.explanation.label if result.explanation is not None else None
        st.download_button(
            "Download findings CSV",
            data=_export_callable(
                job_id,
                "csv",
                explanation_label,
            ),
            file_name=f"{job_id}-findings.csv",
            mime="text/csv",
        )
        st.download_button(
            "Download findings JSONL",
            data=_export_callable(
                job_id,
                "jsonl",
                explanation_label,
            ),
            file_name=f"{job_id}-findings.jsonl",
            mime="application/x-ndjson",
        )


def main() -> None:
    st.set_page_config(page_title="Dynamic File Comparison", layout="wide")
    st.title("Dynamic File Comparison")
    st.caption("Compare local files or S3 objects; paths are read by the Streamlit host.")

    with st.form("comparison"):
        source_a_uri = st.text_input("File A path or S3 URI")
        source_b_uri = st.text_input("File B path or S3 URI")
        selected_levels = st.multiselect(
            "Comparison levels",
            list(_COMPARISON_LEVEL_OPTIONS),
            default=["Raw bytes", "Metadata", "XML structure"],
        )
        preset = st.selectbox("Comparison preset", ["strict", "lenient"])
        record_key = st.text_input("Record key fields (comma-separated, if needed)")
        currency_field = st.text_input("Currency field (if needed)")
        amount_field = st.text_input("Amount field (if needed)")
        debit_field = st.text_input("Debit field (if needed)")
        credit_field = st.text_input("Credit field (if needed)")
        submitted = st.form_submit_button("Start comparison")

    if submitted:
        if not source_a_uri.strip() or not source_b_uri.strip():
            st.error("Enter both File A and File B paths or S3 URIs.")
        else:
            try:
                source_a = _source(source_a_uri.strip())
                source_b = _source(source_b_uri.strip())
                comparison_config = _comparison_config(
                    selected_levels,
                    preset,
                    record_key=record_key,
                    currency_field=currency_field,
                    amount_field=amount_field,
                    debit_field=debit_field,
                    credit_field=credit_field,
                )
                agent = ComparisonAgent(_provider())
                job_id = agent.submit(
                    _runner(),
                    source_a,
                    source_b,
                    comparison_config=comparison_config,
                )
                st.session_state["comparison_job_id"] = str(job_id)
                st.session_state.pop("finding_page", None)
            except Exception as exc:
                st.error(f"Could not start comparison: {exc}")

    job_id_text = st.session_state.get("comparison_job_id")
    if not job_id_text:
        return
    job_id = UUID(job_id_text)
    try:
        snapshot = _runner().get_job(job_id)
    except Exception as exc:
        st.error(f"Could not read comparison job status: {exc}")
        return

    if snapshot.status in {JobStatus.PENDING, JobStatus.RUNNING}:
        st.subheader("Comparison progress")
        if snapshot.percent is not None:
            st.progress(snapshot.percent / 100)
            st.caption(f"{snapshot.percent:.0f}% complete")
        else:
            st.progress(0.0, text="Comparison is running")
        if snapshot.rows_processed and snapshot.total_rows:
            st.caption(f"{snapshot.rows_processed} / {snapshot.total_rows} stages")
        if st.button("Cancel comparison"):
            _runner().cancel(job_id)
            st.info("Cancellation requested.")
        time.sleep(0.5)
        st.rerun()
    elif snapshot.status is JobStatus.FAILED:
        st.error(f"Comparison failed: {snapshot.error or 'unknown error'}")
    elif snapshot.status is JobStatus.CANCELLED:
        st.warning("Comparison was cancelled.")
    elif snapshot.status in {JobStatus.COMPLETED, JobStatus.TRUNCATED}:
        _render_results(job_id, snapshot)


if __name__ == "__main__":
    main()
