from __future__ import annotations

from common.comparison_config import Strictness
from ui.app import _comparison_config


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
