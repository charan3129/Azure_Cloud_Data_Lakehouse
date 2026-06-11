"""Tests for quality gate logic."""
import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from great_expectations.plugins.layer_promotion_gate import (
    LayerPromotionGate
)


class TestLayerPromotionGate:
    """Validate quality gate promotion logic."""

    def test_bronze_to_silver_promote(self):
        gate = LayerPromotionGate("bronze_to_silver")
        gate.check_success_rate(95, 100)
        gate.check_null_keys(0, 1000)
        gate.check_row_count(500)
        result = gate.evaluate_gate()
        assert result["decision"] == "PROMOTE"

    def test_bronze_to_silver_block_low_success(self):
        gate = LayerPromotionGate("bronze_to_silver")
        gate.check_success_rate(80, 100)
        gate.check_null_keys(0, 1000)
        gate.check_row_count(500)
        result = gate.evaluate_gate()
        assert result["decision"] == "BLOCK"

    def test_silver_to_gold_promote(self):
        gate = LayerPromotionGate("silver_to_gold")
        gate.check_success_rate(99, 100)
        gate.check_null_keys(0, 1000)
        gate.check_row_count(100)
        result = gate.evaluate_gate()
        assert result["decision"] == "PROMOTE"

    def test_silver_to_gold_block_null_keys(self):
        gate = LayerPromotionGate("silver_to_gold")
        gate.check_success_rate(100, 100)
        gate.check_null_keys(5, 1000)
        gate.check_row_count(100)
        result = gate.evaluate_gate()
        assert result["decision"] == "BLOCK"

    def test_silver_to_gold_block_empty(self):
        gate = LayerPromotionGate("silver_to_gold")
        gate.check_success_rate(100, 100)
        gate.check_null_keys(0, 1000)
        gate.check_row_count(0)
        result = gate.evaluate_gate()
        assert result["decision"] == "BLOCK"

    def test_invalid_gate_name(self):
        with pytest.raises(ValueError):
            LayerPromotionGate("invalid_gate")

    def test_gate_report_structure(self):
        gate = LayerPromotionGate("bronze_to_silver")
        gate.check_success_rate(100, 100)
        gate.check_null_keys(0, 500)
        gate.check_row_count(200)
        result = gate.evaluate_gate()
        assert "gate" in result
        assert "decision" in result
        assert "timestamp" in result
        assert "checks" in result
        assert len(result["checks"]) == 3

    def test_edge_case_zero_total(self):
        gate = LayerPromotionGate("bronze_to_silver")
        gate.check_success_rate(0, 0)
        gate.check_null_keys(0, 0)
        gate.check_row_count(0)
        result = gate.evaluate_gate()
        assert result["decision"] == "BLOCK"
