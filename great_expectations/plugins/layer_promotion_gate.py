"""
Quality Gate Plugin: Controls promotion between medallion layers.
A layer only promotes if ALL quality checks pass.
"""
import json
import os
from datetime import datetime


class LayerPromotionGate:
    """Enforces quality gates between Bronze, Silver, and Gold layers."""

    THRESHOLDS = {
        "bronze_to_silver": {
            "min_success_rate": 0.95,
            "max_null_key_pct": 0.01,
            "min_row_count": 1
        },
        "silver_to_gold": {
            "min_success_rate": 0.99,
            "max_null_key_pct": 0.0,
            "min_row_count": 1,
            "require_hash_uniqueness": True
        }
    }

    def __init__(self, gate_name):
        if gate_name not in self.THRESHOLDS:
            raise ValueError(
                f"Unknown gate: {gate_name}. "
                f"Valid: {list(self.THRESHOLDS.keys())}"
            )
        self.gate_name = gate_name
        self.thresholds = self.THRESHOLDS[gate_name]
        self.results = []

    def check_success_rate(self, passed, total):
        """Check if validation success rate meets threshold."""
        rate = passed / total if total > 0 else 0
        result = {
            "check": "success_rate",
            "value": rate,
            "threshold": self.thresholds["min_success_rate"],
            "passed": rate >= self.thresholds["min_success_rate"]
        }
        self.results.append(result)
        return result["passed"]

    def check_null_keys(self, null_count, total_count):
        """Check if null key percentage is within threshold."""
        pct = null_count / total_count if total_count > 0 else 0
        result = {
            "check": "null_key_percentage",
            "value": pct,
            "threshold": self.thresholds["max_null_key_pct"],
            "passed": pct <= self.thresholds["max_null_key_pct"]
        }
        self.results.append(result)
        return result["passed"]

    def check_row_count(self, count):
        """Verify minimum row count."""
        result = {
            "check": "row_count",
            "value": count,
            "threshold": self.thresholds["min_row_count"],
            "passed": count >= self.thresholds["min_row_count"]
        }
        self.results.append(result)
        return result["passed"]

    def evaluate_gate(self):
        """Return overall gate decision."""
        all_passed = all(r["passed"] for r in self.results)
        return {
            "gate": self.gate_name,
            "decision": "PROMOTE" if all_passed else "BLOCK",
            "timestamp": datetime.now(tz=None).isoformat(),
            "checks": self.results
        }

    def save_report(self, output_dir="reports"):
        """Save gate evaluation report to JSON."""
        os.makedirs(output_dir, exist_ok=True)
        report = self.evaluate_gate()
        fname = (f"{self.gate_name}_{report['timestamp'][:10]}"
                 f".json")
        path = os.path.join(output_dir, fname)
        with open(path, "w") as f:
            json.dump(report, f, indent=2)
        return path
