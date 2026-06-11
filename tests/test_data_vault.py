"""Tests for Data Vault transformation logic."""
import hashlib
import json
import os
import pytest


def md5_hash(*values):
    """Replicate the hash key generation from bronze_to_silver notebook."""
    concat = "||".join(str(v).upper().strip() for v in values)
    return hashlib.md5(concat.encode()).hexdigest()


def md5_hash_diff(*values):
    """Replicate hash diff generation for satellite change detection."""
    concat = "||".join(str(v) if v is not None else "" for v in values)
    return hashlib.md5(concat.encode()).hexdigest()


class TestHashKeyGeneration:
    """Validate Data Vault hash key generation."""

    def test_single_key_hash(self):
        hk = md5_hash("CUST-10001")
        assert len(hk) == 32
        assert hk == md5_hash("CUST-10001")  # deterministic

    def test_composite_key_hash(self):
        hk = md5_hash("ORD-001", "CUST-10001")
        assert len(hk) == 32
        hk2 = md5_hash("ORD-001", "CUST-10002")
        assert hk != hk2  # different inputs → different hashes

    def test_hash_case_insensitive(self):
        assert md5_hash("cust-10001") == md5_hash("CUST-10001")

    def test_hash_trims_whitespace(self):
        assert md5_hash("  CUST-10001  ") == md5_hash("CUST-10001")

    def test_hash_diff_detects_changes(self):
        hd1 = md5_hash_diff("Alice", "Johnson", "alice@test.com")
        hd2 = md5_hash_diff("Alice", "Smith", "alice@test.com")
        assert hd1 != hd2

    def test_hash_diff_handles_nulls(self):
        hd = md5_hash_diff("Alice", None, "alice@test.com")
        assert len(hd) == 32


class TestDataVaultStructure:
    """Validate Data Vault model relationships."""

    def test_hub_customer_key_from_business_key(self):
        customer_id = "CUST-10001"
        hk = md5_hash(customer_id)
        assert isinstance(hk, str)
        assert len(hk) == 32

    def test_link_key_from_hub_keys(self):
        order_id = "ORD-001"
        customer_id = "CUST-10001"
        lnk_hk = md5_hash(order_id, customer_id)
        hub_order_hk = md5_hash(order_id)
        hub_customer_hk = md5_hash(customer_id)
        assert lnk_hk != hub_order_hk
        assert lnk_hk != hub_customer_hk

    def test_satellite_change_detection(self):
        v1 = md5_hash_diff("Alice", "Johnson", "alice@test.com", "Gold")
        v2 = md5_hash_diff("Alice", "Johnson", "alice@test.com", "Platinum")
        assert v1 != v2  # loyalty tier changed → new satellite record


class TestSampleData:
    """Validate sample data files."""

    @pytest.fixture
    def samples_dir(self):
        return os.path.join(os.path.dirname(__file__), "..", "data",
                            "samples")

    def test_orders_sample_valid(self, samples_dir):
        with open(os.path.join(samples_dir, "orders_sample.json")) as f:
            data = json.load(f)
        assert len(data) >= 2
        for order in data:
            assert "order_id" in order
            assert "customer_id" in order
            assert "line_items" in order
            assert len(order["line_items"]) >= 1

    def test_customers_sample_valid(self, samples_dir):
        with open(os.path.join(samples_dir, "customers_sample.json")) as f:
            data = json.load(f)
        assert len(data) >= 2
        for cust in data:
            assert "customer_id" in cust
            assert "email" in cust

    def test_clickstream_sample_valid(self, samples_dir):
        with open(os.path.join(samples_dir, "clickstream_sample.json")) as f:
            data = json.load(f)
        assert len(data) >= 2
        event_types = {e["event_type"] for e in data}
        assert "page_view" in event_types

    def test_inventory_sample_valid(self, samples_dir):
        with open(os.path.join(samples_dir, "inventory_sample.json")) as f:
            data = json.load(f)
        assert len(data) >= 2
        for item in data:
            assert "product_id" in item
            assert item["quantity_on_hand"] >= 0
