"""Tests for ADF JSON configuration files."""
import json
import os

ADF_DIR = os.path.join(os.path.dirname(__file__), "..", "adf")


def load_json(subdir, filename):
    path = os.path.join(ADF_DIR, subdir, filename)
    with open(path) as f:
        return json.load(f)


class TestLinkedServices:
    """Validate ADF linked service definitions."""

    def test_adls_linked_service_structure(self):
        ls = load_json("linkedservices", "ls_adls_lakehouse.json")
        assert ls["name"] == "ls_adls_lakehouse"
        props = ls["properties"]
        assert props["type"] == "AzureBlobFS"
        assert "url" in props["typeProperties"]

    def test_databricks_linked_service(self):
        ls = load_json("linkedservices", "ls_databricks.json")
        assert ls["properties"]["type"] == "AzureDatabricks"
        tp = ls["properties"]["typeProperties"]
        assert "domain" in tp
        assert "existingClusterId" in tp

    def test_rest_orders_linked_service(self):
        ls = load_json("linkedservices", "ls_rest_orders_api.json")
        assert ls["properties"]["type"] == "RestService"

    def test_sftp_linked_service(self):
        ls = load_json("linkedservices", "ls_sftp_inventory.json")
        assert ls["properties"]["type"] == "Sftp"
        assert ls["properties"]["typeProperties"]["port"] == 22


class TestDatasets:
    """Validate ADF dataset definitions."""

    def test_all_datasets_have_linked_service(self):
        ds_dir = os.path.join(ADF_DIR, "datasets")
        for fname in os.listdir(ds_dir):
            if fname.endswith(".json"):
                ds = load_json("datasets", fname)
                assert "linkedServiceName" in ds["properties"], \
                    f"{fname} missing linkedServiceName"

    def test_bronze_dataset_references_adls(self):
        ds = load_json("datasets", "ds_adls_bronze.json")
        ref = ds["properties"]["linkedServiceName"]["referenceName"]
        assert ref == "ls_adls_lakehouse"


class TestPipelines:
    """Validate ADF pipeline definitions."""

    def test_master_pipeline_has_all_stages(self):
        pl = load_json("pipelines", "pl_master_lakehouse.json")
        activities = pl["properties"]["activities"]
        names = [a["name"] for a in activities]
        assert "IngestOrders" in names
        assert "IngestClickstream" in names
        assert "IngestInventory" in names
        assert "IngestCustomers" in names
        assert "RunBronzeToSilverNotebook" in names
        assert "RunSilverToGoldNotebook" in names
        assert "TriggerAirflowQuality" in names

    def test_master_pipeline_dependencies(self):
        pl = load_json("pipelines", "pl_master_lakehouse.json")
        activities = pl["properties"]["activities"]
        b2s = [a for a in activities
               if a["name"] == "RunBronzeToSilverNotebook"][0]
        dep_names = [d["activity"] for d in b2s["dependsOn"]]
        assert "IngestOrders" in dep_names
        assert "IngestClickstream" in dep_names

    def test_all_pipelines_have_retry_policy(self):
        pl_dir = os.path.join(ADF_DIR, "pipelines")
        for fname in os.listdir(pl_dir):
            if fname.endswith(".json") and "master" not in fname:
                pl = load_json("pipelines", fname)
                activities = pl["properties"]["activities"]
                for act in activities:
                    if "policy" in act:
                        assert act["policy"]["retry"] >= 1, \
                            f"{fname}/{act['name']} has no retry"

    def test_pipeline_parameters_defined(self):
        pl = load_json("pipelines", "pl_master_lakehouse.json")
        params = pl["properties"]["parameters"]
        assert "start_date" in params
        assert "end_date" in params
        assert "process_date" in params
