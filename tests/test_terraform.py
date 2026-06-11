"""Tests for Terraform configuration files."""
import os

TF_DIR = os.path.join(os.path.dirname(__file__), "..", "terraform")


class TestTerraformStructure:
    """Validate Terraform module structure."""

    def test_main_tf_exists(self):
        assert os.path.exists(os.path.join(TF_DIR, "main.tf"))

    def test_variables_tf_exists(self):
        assert os.path.exists(os.path.join(TF_DIR, "variables.tf"))

    def test_outputs_tf_exists(self):
        assert os.path.exists(os.path.join(TF_DIR, "outputs.tf"))

    def test_all_modules_have_required_files(self):
        modules_dir = os.path.join(TF_DIR, "modules")
        for module in os.listdir(modules_dir):
            mod_path = os.path.join(modules_dir, module)
            if os.path.isdir(mod_path):
                assert os.path.exists(
                    os.path.join(mod_path, "main.tf")), \
                    f"Module {module} missing main.tf"
                assert os.path.exists(
                    os.path.join(mod_path, "variables.tf")), \
                    f"Module {module} missing variables.tf"
                assert os.path.exists(
                    os.path.join(mod_path, "outputs.tf")), \
                    f"Module {module} missing outputs.tf"

    def test_four_modules_exist(self):
        modules_dir = os.path.join(TF_DIR, "modules")
        modules = [d for d in os.listdir(modules_dir)
                   if os.path.isdir(os.path.join(modules_dir, d))]
        assert set(modules) == {
            "storage", "databricks", "data_factory", "synapse"
        }

    def test_storage_module_has_containers(self):
        main_tf = os.path.join(TF_DIR, "modules", "storage", "main.tf")
        with open(main_tf) as f:
            content = f.read()
        for container in ["bronze", "silver", "gold", "raw"]:
            assert f'"{container}"' in content, \
                f"Storage module missing {container} container"

    def test_tfvars_example_exists(self):
        assert os.path.exists(
            os.path.join(TF_DIR, "terraform.tfvars.example"))
