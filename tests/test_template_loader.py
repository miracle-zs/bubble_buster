import time
from pathlib import Path
import pytest

from core.template_loader import load_template, clear_template_cache


class TestTemplateLoader:
    def setup_method(self) -> None:
        clear_template_cache()

    def test_load_existing_templates(self) -> None:
        dashboard = load_template("dashboard.html")
        assert "<!doctype html>" in dashboard
        assert "Bubble Buster Console" in dashboard
        assert "__REFRESH_SEC__" in dashboard

        overview = load_template("accounts_overview.html")
        assert "<!doctype html>" in overview
        assert "Bubble Buster Overview" in overview
        assert "__REFRESH_SEC__" in overview

    def test_load_non_existent_template_raises(self) -> None:
        with pytest.raises(FileNotFoundError):
            load_template("does_not_exist.html")

    def test_cache_and_reload_on_mtime_change(self, tmp_path: Path) -> None:
        tpl_file = tmp_path / "test.html"
        tpl_file.write_text("v1 content", encoding="utf-8")

        c1 = load_template("test.html", base_dir=tmp_path)
        assert c1 == "v1 content"

        # Cached read
        c2 = load_template("test.html", base_dir=tmp_path)
        assert c2 == "v1 content"

        # Modify file and change mtime
        time.sleep(0.01)
        tpl_file.write_text("v2 updated content", encoding="utf-8")
        # Ensure mtime changes
        new_mtime = tpl_file.stat().st_mtime + 1.0
        import os
        os.utime(str(tpl_file), (new_mtime, new_mtime))

        c3 = load_template("test.html", base_dir=tmp_path)
        assert c3 == "v2 updated content"
