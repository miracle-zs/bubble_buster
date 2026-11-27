import time
from pathlib import Path
from typing import cast
import pytest

from web.template_loader import (
    TopbarPage,
    clear_template_cache,
    inject_ops_topbar,
    load_template,
    render_ops_topbar,
    static_asset_version,
    version_static_path,
    version_static_urls,
)


class TestTemplateLoader:
    def setup_method(self) -> None:
        clear_template_cache()

    def test_load_existing_templates(self) -> None:
        dashboard = load_template("dashboard.html")
        assert "<!doctype html>" in dashboard
        assert "Bubble Buster Console" in dashboard
        assert "__PAGE_CONFIG_JSON__" in dashboard
        assert "__REFRESH_SEC__" in dashboard

        overview = load_template("accounts_overview.html")
        assert "<!doctype html>" in overview
        assert "Bubble Buster 账户总览" in overview
        assert "__PAGE_CONFIG_JSON__" in overview


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

    def test_cache_isolates_same_filename_across_directories(self, tmp_path: Path) -> None:
        dir_a = tmp_path / "a"
        dir_b = tmp_path / "b"
        dir_a.mkdir()
        dir_b.mkdir()
        (dir_a / "same.html").write_text("from dir a", encoding="utf-8")
        (dir_b / "same.html").write_text("from dir b", encoding="utf-8")

        assert load_template("same.html", base_dir=dir_a) == "from dir a"
        assert load_template("same.html", base_dir=dir_b) == "from dir b"

    def test_static_asset_version_tracks_content_changes(self, tmp_path: Path) -> None:
        asset = tmp_path / "ops.css"
        asset.write_text("body { color: #fff; }", encoding="utf-8")

        v1 = static_asset_version("ops.css", static_dir=tmp_path)
        assert len(v1) == 10
        assert v1 == static_asset_version("ops.css", static_dir=tmp_path)

        html = '<link rel="stylesheet" href="../../static/ops.css" /><script src="static/ops.css?v=old"></script>'
        versioned = version_static_urls(html, static_dir=tmp_path)
        assert f'href="../../static/ops.css?v={v1}"' in versioned
        assert f'src="static/ops.css?v={v1}"' in versioned

        assert version_static_path("/static/ops.css", static_dir=tmp_path) == f"/static/ops.css?v={v1}"
        assert version_static_path("https://cdn.example.com/echarts.min.js", static_dir=tmp_path) == (
            "https://cdn.example.com/echarts.min.js"
        )

        # Updating file bytes changes the content hash automatically without restart
        asset.write_text("body { color: #000; font-size: 14px; }", encoding="utf-8")
        v2 = static_asset_version("ops.css", static_dir=tmp_path)
        assert len(v2) == 10
        assert v2 != v1

    def test_static_asset_version_crashes_early_on_missing_or_escape(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            static_asset_version("does_not_exist.js", static_dir=tmp_path)
        with pytest.raises(ValueError):
            static_asset_version("../outside.js", static_dir=tmp_path)

    def test_render_and_inject_shared_ops_topbar(self) -> None:
        overview_bar = render_ops_topbar(
            active_page="overview",
            home_href="./",
            comparison_href="accounts/comparison/",
        )
        assert 'id="ops-nav-overview" class="ops-nav-link" href="./" aria-current="page"' in overview_bar
        assert 'id="ops-nav-comparison" class="ops-nav-link" href="accounts/comparison/">' in overview_bar
        assert "<kbd>C</kbd>" in overview_bar
        assert "<kbd>/</kbd>" not in overview_bar

        account_bar = render_ops_topbar(
            active_page="account",
            home_href="/",
            comparison_href="/accounts/comparison/",
        )
        assert 'aria-current="page"' not in account_bar
        assert "<kbd>/</kbd>" in account_bar
        assert "<kbd>C</kbd>" in account_bar

        comparison_bar = render_ops_topbar(
            active_page="comparison",
            home_href="../../",
            comparison_href="./",
        )
        assert 'id="ops-nav-brand" class="ops-brand" href="../../"' in comparison_bar
        assert 'id="ops-nav-comparison" class="ops-nav-link" href="./" aria-current="page"' in comparison_bar
        assert "<kbd>C</kbd>" not in comparison_bar

        injected = inject_ops_topbar(
            "<body>\n__OPS_TOPBAR__\n</body>",
            active_page="overview",
            home_href="./",
            comparison_href="accounts/comparison/",
        )
        assert "__OPS_TOPBAR__" not in injected
        assert 'class="ops-topbar"' in injected

    def test_ops_topbar_crashes_early_on_invalid_page_or_missing_placeholder(self) -> None:
        invalid_page = cast(TopbarPage, "unknown")
        with pytest.raises(KeyError):
            render_ops_topbar(
                active_page=invalid_page,
                home_href="./",
                comparison_href="./",
            )
        with pytest.raises(ValueError):
            inject_ops_topbar(
                "<body>no placeholder</body>",
                active_page="overview",
                home_href="./",
                comparison_href="accounts/comparison/",
            )
