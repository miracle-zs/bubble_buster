"""HTML template loader and content-hash static asset versioning for web dashboard."""

import hashlib
from html import escape as html_escape
import re
from pathlib import Path
from typing import Dict, Literal, Optional, Tuple

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_DEFAULT_TEMPLATES_DIR = (_PROJECT_ROOT / "templates").resolve()
_DEFAULT_STATIC_DIR = (_PROJECT_ROOT / "app_static").resolve()

_TEMPLATE_CACHE: Dict[str, Tuple[int, int, str]] = {}
_ASSET_VERSION_CACHE: Dict[str, Tuple[int, int, str]] = {}

_STATIC_URL_PATTERN = re.compile(
    r"""(?P<attr>(?:href|src)=["'])(?P<prefix>(?:\.\./)*static/|/static/)(?P<asset>[^"'?#]+)(?:\?[^"']*)?(?P<quote>["'])"""
)

TopbarPage = Literal["overview", "account", "comparison"]

_PAGE_HOTKEYS: Dict[TopbarPage, Tuple[Tuple[str, str], ...]] = {
    "overview": (
        ("手动刷新", "R"),
        ("返回账户总览", "0"),
        ("打开权益对比", "C"),
        ("切换账户详情", "1-9"),
        ("打开/关闭本面板", "?"),
    ),
    "account": (
        ("手动刷新", "R"),
        ("聚焦订单筛选", "/"),
        ("返回账户总览", "0"),
        ("打开权益对比", "C"),
        ("切换账户详情", "1-9"),
        ("打开/关闭本面板", "?"),
    ),
    "comparison": (
        ("手动刷新", "R"),
        ("返回账户总览", "0"),
        ("切换账户详情", "1-9"),
        ("打开/关闭本面板", "?"),
    ),
}


def load_template(name: str, base_dir: Optional[Path] = None) -> str:
    """Load an HTML template with stat-based in-memory caching.

    If the template file is modified on disk (e.g. during frontend dev),
    the cache automatically invalidates without needing a server restart.
    """
    templates_dir = base_dir or _DEFAULT_TEMPLATES_DIR
    target = templates_dir / name
    if not target.exists():
        raise FileNotFoundError(f"Template not found: {target}")

    cache_key = str(target.resolve())
    st = target.stat()
    cached = _TEMPLATE_CACHE.get(cache_key)
    if cached is not None and cached[0] == st.st_mtime_ns and cached[1] == st.st_size:
        return cached[2]

    content = target.read_text(encoding="utf-8")
    _TEMPLATE_CACHE[cache_key] = (st.st_mtime_ns, st.st_size, content)
    return content


def static_asset_version(rel_path: str, static_dir: Optional[Path] = None) -> str:
    """Return a 10-char SHA-256 content hash for a static asset, cached by (mtime_ns, size)."""
    root = (static_dir or _DEFAULT_STATIC_DIR).resolve()
    target = (root / rel_path.lstrip("/")).resolve()
    if root not in target.parents and target != root:
        raise ValueError(f"Static asset escapes static root: {rel_path}")
    if not target.is_file():
        raise FileNotFoundError(f"Static asset not found: {target}")

    cache_key = str(target)
    st = target.stat()
    cached = _ASSET_VERSION_CACHE.get(cache_key)
    if cached is not None and cached[0] == st.st_mtime_ns and cached[1] == st.st_size:
        return cached[2]

    digest = hashlib.sha256(target.read_bytes()).hexdigest()[:10]
    _ASSET_VERSION_CACHE[cache_key] = (st.st_mtime_ns, st.st_size, digest)
    return digest


def version_static_path(url_path: str, static_dir: Optional[Path] = None) -> str:
    """Append ?v=<hash> to a local /static/... or relative static/... path; pass external URLs through."""
    if not url_path or "://" in url_path:
        return url_path
    base = url_path.split("?", 1)[0]
    for marker in ("/static/", "static/"):
        idx = base.find(marker)
        if idx >= 0:
            rel_path = base[idx + len(marker):]
            if rel_path:
                return f"{base}?v={static_asset_version(rel_path, static_dir=static_dir)}"
    return url_path


def version_static_urls(html: str, static_dir: Optional[Path] = None) -> str:
    """Rewrite href/src attributes pointing at local static/ assets to include ?v=<sha256[:10]>."""

    def _replace(match: re.Match[str]) -> str:
        asset = match.group("asset")
        version = static_asset_version(asset, static_dir=static_dir)
        return f"{match.group('attr')}{match.group('prefix')}{asset}?v={version}{match.group('quote')}"

    return _STATIC_URL_PATTERN.sub(_replace, html)


def render_ops_topbar(
    *,
    active_page: TopbarPage,
    home_href: str,
    comparison_href: str,
    templates_dir: Optional[Path] = None,
) -> str:
    """Render the shared skip-link and topbar partial for a dashboard page."""
    hotkeys = _PAGE_HOTKEYS[active_page]
    hotkey_lines = "\n".join(
        f"            <li><span>{html_escape(label)}</span><kbd>{html_escape(key)}</kbd></li>"
        for label, key in hotkeys
    )
    safe_home = html_escape(home_href, quote=True)
    safe_comp = html_escape(comparison_href, quote=True)
    replacements = {
        "__BRAND_HREF__": safe_home,
        "__OVERVIEW_HREF__": safe_home,
        "__OVERVIEW_CURRENT__": ' aria-current="page"' if active_page == "overview" else "",
        "__COMPARISON_HREF__": safe_comp,
        "__COMPARISON_CURRENT__": ' aria-current="page"' if active_page == "comparison" else "",
        "__HOTKEY_ITEMS_HTML__": hotkey_lines,
    }
    pattern = re.compile("|".join(re.escape(key) for key in replacements))
    partial = load_template("_ops_topbar.html", base_dir=templates_dir)
    return pattern.sub(lambda match: replacements[match.group(0)], partial).rstrip("\n")


def inject_ops_topbar(
    html: str,
    *,
    active_page: TopbarPage,
    home_href: str,
    comparison_href: str,
    templates_dir: Optional[Path] = None,
) -> str:
    """Replace the __OPS_TOPBAR__ placeholder with the rendered shared topbar."""
    if "__OPS_TOPBAR__" not in html:
        raise ValueError("Template is missing __OPS_TOPBAR__ placeholder")
    topbar = render_ops_topbar(
        active_page=active_page,
        home_href=home_href,
        comparison_href=comparison_href,
        templates_dir=templates_dir,
    )
    return html.replace("__OPS_TOPBAR__", topbar, 1)


def clear_template_cache() -> None:
    """Clear in-memory template and static asset version caches (mainly used for testing)."""
    _TEMPLATE_CACHE.clear()
    _ASSET_VERSION_CACHE.clear()
