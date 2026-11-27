"""Web presentation and dashboard utilities."""

from web.template_loader import (
    clear_template_cache,
    inject_ops_topbar,
    load_template,
    render_ops_topbar,
)

__all__ = ["load_template", "clear_template_cache", "render_ops_topbar", "inject_ops_topbar"]
