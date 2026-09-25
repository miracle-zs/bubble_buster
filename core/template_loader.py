from pathlib import Path
from typing import Dict, Optional, Tuple

_DEFAULT_TEMPLATES_DIR = (Path(__file__).resolve().parents[1] / "templates").resolve()
_TEMPLATE_CACHE: Dict[str, Tuple[float, str]] = {}


def load_template(name: str, base_dir: Optional[Path] = None) -> str:
    """Load an HTML template with mtime-based in-memory caching.

    If the template file is modified on disk (e.g. during frontend dev),
    the cache automatically invalidates without needing a server restart.
    """
    templates_dir = base_dir or _DEFAULT_TEMPLATES_DIR
    target = templates_dir / name
    if not target.exists():
        raise FileNotFoundError(f"Template not found: {target}")

    mtime = target.stat().st_mtime
    cached = _TEMPLATE_CACHE.get(name)
    if cached is not None and cached[0] == mtime:
        return cached[1]

    content = target.read_text(encoding="utf-8")
    _TEMPLATE_CACHE[name] = (mtime, content)
    return content


def clear_template_cache() -> None:
    """Clear in-memory template cache (mainly used for testing)."""
    _TEMPLATE_CACHE.clear()
