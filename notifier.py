import logging
from datetime import datetime, timezone
from typing import Iterable, List, Optional, Sequence, Tuple

import requests

LOGGER = logging.getLogger(__name__)
SERVERCHAN_API = "https://sctapi.ftqq.com/{sendkey}.send"


class ServerChanNotifier:
    def __init__(
        self,
        enabled: bool,
        sendkey: str,
        proxies: Optional[dict] = None,
        timeout_sec: int = 10,
    ):
        self.enabled = enabled
        self.sendkey = (sendkey or "").strip()
        self.timeout_sec = timeout_sec
        self.session = requests.Session()
        if proxies:
            self.session.proxies = proxies

    def send(self, title: str, content: str) -> None:
        if not self.enabled:
            return
        if not self.sendkey:
            LOGGER.warning("ServerChan enabled but sendkey is empty; skip notification")
            return

        url = SERVERCHAN_API.format(sendkey=self.sendkey)
        data = {"title": title, "desp": content}
        try:
            response = self.session.post(url, data=data, timeout=self.timeout_sec)
            response.raise_for_status()
        except requests.RequestException as exc:
            LOGGER.error("Failed to send ServerChan notification: %s", exc)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _normalize_markdown_cell(value: object) -> str:
    text = str(value)
    text = text.replace("\n", "<br>")
    text = text.replace("|", "\\|")
    return text


def format_markdown_kv_table(rows: Sequence[Tuple[object, object]]) -> str:
    if not rows:
        return "_无_"
    lines = [
        "| 字段 | 值 |",
        "|---|---|",
    ]
    for key, value in rows:
        lines.append(f"| {_normalize_markdown_cell(key)} | {_normalize_markdown_cell(value)} |")
    return "\n".join(lines)


def format_markdown_list_section(title: str, items: Iterable[object], max_items: int = 20) -> str:
    values: List[str] = [str(item) for item in items if str(item).strip()]
    if not values:
        return ""
    cap = max(1, int(max_items))
    shown = values[:cap]
    lines = [f"### {title}", ""]
    lines.extend(f"- {value}" for value in shown)
    hidden = len(values) - len(shown)
    if hidden > 0:
        lines.append(f"- ... 其余 {hidden} 条省略")
    return "\n".join(lines)
