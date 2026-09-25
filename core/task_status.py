"""Domain utilities for formatting, classifying, and normalizing scheduled task execution statuses."""

from __future__ import annotations

from typing import Any, Dict, List, Optional


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return default


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def status_from_error_count(errors: int, successes: int) -> str:
    if errors <= 0:
        return "SUCCESS"
    if successes > 0:
        return "PARTIAL"
    return "FAILED"


def format_symbol_field(value: Any) -> str:
    if isinstance(value, str):
        symbols = [x.strip().upper() for x in value.split(",") if x.strip()]
    elif isinstance(value, (list, tuple, set)):
        symbols = [str(x).strip().upper() for x in value if str(x).strip()]
    else:
        symbols = []
    if not symbols:
        return "-"
    unique: List[str] = []
    for sym in symbols:
        if sym not in unique:
            unique.append(sym)
    return ",".join(unique)


def append_summary_part(parts: List[str], key: str, value: Any) -> None:
    text = str(value).strip()
    if text == "" or text == "-":
        return
    parts.append(f"{key}={text}")


def task_status_template() -> Dict[str, Dict[str, Any]]:
    return {
        "entry": {"status": "UNKNOWN", "time_local": None, "summary": "--"},
        "daily_loss_cut": {"status": "UNKNOWN", "time_local": None, "summary": "--"},
        "noon_protection": {"status": "UNKNOWN", "time_local": None, "summary": "--"},
        "manage": {"status": "UNKNOWN", "time_local": None, "summary": "--"},
        "equity_recovery_take_profit": {"status": "UNKNOWN", "time_local": None, "summary": "--"},
    }


def format_task_status(
    task_key: str,
    payload: Dict[str, Any],
    time_local: Optional[str] = None,
) -> Dict[str, Any]:
    """Given a raw result payload from a task execution, calculate normalized status and summary."""
    status = "UNKNOWN"
    summary = "--"

    if task_key == "entry":
        opened = safe_int(payload.get("opened"), 0)
        failed = safe_int(payload.get("failed"), 0)
        skipped = safe_int(payload.get("skipped"), 0)
        status_raw = str(payload.get("status") or "").upper()
        if status_raw in {"SUCCESS", "FAILED", "SKIPPED", "RUNNING"}:
            status = status_raw
        else:
            status = status_from_error_count(failed, opened)
        entry_failed_symbols = format_symbol_field(payload.get("entry_failed_symbols"))
        skipped_symbols = format_symbol_field(payload.get("skipped_symbols"))
        parts = [f"opened={opened}", f"failed={failed}", f"skipped={skipped}"]
        append_summary_part(parts, "failed_symbols", entry_failed_symbols)
        append_summary_part(parts, "skipped_symbols", skipped_symbols)
        summary = " ".join(parts)

    elif task_key == "daily_loss_cut":
        total = safe_int(payload.get("total"), 0)
        closed_loss_cut = safe_int(payload.get("closed_loss_cut"), 0)
        errors = safe_int(payload.get("errors"), 0)
        status = status_from_error_count(errors, max(0, total - errors))
        closed_symbols = format_symbol_field(payload.get("closed_symbols"))
        failed_symbols = format_symbol_field(payload.get("failed_symbols"))
        parts = [f"total={total}", f"closed={closed_loss_cut}", f"errors={errors}"]
        append_summary_part(parts, "closed_symbols", closed_symbols)
        append_summary_part(parts, "failed_symbols", failed_symbols)
        summary = " ".join(parts)

    elif task_key == "noon_protection":
        total = safe_int(payload.get("total"), 0)
        updated_sl = safe_int(payload.get("updated_sl"), 0)
        skipped = safe_int(payload.get("skipped"), 0)
        errors = safe_int(payload.get("errors"), 0)
        status = status_from_error_count(errors, max(0, updated_sl + skipped))
        failed_symbols = format_symbol_field(payload.get("failed_symbols"))
        parts = [f"total={total}", f"updated={updated_sl}", f"skipped={skipped}", f"errors={errors}"]
        append_summary_part(parts, "failed_symbols", failed_symbols)
        summary = " ".join(parts)

    elif task_key == "manage":
        if payload.get("skipped"):
            status = "SKIPPED"
            reason = str(payload.get("reason") or "SKIPPED").strip() or "SKIPPED"
            summary = f"reason={reason}"
        elif payload.get("error"):
            status = "FAILED"
            summary = f"error={str(payload.get('error'))[:80]}"
        else:
            manage_summary = payload.get("summary")
            if isinstance(manage_summary, dict):
                total = safe_int(manage_summary.get("total"), 0)
                closed_tp = safe_int(manage_summary.get("closed_tp"), 0)
                closed_sl = safe_int(manage_summary.get("closed_sl"), 0)
                closed_timeout = safe_int(manage_summary.get("closed_timeout"), 0)
                updated_sl = safe_int(manage_summary.get("updated_sl"), 0)
                errors = safe_int(manage_summary.get("errors"), 0)
                status = status_from_error_count(
                    errors,
                    max(0, total + closed_tp + closed_sl + closed_timeout + updated_sl),
                )
                summary = (
                    f"total={total} tp={closed_tp} sl={closed_sl} "
                    f"timeout={closed_timeout} updated={updated_sl} errors={errors}"
                )
            else:
                status = "SUCCESS"
                summary = "ok"

    elif task_key in {"equity_recovery_take_profit", "portfolio_take_profit"}:
        status_raw = str(payload.get("status") or "").upper()
        if any(
            key in payload
            for key in ("baseline_equity", "threshold_equity", "closed_take_profit", "cycle_date")
        ):
            if status_raw in {"TRIGGERED", "TRIGGERED_RETRY", "ALREADY_TRIGGERED"}:
                status = "SUCCESS"
            elif status_raw in {"MONITORING", "SKIPPED", "DISABLED"}:
                status = "SKIPPED"
            elif status_raw in {"FAILED", "ERROR"}:
                status = "FAILED"
            else:
                status = "UNKNOWN"
            closed = safe_int(payload.get("closed_take_profit"), 0)
            adjusted = safe_int(payload.get("adjusted_take_profit"), closed)
            errors = safe_int(payload.get("errors"), 0)
            current_equity = safe_float(payload.get("current_equity"))
            threshold_equity = safe_float(payload.get("threshold_equity"))
            actual_profit_pct = safe_float(payload.get("actual_profit_pct"))
            summary = (
                f"equity={current_equity:.2f}/{threshold_equity:.2f} "
                f"profit={actual_profit_pct:.2f}% adjusted={adjusted} "
                f"closed={closed} errors={errors}"
            )
            return {
                "status": status,
                "time_local": time_local,
                "summary": summary,
            }

        if status_raw in {"TRIGGERED", "PARTIAL"}:
            status = "SUCCESS"
        elif status_raw in {"NOT_TRIGGERED", "SKIPPED", "DISABLED"}:
            status = "SKIPPED"
        elif status_raw in {"FAILED", "ERROR"}:
            status = "FAILED"
        else:
            status = "UNKNOWN"
        adjusted = safe_int(payload.get("adjusted"), 0)
        errors = safe_int(payload.get("errors"), 0)
        reduced_notional = safe_float(payload.get("reduced_notional"))
        summary = f"adjusted={adjusted} errors={errors} reduced={reduced_notional:.2f}"

    return {
        "status": status,
        "time_local": time_local,
        "summary": summary,
    }
