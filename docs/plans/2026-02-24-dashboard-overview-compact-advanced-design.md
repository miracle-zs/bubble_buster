# Dashboard Overview + Compact/Advanced Detail Design

Date: 2026-02-24
Status: Approved

## Goal

Change `/bubble/` to account overview page, support per-account detail page at `/bubble/account/{account_id}/` with default compact mode and switchable advanced mode.

## Routes

- `/` => overview HTML
- `/account/{account_id}/` => detail HTML
- Existing APIs remain unchanged:
  - `/api/accounts/summary`
  - `/api/account/{account_id}/snapshot`

## UI

- Overview page: account cards (balance/open positions/last status), click to detail.
- Detail page: default compact view, query parameter `view=compact|advanced`, with button toggle.

## Implementation Notes

- Minimal backend change: add two HTML render functions and FastAPI routes.
- Reuse existing snapshot API and account summary API.
- Keep existing full-detail dashboard rendering for advanced mode.

## Testing

- Add route tests in `tests/test_dashboard_fastapi.py` for overview/detail HTML endpoints.
- Keep existing API tests passing.
