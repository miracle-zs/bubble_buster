# Dashboard Task Board Redesign

## Goal
Improve the standalone task board for debugging by making task results readable, filterable, and anomaly-focused.

## Scope
- Keep task board separate from account cards
- Add board header with latest update time and filters
- Sort accounts with FAILED/PARTIAL first
- Show account mode labels
- Add special emphasis for equity recovery take-profit monitoring
- Render task-specific result templates with two-line result column
- Collapse symbol details by default and allow expand/collapse
- Add hover tooltips with full details

## Implementation Steps
1. Add tests covering new task-board structure and expected JS hooks.
2. Refactor task-row rendering helpers in dashboard overview HTML/JS.
3. Add board filters, latest update header, and anomaly-first sorting.
4. Add result template formatters and symbol collapse/expand behavior.
5. Verify with unit tests and py_compile.
