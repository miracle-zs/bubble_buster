---
name: Bubble Buster — Obsidian Desk Quantitative Terminal
description: Calm, high-signal dark operations console for multi-account Binance USDT-M quantitative monitoring.
colors:
  desk-void: "#080a0f"
  desk-inset: "#0b0e14"
  desk-surface: "#10141c"
  desk-subtle: "#131822"
  desk-elevated: "#151b26"
  desk-hover: "#1a2230"
  hairline-subtle: "#181f2b"
  hairline-default: "#222a38"
  hairline-strong: "#344054"
  ink-primary: "#f3f6fb"
  ink-secondary: "#c2ccd9"
  ink-muted: "#8b99ad"
  brand-gold: "#f5b942"
  telemetry-cyan: "#38bdf8"
  telemetry-cyan-ink: "#7dd3fc"
  signal-emerald: "#10b981"
  signal-emerald-ink: "#34d399"
  signal-amber: "#f59e0b"
  signal-amber-ink: "#fbbf24"
  signal-crimson: "#f43f5e"
  signal-crimson-ink: "#fb7185"
  readonly-violet: "#a78bfa"
  readonly-violet-ink: "#c4b5fd"
  phase-teal: "#2dd4bf"
  phase-orange: "#fb923c"
typography:
  display:
    fontFamily: "ui-monospace, SFMono-Regular, SF Mono, JetBrains Mono, Menlo, Monaco, Consolas, monospace"
    fontSize: "1.75rem"
    fontWeight: 700
    lineHeight: 1.15
    letterSpacing: "-0.02em"
    fontFeature: "tnum"
  headline:
    fontFamily: "-apple-system, BlinkMacSystemFont, SF Pro Text, Segoe UI, PingFang SC, Noto Sans SC, sans-serif"
    fontSize: "1.375rem"
    fontWeight: 700
    lineHeight: 1.2
    letterSpacing: "-0.02em"
  title:
    fontFamily: "-apple-system, BlinkMacSystemFont, SF Pro Text, Segoe UI, PingFang SC, Noto Sans SC, sans-serif"
    fontSize: "1.0625rem"
    fontWeight: 700
    lineHeight: 1.25
    letterSpacing: "-0.015em"
  body:
    fontFamily: "-apple-system, BlinkMacSystemFont, SF Pro Text, Segoe UI, PingFang SC, Noto Sans SC, sans-serif"
    fontSize: "0.875rem"
    fontWeight: 400
    lineHeight: 1.5
    letterSpacing: "0em"
  data-cell:
    fontFamily: "ui-monospace, SFMono-Regular, SF Mono, JetBrains Mono, Menlo, Monaco, Consolas, monospace"
    fontSize: "0.8125rem"
    fontWeight: 600
    lineHeight: 1.4
    fontFeature: "tnum"
  label:
    fontFamily: "-apple-system, BlinkMacSystemFont, SF Pro Text, Segoe UI, PingFang SC, Noto Sans SC, sans-serif"
    fontSize: "0.75rem"
    fontWeight: 600
    lineHeight: 1.3
rounded:
  sm: "4px"
  md: "6px"
  pill: "999px"
spacing:
  xs: "4px"
  sm: "8px"
  md: "12px"
  lg: "16px"
  xl: "24px"
components:
  button-default:
    backgroundColor: "{colors.desk-elevated}"
    textColor: "{colors.ink-secondary}"
    rounded: "{rounded.md}"
    padding: "5px 11px"
    height: "32px"
  button-active:
    backgroundColor: "{colors.desk-hover}"
    textColor: "{colors.ink-primary}"
    rounded: "{rounded.md}"
    padding: "5px 11px"
    height: "32px"
  surface-card:
    backgroundColor: "{colors.desk-surface}"
    textColor: "{colors.ink-primary}"
    rounded: "{rounded.md}"
    padding: "14px 16px"
  strategy-chip:
    backgroundColor: "{colors.desk-subtle}"
    textColor: "{colors.ink-secondary}"
    rounded: "{rounded.sm}"
    padding: "4px 8px"
  status-badge:
    backgroundColor: "{colors.desk-subtle}"
    textColor: "{colors.ink-secondary}"
    rounded: "{rounded.pill}"
    padding: "2px 8px"
  input-search:
    backgroundColor: "{colors.desk-inset}"
    textColor: "{colors.ink-primary}"
    rounded: "{rounded.sm}"
    padding: "4px 10px"
    height: "32px"
  topbar-nav:
    backgroundColor: "{colors.desk-surface}"
    textColor: "{colors.ink-secondary}"
    height: "48px"
    padding: "0 16px"
---

# Design System: Bubble Buster — Obsidian Desk Quantitative Terminal

## Overview

**Creative North Star: "Obsidian Desk Quantitative Terminal"**

Bubble Buster is designed as a calm, precision-instrument monitoring desk for live cryptocurrency futures operations. Surfaces stay dark, flat, and disciplined so real risk signals — portfolio stop proximity, execution failures, and equity drawdowns — cut through immediately without decorative noise.

Hierarchy relies on tonal graphite stepping, crisp 1px hairlines, and tabular monospace figures rather than drop shadows or marketing flourishes. Every pixel on screen earns its place by answering an operational question: is the fleet healthy, how fresh is the snapshot, how close is each account to its stop threshold, and what failed?

**Key Characteristics:**
- Deep obsidian-slate surfaces stepped in tight neutral intervals with 1px hairline borders.
- Strictly decoupled semantic hues: cyan for navigation/telemetry focus, emerald for healthy/positive PnL, amber for caution/waiting, crimson for losses/stop/failure, and violet reserved exclusively for read-only accounts.
- Tabular monospace numerals (`tabular-nums`) for all prices, equity figures, percentages, timestamps, and counts.
- Flat-by-default containers with hairline internal dividers instead of nested cards.

## Colors

The palette anchors on cool deep-slate neutrals verified at ≥4.5:1 WCAG AA contrast, reserving chromatic saturation strictly for directional PnL, risk state, and account-mode identity.

### Primary
- **Telemetry Cyan** (`desk` active focus, navigation underlines, interactive links, zoom handles, and primary strategy chips; ink variant used for text on dark surfaces).
- **Brand Gold** (reserved solely for the rotated diamond mark in the topbar brand anchor).

### Secondary
- **Signal Emerald** (healthy service status, positive PnL/return, active TP/SL orders, and low-consumption risk bars).
- **Signal Amber** (caution proximity, waiting signals, delayed snapshots, unpriced closed-position count warnings).
- **Signal Crimson** (negative PnL/return, triggered portfolio stop-loss, failed task rows, offline/stale snapshot alerts).

### Tertiary
- **Readonly Violet** (exclusively identifies read-only benchmark accounts across nav links, badges, strips, and detail headers).
- **Phase Teal & Phase Orange** (distinguish scale-in entries and wait-bearish stages from readonly violet in daily entry timelines).

### Neutral
- **Desk Void & Desk Inset** (page canvas and recessed chart/table/progress tracks).
- **Desk Surface, Subtle & Elevated** (card body, table header/striped rows, and panel headers/interactive button resting states).
- **Hairline Subtle, Default & Strong** (internal 1px grid gaps, panel borders, and hover/overlay borders).
- **Ink Primary, Secondary & Muted** (primary headings/KPIs, table copy/controls, and metadata labels — all ≥4.5:1 on surface backgrounds).

### Named Rules
**The Readonly Violet Quarantine Rule.** Violet is never used for strategy phases, alerts, or decorative highlights; seeing violet anywhere instantly signals a read-only non-trading account.

**The Calm-Until-Broken Rule.** Neutral counts (`0` anomalies, `0` pending signals) remain neutral ink on neutral surfaces. Amber and crimson borders or backgrounds appear only when an actual threshold, warning, or failure is active.

## Typography

**Display & Data Font:** System monospace stack (`ui-monospace`, `SFMono-Regular`, `SF Mono`, `JetBrains Mono`, `Menlo`, `Consolas`)
**Body & UI Font:** System sans-serif stack (`-apple-system`, `BlinkMacSystemFont`, `SF Pro Text`, `Segoe UI`, `PingFang SC`, `Noto Sans SC`)

**Character:** Crisp, zero-latency native Chinese/Latin sans-serif paired with tabular monospace numerals so changing equity figures never jitter horizontally across polling ticks.

### Hierarchy
- **Display** (700, 1.75rem / 28px, 1.15 line-height, -0.02em tracking, `tabular-nums`): Primary hero KPIs (strategy equity, comparison percentage change, readonly 30-day net PnL).
- **Headline** (700, 1.375rem / 22px, 1.2 line-height, -0.02em tracking): Page titles and secondary KPI values.
- **Title** (700, 1.0625rem / 17px, 1.25 line-height, -0.015em tracking): Section headings and account identifiers.
- **Body** (400, 0.875rem / 14px, 1.5 line-height): Descriptive text, alert banners, and risk explanations (kept ≤760px measure).
- **Data Cell** (600, 0.8125rem / 13px, 1.4 line-height, `tabular-nums`): Dense table cells, controls, command values, and log tails.
- **Label** (600, 0.75rem / 12px, 1.3 line-height): 12px floor for metadata, badges, table headers, and axis ticks; no sub-12px micro-text.

### Named Rules
**The 12px Floor Rule.** No legible UI text, badge, or chart axis label drops below `0.75rem` (12px).

**The Tabular Lock Rule.** Every numeric column, KPI figure, countdown, and timestamp uses monospace `font-variant-numeric: tabular-nums` and right-aligns inside tables.

## Layout

Pages share a fluid centered shell (`max-width: 1680px`, `14px 16px 36px` desktop padding, `10px 10px 24px` below `760px`) anchored beneath a sticky `48px` topbar (`scroll-padding-top: 60px`).

- **Grid Composition**: Managed overview cards run 4-up above `1360px`, 2-up at `761px–1360px`, and 1-up at `≤760px`. Detail analytics split `2fr : 0.88fr` above `1180px` and stack below.
- **Hairline Grid Strips**: Multi-cell strips (command bar, KPI summary rail, entry progress overview, risk facts) use `gap: 1px` over a hairline-tinted parent background rather than per-cell margins or nested boxes.
- **Symmetrical Mobile Fallbacks**: 5-metric readonly rails promote the primary balance cell to `grid-column: 1 / -1` on 2-column mobile viewports so no orphan blank cell appears.
- **Container-Aware Tables**: Scrollable data tables wrap in `container-type: inline-size` regions with sticky `thead` and sticky `tbody th[scope="row"]` below `780px`, hiding headers when `.is-empty` so empty messages remain centered on 390px screens.

## Elevation & Depth

Surfaces are flat by default; spatial layering comes from subtle surface tone stepping (`desk-void` → `desk-surface` → `desk-elevated`) and 1px hairline borders rather than ambient card shadows. Drop shadows are reserved exclusively for floating top-layer overlays (strategy popovers, hotkey panel, skip link, and the fixed body-portal tooltip).

### Shadow Vocabulary
- **Overlay Elevation** (`box-shadow: 0 14px 36px rgba(2, 4, 10, 0.72), 0 2px 8px rgba(2, 4, 10, 0.5)`): Applied strictly to floating popovers, tooltip portals, and the keyboard skip link.

### Named Rules
**The Flat-By-Default Rule.** Cards, strips, and tables never carry drop shadows at rest. Depth is expressed through tonal stepping and 1px borders; shadows appear only on floating overlays.

## Shapes

Geometry stays crisp and utilitarian to match quantitative instrumentation: `6px` (`--radius`) on primary panels, buttons, and segmented frames; `4px` (`--radius-sm`) on inner tags, inputs, chart canvases, and kbd keycaps; full pill (`999px`) reserved strictly for status badges, freshness pills, count chips, and native `<progress>` tracks. Multi-cell rails use `overflow: clip` on the outer frame rather than fragile per-child corner radii.

## Components

Every interactive component shares a unified focus ring (`2px solid` telemetry cyan, `2px` offset), adaptive hit sizing (`32px` desktop, `44px` on `@media (pointer: coarse)` and `≥40px` on `≤760px`), and forced-colors high-contrast fallbacks.

### Buttons
- **Shape:** Subtle rounded rectangle (`6px` radius), minimum `44px` width and adaptive target height.
- **Default:** Elevated slate background with secondary ink and default hairline border; transitions border and background to hover slate in `120ms`.
- **Active / Selected:** Translucent telemetry-cyan wash with cyan border and primary ink.
- **Disabled:** `0.65` opacity with `progress` cursor during in-flight refreshes.

### Segmented Controls
- **Style:** Recessed `desk-inset` track (`6px` radius, `2px` padding) housing flat `4px`-radius toggle buttons (`aria-pressed`). Active segments adopt the translucent telemetry-cyan fill and border.

### Chips & Status Badges
- **Strategy Tags:** Compact `4px`-radius rectangular chips (`4px 8px` padding, `1.35` line-height) in neutral, primary-cyan, or off-crimson variants; safe for 2-line mobile wrapping.
- **Status & Freshness Pills:** Full pill (`999px` radius) with semantic border + 12–14% tinted fill + AA-verified ink (`ok`, `warn`, `bad`, `readonly`).

### Cards / Containers
- **Corner Style:** `6px` radius, `1px solid` default hairline border, `desk-surface` body, and `desk-elevated` header bar separated by a bottom hairline.
- **Internal Structure:** Flat hairline dividers (`border-top` / `border-left` or 1px gap grids) instead of nested bordered cards.

### Inputs & Selects
- **Style:** Recessed `desk-inset` fill, `1px` default border, `4px` radius, adaptive target height, muted placeholder ink.

### Navigation
- **Topbar:** Sticky `48px` translucent `desk-surface` bar (`backdrop-filter: blur(10px)`) with shared partial (`templates/_ops_topbar.html`), gold diamond brand mark, venue chip, horizontally scrollable account switcher with bilateral left/right edge fade masks, and `?` hotkey popover trigger.

### Signature Components
- **Top-Layer Term Tooltip Portal:** Dotted-underlined `.ops-term` triggers attach to a single fixed `document.body` tooltip (`role="tooltip"`, `aria-describedby`), auto-clamping inside viewport bounds with zero layout overflow or ancestor clipping.
- **Native Risk Budget Progress Bar:** Semantic `<progress class="ops-progress">` with separate WebKit/Mozilla fill pseudo-elements driven by `--progress-fill`, respecting `prefers-reduced-motion` and `forced-colors`.
- **Differential Strategy Highlights & Multi-Pattern Comparison Chart:** Automatically surfaces per-account strategy deltas while pairing color with distinct line patterns (solid, dashed, dotted, dash-dot) for color-blind legibility.

## Do's and Don'ts

### Do:
- **Do** keep `[hidden] { display: none !important; }` global in `ops.css` so author `display` rules never expose hidden controls.
- **Do** use hairline 1px dividers and tonal stepping inside panels instead of nesting bordered cards inside cards.
- **Do** encode status with text + shape/line-pattern in addition to color (e.g., distinct legend markers, explicit status labels, multi-pattern chart lines).
- **Do** use native semantic primitives (`<progress>`, `<details>`, `<search>`, `role="tablist"`, `aria-sort`) before reaching for generic `<div>` widgets.
- **Do** keep all static asset references routed through `web/template_loader.py` for automatic content-hash `?v=` versioning.

### Don't:
- **Don't** put one-sided colored accent bars down the side of rounded cards; highlight specific failing rows or use full subtle borders/badges instead.
- **Don't** render CSS `::after` pseudo-element tooltips inside bordered/grid containers (they inflate `scrollWidth`, clip under `overflow: hidden`, and fail screen readers).
- **Don't** apply warning/danger colors to healthy zero states (`待触发 0`, `异常 0`) or label stale historical snapshots as "在线快照".
- **Don't** reuse readonly violet (`#a78bfa`) on managed accounts or strategy entry phases.
- **Don't** add inline executable `<script>` blocks or inline `<style>` tags in templates (violates strict CSP `script-src 'self'`).
