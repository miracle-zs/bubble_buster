# DESIGN.md

Bubble Buster 控制台的视觉契约。方向为 **Primer Quiet Ops**(安静、克制的开发者工具风格),选型依据见 `docs/design-style-research.md`。前端为 `dashboard_server.py` 内嵌的服务端 HTML + 原生 CSS,不引入组件框架。

## Overview

- 这是长时间反复查看的交易运行控制台,不是营销页面:信息可扫描性优先于装饰。
- 层级靠 1px 边框和两级表面灰阶表达,不靠阴影、渐变和光效。
- 主视觉只有「中性灰 + 单一蓝色强调」;绿 / 黄 / 红只承担状态语义。

## Colors

所有色值必须引用 CSS token,禁止在规则里散布新的十六进制色值。

| Token | 值 | 用途 |
| --- | --- | --- |
| `--bg` | `#0d1117` | 页面背景 |
| `--surface` | `#161b22` | 一级表面(卡片、面板、Header) |
| `--surface-2` | `#0d1117` | 内嵌表面(日志、表格容器、meta 项、按钮底) |
| `--line` | `#30363d` | 容器边框、面板标题分隔 |
| `--line-muted` | `#21262d` | 表格行、列表行等次级分隔线 |
| `--line-strong` | `#484f58` | hover 时的边框强调 |
| `--text` | `#f0f6fc` | 主文本 |
| `--muted` | `#8b949e` | 次文本、标签、说明 |
| `--accent` | `#58a6ff` | 唯一主强调:链接、账号名、激活态、图表主线 |
| `--accent-soft` | `rgba(88,166,255,0.12)` | 激活态底色 |
| `--ok` | `#3fb950` | 成功 / 盈利 / RUNNING |
| `--warn` | `#d29922` | 警告 / 等待 / SKIPPED |
| `--bad` | `#f85149` | 失败 / 亏损 / 异常 |
| `--purple` | `#a371f7` | 只读账户模式标识(仅总览页) |

ECharts / SVG 内无法引用 CSS 变量处,使用与 token 相同的字面值(`#58a6ff`、`#8b949e`、`#30363d`、`#161b22`)。

## Typography

- 正文:`--font-ui`(Avenir Next / SF Pro Text / PingFang SC / Noto Sans SC)。
- 数字、时间、ID、日志:`--font-mono`(SFMono-Regular / Menlo / Roboto Mono),并开启 `font-variant-numeric: tabular-nums` 保证纵向可比较。
- KPI 标签:11–12px 大写 + `letter-spacing: 0.05em`,是唯一允许的大写字距场景(另有表格列头 10px 大写)。
- 面板标题:13px / 650,正常大小写,不用主题色、不发光。
- 字重梯度:400 正文、600 强调、650–700 数据与标题;不超过 700。

## Layout & Spacing

- 页面容器:单账户页 `max-width: 1240px`,总览页 `1200px`。
- 栅格:桌面保持现有列数(4 列运行 KPI、5 列绩效 KPI、双栏图表区);≤1020px 降列,≤560px 单列;总览任务表在 ≤640px 折叠为单列。
- 常用间距:面板内边距 14–16px,卡片间距 12px,行高节奏 7–10px。

## Borders, Elevation & Shapes

- 普通容器一律 `1px solid var(--line)` + `border-radius: 6px`(`--radius`)。
- 次级分隔(表格行、统计行、时间线)用 `--line-muted`。
- **禁止投影**:常驻面板不使用 box-shadow;唯一例外是浮层(tooltip)和作为 1px 贴边线的 inset shadow(如 sticky 表头底线)。
- 完全圆角(999px)只允许出现在状态 badge / 模式 badge 上;按钮、chip、容器一律 6px。

## Components

- **状态**:颜色永远伴随明确文字(`SUCCESS` / `RUNNING` / `FAILED`…),不用颜色作唯一信号。badge 用「淡色底 + 同色边 + 语义色文字」三件套。
- **按钮 / chip**:默认透明或 `--surface-2` 底 + `--line` 边;激活态 `--accent-soft` 底 + 半透明 accent 边;hover 只变边框与文字色,不位移、不发光。
- **表格**:行分隔 `--line-muted`,表头 sticky + `--surface` 底 + muted 文字,无斑马纹;hover 行用中性 `rgba(177,186,196,0.06)`。
- **图表**:权益曲线按当前窗口的净变化表达语义:净上涨使用 `--ok`,净下跌使用 `--bad`,持平使用 `--accent`;面积填充只使用对应颜色的低透明度版本,不把红绿扩散到结构性 UI;网格线用边框 token。
- **日志**:`--surface-2` 内嵌底 + `#c9d1d9` 等宽文本。

## Do's and Don'ts

- ✅ 新增视觉一律先复用 token;需要新色值时先扩充本文件。
- ✅ 桌面保持高密度双栏;移动端复杂表格横向滚动或按既有断点折叠。
- ❌ 背景纹理、径向光晕、容器渐变、文字发光、hover 上浮、backdrop-filter。
- ❌ 青色 / 绿色同时承担品牌强调;accent 只有一个。
- ❌ 大面积铺状态色;绿黄红只用于文字、badge、细进度条。
- ❌ 引入前端框架或组件库;保持服务端 HTML + 原生 CSS。
