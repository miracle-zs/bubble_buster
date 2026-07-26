# Bubble Buster 控制台设计风格调研

> 调研日期：2026-07-24  
> 范围：只讨论 Dashboard 首屏的视觉方向，不改变信息架构、业务功能或数据结构。

## 结论摘要

Bubble Buster 是一个需要反复查看状态、持仓、收益曲线和异常事件的交易运行控制台，不是营销网站。合适的设计方向应强调：

- 信息可扫描，运行状态和风险优先于装饰。
- 保留暗色使用习惯，减少长时间查看时的视觉干扰。
- 保留现有首屏结构：顶部运行状态、两组关键指标、双栏图表和数据面板。
- 只使用一套中性背景、一种主强调色；绿色、黄色、红色仅承担成功、警告、失败等语义。
- 主要通过 CSS token、圆角、边框、阴影、字体层级和间距调整完成，暂不引入新的前端框架。

建议优先制作以下三个 HTML 首屏 mock：

1. **Primer Quiet Ops**：改动最小，最接近 GitHub 工具界面，推荐优先级最高。
2. **Carbon Control Room**：更严谨、更像专业交易或工业控制台。
3. **Elastic Observability**：更强调图表、状态与高密度数据浏览。

## 调研方法与来源边界

### 关于 `taste.md`、`DESIGN.md` 和 awesome 合集

GitHub 上没有通用、成熟的 `taste.md` 标准。`taste.md` 更多是各项目自行约定的视觉偏好文件。相对而言，Google Labs Code 的 [`DESIGN.md` 规范](https://github.com/google-labs-code/design.md)已经定义了可机器读取的颜色、字体、间距等 token，以及 Overview、Colors、Typography、Layout、Elevation、Shapes、Components、Do's and Don'ts 等固定章节。其规范明确把 token 作为规范值、把说明文字作为使用语境，适合 Bubble Buster 在最终选型后固化视觉规则。[格式规范](https://github.com/google-labs-code/design.md/blob/main/docs/spec.md)

[`awesome-design-systems`](https://github.com/alexpate/awesome-design-systems) 用于发现候选系统。它把设计系统定义为原则、最佳实践、组件和其他指南的集合，并列出了 Primer、Carbon、Elastic 等大量系统。由于 awesome 列表本身是二手索引，本报告没有使用它来证明具体设计规则；具体判断均回到设计系统官方仓库和官方文档。

最终方向确定后，建议另建项目根目录 `DESIGN.md`，记录 Bubble Buster 自己的 token 和约束，而不是在代码中继续散布十六进制色值。

### 一手来源

- Google Labs Code：[design.md 仓库](https://github.com/google-labs-code/design.md)、[DESIGN.md 规范](https://github.com/google-labs-code/design.md/blob/main/docs/spec.md)
- GitHub Primer：[官方设计系统](https://primer.style/product/)、[Primer primitives](https://github.com/primer/primitives)、[颜色使用](https://primer.style/product/getting-started/foundations/color-usage/)、[DataTable](https://primer.style/product/components/data-table/)
- IBM Carbon：[官方仓库](https://github.com/carbon-design-system/carbon)、[暗色分层](https://carbondesignsystem.com/elements/color/overview/)、[Productive typography](https://carbondesignsystem.com/elements/typography/style-strategies/)、[Data table](https://carbondesignsystem.com/components/data-table/usage/)、[Spacing](https://carbondesignsystem.com/elements/spacing/overview/)、[Motion](https://carbondesignsystem.com/elements/motion/overview/)
- Elastic EUI：[官方仓库](https://github.com/elastic/eui)、[颜色模式](https://eui.elastic.co/docs/getting-started/theming/color-mode/)、[颜色 token](https://eui.elastic.co/docs/getting-started/theming/tokens/colors/)、[边框](https://eui.elastic.co/docs/getting-started/theming/tokens/borders/)、[Sizing](https://eui.elastic.co/docs/getting-started/theming/tokens/sizing/)、[Data grid](https://eui.elastic.co/docs/components/data-grid/)

## 现有界面诊断

现有 `dashboard_server.py` 的信息架构是合理的，首屏已经形成稳定的阅读顺序：

1. 产品名、刷新时间、下次开仓、服务状态。
2. Open Positions、Open Symbols、Recent Errors、Last Run Status。
3. Account Equity、Equity Change、Max Drawdown、Window Cashflow。
4. Equity Curve 与 Drawdown Stats。
5. 持仓、运行记录、订单事件和日志等明细。

不满意的来源主要是视觉语言叠加过多，而不是布局错误：

- 背景同时使用两个径向渐变、纵向渐变和网格纹理。
- Header、Card、Panel 都有渐变、阴影和半透明，层级边界不够克制。
- 16px 圆角、胶囊按钮、卡片光晕和 hover 位移同时出现，偏展示型界面。
- 青色与绿色同时承担品牌强调，状态色和主题色的职责有重叠。
- 标题大写、较宽字距、发光文字和等宽数据共同存在，视觉信号较多。
- 内容区本身是高密度操作台，但容器较厚、留白较大，降低了单位屏幕的信息量。

因此三个候选方案都不改变 DOM 主结构，只重新定义视觉 token 和组件表现。

## 候选一：Primer Quiet Ops

### 风格定位

安静、克制、熟悉的开发者工具风格。以 GitHub Primer 的暗色模式为参考，把视觉重点放在文本、边框、状态和数据本身，去掉装饰性光效。

Primer 是 GitHub 用于构建产品界面的指南、原则和模式集合，基础包括颜色、字体、间距和布局。[Primer 官方说明](https://primer.github.io/design/) Primer primitives 提供 light、dark 及高对比度模式的颜色、字体和间距 token。[Primer primitives 仓库](https://github.com/primer/primitives)

### 视觉原则

- 深灰黑背景，不使用背景渐变和网格纹理。
- 容器主要靠 1px 低对比边框区分，不依赖重阴影。
- 统一 6px 圆角；状态 badge 可以保留胶囊形，普通容器不做胶囊。
- 标题使用系统无衬线字体，数字和时间使用等宽字体。
- 主强调色只用冷蓝或现有青蓝；绿色、黄色、红色仅作为状态语义色。
- 表格使用紧凑或普通密度。Primer DataTable 明确提供 condensed、normal、spacious 三档，其中 condensed 用于在较小区域最大化数据可见性。[DataTable 密度说明](https://primer.style/product/components/data-table/)

### 建议 token

以下是面向 Bubble Buster 的落地建议，不要求逐字复制 Primer：

| 角色 | 建议值 |
| --- | --- |
| 页面背景 | `#0d1117` |
| 一级表面 | `#161b22` |
| 二级表面 | `#1c2128` |
| 边框 | `#30363d` |
| 主文本 | `#f0f6fc` |
| 次文本 | `#8b949e` |
| 主强调 | `#58a6ff`，或继续使用当前 `#4ec1ff` |
| 成功 / 警告 / 失败 | `#3fb950` / `#d29922` / `#f85149` |

这里视觉主题只有“中性灰 + 蓝”两类；三种状态色不参与大面积装饰。

### 首屏映射

- Header 改为无阴影的横向工具栏，下边框代替完整卡片。
- 两组 KPI 保持 4 列和 5 列布局，卡片变为紧凑的 bordered blocks。
- Equity Curve 与 Drawdown Stats 保持现有比例，标题栏改为普通文本层级。
- 时间窗口仍用 segmented control，但激活项只用蓝色填充或底边。
- 图表网格线和表格分隔线使用同一边框 token。
- Service、Last Run Status 使用小号状态 badge，不再依赖发光大字。

### 适合 Bubble Buster 的原因

- 与当前暗色、青蓝强调、开发者控制台语境最接近。
- 现有 HTML 结构几乎不用调整，只需重写 CSS token 和装饰规则。
- GitHub 式视觉对技术用户熟悉，运行状态与代码/日志内容能够自然共存。
- 视觉退让后，账户权益、运行异常和持仓变化会更突出。

### 改动范围

- **低**：集中修改 `:root`、`body`、`.header`、`.card`、`.panel`、表格、tab 和状态类。
- 删除背景纹理、渐变、卡片光晕、文字发光和 hover 位移。
- 不需要引入 Primer React 或 Primer CSS，先把其原则映射成现有原生 CSS。

### 风险

- 如果灰度层级处理过弱，界面可能显得过于普通。
- 与 GitHub 外观较接近，产品独特性主要依赖 Bubble Buster 的品牌名和青蓝强调色。
- 紧凑模式需要检查中文、长 symbol 和错误信息在小屏上的截断。

## 候选二：Carbon Control Room

### 风格定位

严谨、工业化、强调层级的专业控制台。参考 IBM Carbon Gray 100 暗色主题和 productive 产品界面，而不是 Carbon 的营销或 expressive 风格。

Carbon 的 productive 字体体系专门服务于用户长时间停留、完成明确任务、频繁操作的产品界面，强调空间效率和固定字号层级；这与交易运行监控场景高度一致。[Productive typography](https://carbondesignsystem.com/elements/typography/style-strategies/) Carbon 的暗色主题按层级逐步变亮：Gray 100 页面上叠 Gray 90、Gray 80 等表面，以亮度而不是阴影表达深度。[颜色与暗色分层](https://carbondesignsystem.com/elements/color/overview/)

### 视觉原则

- 页面使用近黑 Gray 100，内容层按 Gray 90、Gray 80 逐层变亮。
- 边角接近直角或 2–4px 小圆角，减少卡片感。
- 14px productive 基础字号，KPI 数字通过字重和对齐建立层级，不做超大展示字。
- 使用严格的 2/4/8 间距节奏。Carbon 的 spacing scale 本身以 2、4、8 的倍数组织。[Spacing](https://carbondesignsystem.com/elements/spacing/overview/)
- 主强调色为蓝或当前青蓝，面积较小；状态仍使用绿、黄、红。
- 表格行高统一、列标题明确。Carbon Data Table 提供五档行高，并要求表头与表体使用同一高度，适合建立稳定密度。[Data table](https://carbondesignsystem.com/components/data-table/usage/)
- 动效只保留快速、功能性的 productive motion。Carbon 将其定义为高效、响应迅速且不打扰任务的运动方式。[Motion](https://carbondesignsystem.com/elements/motion/overview/)

### 建议 token

| 角色 | 建议值 |
| --- | --- |
| 页面背景 | `#161616` |
| 一级表面 | `#262626` |
| 二级表面 | `#393939` |
| 边框 | `#525252` |
| 主文本 | `#f4f4f4` |
| 次文本 | `#c6c6c6` |
| 主强调 | `#78a9ff`，也可替换为当前青蓝 |
| 成功 / 警告 / 失败 | 保留语义绿 / 黄 / 红，但只用于图标、细条和文字 |

视觉主题是“灰阶 + 单一蓝色”，比现有界面更中性。

### 首屏映射

- Header 变为全宽平面标题栏，状态信息靠右排列，去掉独立 pill 容器感。
- KPI 区仍保留现有列数，但卡片之间只用 gutter 和 1px 分隔。
- 每个 KPI 可增加 2px 顶部状态条：默认蓝、异常红，但不使用渐变。
- Equity Curve 和 Drawdown Stats 作为同一层级的工作区面板，标题行统一 40px 左右高度。
- 表格采用更小行高和清晰列分隔，错误列允许两行或 tooltip。
- 全部标题取消过宽字距，统一为 sentence case 或保留现有英文但降低强调。

### 适合 Bubble Buster 的原因

- 交易策略、风控和账户状态具有“控制室”属性，Carbon 的工业产品气质匹配。
- 层级由灰阶表面表达，减少了现有大量边框、光晕和阴影竞争。
- Productive 字体和紧凑表格能提升首屏信息量。
- 设计规则明确，后续账户详情页、任务面板和日志页容易保持一致。

### 改动范围

- **中低**：首屏结构不变，但需要统一重设字号、行高、间距和容器圆角。
- KPI 卡片可以继续使用现有 DOM；若要更完整地体现 Carbon，可能需要把 Header 的 pill 改为更平面的 metadata item。
- 不建议直接引入 Carbon React；项目当前是服务端生成 HTML，CSS token 映射更轻量。

### 风险

- 视觉较冷静，可能比当前界面更“企业软件”，品牌个性弱于 Primer 方向。
- Gray 100 分层如果显示器质量较差，邻近灰色可能不易区分，需要实际设备对比度测试。
- 14px 高密度字体对部分公网用户可能偏小，需保证浏览器缩放和移动端布局。

## 候选三：Elastic Observability

### 风格定位

面向图表、事件、日志和运行健康度的可观测性工作台。参考 Elastic UI Framework 的暗色模式、语义状态色、Data Grid 和响应式表格原则。

EUI 是 Elastic Stack 使用的官方 UI 框架，核心定位是灵活组合、可访问和经过测试的产品组件。[EUI 官方仓库](https://github.com/elastic/eui) 它原生支持 light、dark 和 inverse color mode，并通过相同语义 token 在不同模式映射不同值。[Color mode](https://eui.elastic.co/docs/getting-started/theming/color-mode/)

### 视觉原则

- 深蓝黑背景配冷灰蓝表面，延续 Bubble Buster 当前的色温。
- 4px 圆角和 1px 边框。EUI 当前 medium radius 为 4px，并以统一基础边框处理大多数容器。[Borders](https://eui.elastic.co/docs/getting-started/theming/tokens/borders/)
- 采用 4/8/12/16/24/32 的明确 spacing scale。[Sizing](https://eui.elastic.co/docs/getting-started/theming/tokens/sizing/)
- 主色为蓝，图表可辅以一处 teal；状态色独立承担 success、warning、danger。
- 颜色不单独表达状态，必须同时提供文字或图标。EUI 官方颜色指南明确要求状态不能只依赖颜色。[Colors](https://eui.elastic.co/docs/getting-started/theming/tokens/colors/)
- 图表颜色克制，单序列权益曲线只用一个主色；多序列时再使用少量可区分颜色。
- 高密度明细优先横向滚动和明确列宽，不把复杂表格强制压成小卡片。EUI Data Grid 专门用于列多、结构一致、需要比较和排序的大量数据。[Data grid](https://eui.elastic.co/docs/components/data-grid/)

### 建议 token

| 角色 | 建议值 |
| --- | --- |
| 页面背景 | `#07101f` |
| 一级表面 | `#111c2c` |
| 二级表面 | `#1d2a3e` |
| 边框 | `#2b394f` |
| 主文本 | `#f5f7fa` |
| 次文本 | `#8e9fbc` |
| 主强调 | `#61a2ff` |
| 图表辅助 | `#16c5c0`，仅用于第二序列或选中态 |
| 成功 / 警告 / 失败 | `#24c292` / `#fcd883` / `#ee4c48` |

这里的主视觉为“深蓝灰 + 蓝”，teal 只作为数据可视化辅助，不在容器上铺色。

### 首屏映射

- Header 仍为现有一行结构，但更像 observability toolbar：标题左侧、健康状态与刷新信息右侧。
- 第一组 KPI 作为 service health summary，Recent Errors 和 Last Run Status 的语义更明显。
- 第二组 KPI 作为 financial summary，数字右对齐并启用 tabular numerals。
- Equity Curve 成为首屏最强视觉区域，Drawdown Stats 保持右侧紧凑 definition list。
- Panel 标题行可加入极细的蓝色 active indicator，不使用整块渐变标题。
- 表格与日志使用同一密度和字体规则，横向滚动条更轻。

### 适合 Bubble Buster 的原因

- EUI 本身服务于搜索、日志、监控和可观测性产品，与运行控制台的数据类型最接近。
- 保留现有深蓝黑和青蓝习惯，比 Carbon 更有连续性。
- 对图表、日志、状态、表格和异常信息都有成熟的一手规则可参考。
- 未来如果增加筛选、排序和更复杂的事件查询，这个方向扩展性最好。

### 改动范围

- **中低**：DOM 主结构不变，重点重设色板、圆角、边框和指标数字对齐。
- 为状态增加图标会产生少量 HTML/JS 调整；首轮 mock 可以先使用文字 + badge。
- 不引入 `@elastic/eui`，避免把当前原生 Dashboard 改造成 React 应用。

### 风险

- 如果 teal 使用过多，会回到当前青蓝光效较重的问题，因此必须限制为图表辅助色。
- EUI 完整系统功能较多，直接照搬容易让简单控制台变复杂；应只提取 token 和高密度数据原则。
- 深蓝灰层级在视觉上比 Primer 和 Carbon 更“有主题”，三者中改动感最明显。

## 三个方向对比

| 维度 | Primer Quiet Ops | Carbon Control Room | Elastic Observability |
| --- | --- | --- | --- |
| 整体气质 | 安静的开发者工具 | 严谨的工业控制台 | 专业的可观测性工作台 |
| 与现状连续性 | 高 | 中 | 高 |
| 首屏结构改动 | 极小 | 小 | 小 |
| CSS 改动量 | 最小 | 中等 | 中等 |
| 信息密度 | 中高 | 高 | 高 |
| 图表表现 | 克制、通用 | 严谨、结构化 | 三者中最强 |
| 品牌个性 | 中 | 低至中 | 中高 |
| 实施风险 | 最低 | 低 | 低至中 |
| 推荐顺序 | 1 | 2 | 3 |

## 共同落地约束

无论最终选择哪个方向，都建议遵守以下规则：

1. **不调整信息架构**：保留现有 Header、两组 KPI、Equity Curve、Drawdown Stats 和下方明细面板。
2. **取消装饰叠加**：背景纹理、径向光晕、容器渐变、文字发光和 hover 上浮不同时存在；首轮建议全部去除。
3. **主题色与状态色分离**：主视觉只使用中性色和一个蓝/青强调色；绿、黄、红只用于状态。
4. **统一圆角**：内容容器 4–6px，按钮 4–6px，只有状态 badge 可以使用完全圆角。
5. **统一边界**：普通区域使用 1px 边框；阴影只允许出现在浮层，首屏常驻面板不使用大阴影。
6. **数字可比较**：权益、数量、百分比和时间使用 tabular numerals；日志和订单 ID 使用等宽字体。
7. **颜色不是唯一信号**：`SUCCESS`、`RUNNING`、`FAILED` 等状态保留明确文字，必要时增加图标。
8. **响应式结构不倒退**：桌面保留高密度双栏；移动端 KPI 可两列或单列，复杂表格继续横向滚动。
9. **不先引入组件框架**：首轮通过现有服务端 HTML 和 CSS 实现，选型稳定后再评估是否抽取组件。
10. **建立设计契约**：最终方案确定后新增 `DESIGN.md`，记录颜色、字体、间距、圆角、状态和禁止事项。

## 推荐决策方式

下一步只制作三个独立、静态的 HTML 首屏 mock，使用相同的模拟数据和相同布局，确保比较的是视觉语言而不是内容差异：

- Mock A：Primer Quiet Ops。
- Mock B：Carbon Control Room。
- Mock C：Elastic Observability。

评审时只看四件事：

1. 5 秒内能否确认服务是否正常、是否有错误。
2. 账户权益、回撤和持仓是否容易横向比较。
3. 连续查看 10 分钟是否感觉刺眼或视觉疲劳。
4. 在 1440px 桌面和 390px 手机宽度下，是否仍保持清晰层级。

若以“改动小、简洁现代、延续现有习惯”为最高优先级，建议先选择 **Primer Quiet Ops**；若更看重专业控制台气质，选择 **Carbon Control Room**；若未来重点是图表、日志和事件分析，选择 **Elastic Observability**。
