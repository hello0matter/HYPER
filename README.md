# Hyperliquid 锚定价差 / 相关性监控

首次运行（若环境没有该库）：`pip install -r requirements.txt`

运行：`python hyperliquid_correlation_monitor.py`

## 四个研究页

1. **价差 / 对冲**：实时 WebSocket 盘口与基准价对比。预言机价只能用来体检；要研究可执行价差，需要改为可交易的外部基准。
2. **资金费率**：扫描多个 Hyperliquid 合约的当前资金费与标记溢价。高费率不等于低风险收益。
3. **体育 Positive EV**：记录你自己的概率估计、市场概率、模拟投入与最终结果，保存到脚本目录的 `positive_ev_journal.csv`，用于验证模型而非自动投注。
4. **跨资产联动**：读取公开的 5 分钟 K 线，计算 BTC/ETH/SOL 等合约的历史收益率相关性；并可扫描“小币 vs BTC/ETH”的偏离、beta、资金费和盘口点差，用来研究“直接腿 + 保护腿”的候选组合。相关性只说明过去的联动，不是交易指令。
5. **小币联动监控**：持续刷新候选小币的偏离状态，区分“观察”和“候选”，用于纸面跟踪你关心的小币对冲方向。
6. **历史回测**：对单个“小币 vs 保护腿”做粗略残差回归回测，查看过去是否经常回归、胜率和最大单次亏损。
7. **全局 API**：集中配置自定义 JSON 数据源、请求头、字段路径和 FFD 测试入口。

使用 Hyperliquid 公开只读 WebSocket 行情；不需要 API Key，也没有下单、钱包或私钥功能。初始配置监控 `xyz:GOLD`：DEX 最优买卖价对比同一市场的 `oraclePx`，用来验证数据流、盘口价差与本地接收延迟。

## 服务器采集模式

一条命令即可在服务器上持续采集小币联动数据：

```powershell
python hyperliquid_correlation_monitor.py --server
```

默认监听 `0.0.0.0:8787`，每 60 秒扫描一次，数据保存到脚本目录的 `altcoin_monitor.sqlite3`。客户端 GUI 在第 7 页填写服务器 URL，例如 `http://服务器IP:8787`。如果服务器用 nginx 反代到 `/hl/`，则填写 `http://服务器IP/hl`，然后第 5 页点击“读服务器最新”。

常用接口：

- `GET /health`：服务器状态；
- `GET /dashboard`：浏览器看高级监控台，包含最新表格、归一化 K 线、偏离 Z 曲线、相关性/点差质量图和粗回测入口；
- `GET /latest`：最新一轮扫描；
- `GET /series?asset=SOL&leader=ETH&limit=240`：某个小币相对保护腿的偏离曲线数据；
- `GET /stats?asset=SOL&leader=ETH&limit=240`：某个小币的历史统计；
- `GET /candles?asset=SOL&leader=ETH&hours=24`：小币与保护腿的 5 分钟 K 线归一化对比；
- `GET /backtest?asset=SOL&leader=ETH&hours=168`：粗略残差回归回测；
- `GET /history?asset=SOL&limit=200`：某个币的历史记录。
- `GET /paper?limit=200`：模拟盘持仓、历史平仓、权益曲线和当前模拟参数。

### 模拟盘 / 纸面交易

服务器模式默认开启模拟盘，不会真实下单。规则是：候选出现时模拟开仓；偏离回归、达到固定止盈、止损、超时、相关性恶化或点差恶化时自动模拟平仓；开仓和平仓都会推送钉钉。

常用参数可以放在 `/opt/hyperliquid-monitor/.env`：

```bash
PAPER_ENABLED=1
PAPER_NOTIONAL_USDC=1000
PAPER_EXIT_Z=0.5
PAPER_TAKE_PROFIT_BPS=50
PAPER_STOP_BPS=80
PAPER_MAX_HOLD_MINUTES=360
PAPER_MAX_OPEN=12
PAPER_FEE_BPS=4
PAPER_Z_VALUE_BPS=18
PAPER_MIN_CORR=0.65
HLM_ADMIN_TOKEN=一个足够长的随机口令
```

也可以在 dashboard 的“模拟盘 / 纸面交易”区域直接修改这些参数。因为 dashboard 是公网地址，保存时需要输入 `HLM_ADMIN_TOKEN` 管理口令；参数保存后会实时生效，并写回 `.env`，服务重启后不丢。

也可以用启动参数覆盖，例如：

```powershell
python hyperliquid_correlation_monitor.py --server --paper-notional 500 --paper-stop-bps 60 --paper-exit-z 0.4 --paper-max-open 6
```

模拟收益是残差/Z 回归近似，用于远程观察策略质量，不等于真实成交盈亏。真实成交还需要逐笔成交价、盘口深度、滑点、手续费、资金费结算和爆仓约束。

### 真实交易面板（默认关闭）

Dashboard 顶部“真实交易”会弹出独立面板：它读取主钱包的真实余额和仓位，并把快照存入同一个 SQLite 数据库的独立 `live_account_snapshots` 表，不与模拟盘交易混在一起。真实下单开关默认关闭，网页中不会接收、显示或保存私钥。

Hyperliquid 的 API 钱包私钥只可从 SSH 终端交互配置（不进入 shell 历史）：

```bash
cd /opt/hyperliquid-monitor
.venv/bin/pip install -r requirements.txt
.venv/bin/python hyperliquid_correlation_monitor.py --set-live-api-key
```

替换私钥必须提供旧私钥：

```bash
.venv/bin/python hyperliquid_correlation_monitor.py --change-live-api-key
```

私钥使用服务器 `.env` 中的独立主密钥加密后保存在 `live_api_secret.json`，文件权限为仅服务账户可读；数据库、网页接口和日志均不会包含私钥。该加密保护的是静态文件，不能替代 HTTPS、服务器权限管理或 API 钱包权限控制。

### 钉钉推送

服务器模式会读取 `/opt/hyperliquid-monitor/.env` 中的环境变量：

```bash
DINGTALK_WEBHOOK=https://oapi.dingtalk.com/robot/send?access_token=你的token
DINGTALK_KEYWORD=小测试
HLM_PUBLIC_URL=http://你的服务器/hl
```

推送类型：

- `候选开仓观察`：刚刚满足相关性、偏离和点差过滤条件；
- `候选持续提醒`：同一币对同一方向仍满足条件，冷却时间后再次提醒；
- `候选解除`：上一轮还是候选，这一轮已经不满足条件；
- `谨慎风险`：偏离足够大，但点差等交易质量明显变差，只能人工谨慎看。

同一币对同一方向默认 30 分钟冷却一次，避免刷屏。Webhook 属于敏感地址，不要放到公开网页、仓库或聊天截图中。

常用高级参数示例：

```powershell
python hyperliquid_correlation_monitor.py --server --port 8787 --interval 60 --assets ALL --leaders BTC,ETH --public-url http://你的服务器/hl
```

## 接入自己的锚定报价

将“基准模式”改为 `custom_json`，填写一个返回 JSON 的 HTTPS 地址与价格字段路径。例如响应为 `{"data":{"last":4107.5}}` 时填写 `data.last`。点击“保存配置”会在脚本旁生成 `monitor_config.json`。可以用任何你有授权的数据商、交易所或内部报价服务。

通用外部行情适配器还支持：

- `GET`（查询参数直接写在 URL 中）或 `POST JSON` 请求；
- 最新价字段，或同时填写买一/卖一字段（后者才能研究可成交价差）；
- 可选源时间戳字段（Unix 秒、Unix 毫秒或 ISO 时间）；
- 可选 HTTP 请求头，例如 `{"Authorization":"Bearer ${IFIND_API_KEY}"}`。先在 PowerShell 设置 `$env:IFIND_API_KEY='实际密钥'`，不要把密钥直接保存到 `monitor_config.json`。

拿到供应商文档后，先点击“测试自定义源”。只有测试同时得到合理的买一、卖一和源时间戳，才适合进入价差研究；只返回“最新价”的源只适合趋势与相关性观察。

### FFD 研究快照源

完成本地 FFD MCP 安装后，基准模式可选择 `ffd_crypto_snapshot`，并将 DEX 合约和 FFD 加密标的设为同名，例如 `BTC` / `BTC` 或 `ETH` / `ETH`。该源读取 FFD 的最新可用加密快照，并为避免无效消耗配额，最低每 60 秒刷新一次。

FFD 当前快照不提供可成交买一/卖一，因此本程序会将其标为**研究用**，不会视作套利或延迟交易基准。它适合做跨源走势、事件前后变化与相关性研究；实际价差仍应采用有买一、卖一、源时间戳的交易所数据。

工具每次采集记录两源价格，并从滚动样本计算：

- DEX 中间价与基准价的价差（bps）；
- 对数收益率 Pearson 相关性；
- 在设定范围内扫描哪一方领先/滞后（正值表示基准收益率在后）；
- 价差 Z-score、Hyperliquid 服务时间相对本机时间的滞后、HTTP 请求耗时；
- 合约每小时资金费率、简单年化和标记溢价；正费率由多头付给空头，负费率相反；
- 仅当价差同时超过手动阈值和“往返成本 + 安全垫”时标为候选。

Hyperliquid 的盘口、预言机价格与资金费率均以持续 WebSocket 推送接收，不再按设置的秒数轮询其 HTTP 接口。“记录间隔”只控制将最新推送保存成统计样本的频率。`custom_json` 外部报价尚未定义统一 WebSocket 格式，因此仍按记录间隔通过 HTTP 刷新。

界面的“最新盘口数据年龄”是本机收到的最新盘口里服务器时间距现在的差值；它包含交易所没有新的盘口推送、服务端处理和网络传输，**不是单独的网络延迟测量**。

界面中的两条走势曲线以启动时价格归一化为 100，方便比较谁先涨跌；下图是 DEX 相对基准的实际价差（bps）。红色虚线是候选阈值。曲线仅显示本次启动后收集的数据，当前版本不保存历史行情。

程序将相关性建立在**收益率**而不是绝对价格上；绝对价格会在价差与 Z-score 中单独处理。1 分钟 K 线级别的相关性需要至少约 9 个采样区间，默认 5 秒采样，所以启动后约一分钟才开始有初步读数。若需严格 K 线对齐，应设置一个能返回时间戳及历史 OHLC 的外部行情源；本版本的核心是实时相对定价与可观测时延。

“小币联动机会扫描”会把 BTC/ETH 等大币当作保护腿，计算候选小币的历史相关性、beta 和最新残差 Z-score。Z 为正代表小币相对保护腿偏强，Z 为负代表相对偏弱；界面给出的“做多/做空 + 保护腿”只是回归假设下的研究方向。真正能否交易还要看偏离回归速度、盘口点差、滑点、资金费、爆仓距离和连续纸面记录结果。

“观察”表示没有同时通过相关性、偏离和盘口过滤，只记录不动作；“候选”表示通过当前阈值，可以进入纸面跟踪或更细的回测。历史回测页使用 5 分钟收盘价做粗模拟，不包含真实盘口滑点、资金费变化和爆仓约束，因此不能直接当成实盘收益。

资金费率应当被当作持仓成本/拥挤度过滤器，而非方向预测器。比如持续为正且很高，说明做多要支付、做空会收取资金费；但价格仍可能继续上涨，使空头的方向损失超过资金费收入。执行前应至少检查费率是否已持续多个结算周期、OI/成交量是否支持、盘口深度和爆仓价格，并以净收益（价差 − 双边费用 − 滑点 ± 预期资金费）判断。

## 重要限制

`xyz:GOLD` 是 Hyperliquid 上由 XYZ DEX 部署的永续合约；它不是现货 `GOLD-USDC`。默认预言机同样服务于该合约，所以它适合检查交易价偏离预言机，**不构成跨市场套利证明**。实际套利候选还必须覆盖深度、滑点、双边手续费、资金费、交易时段、结算/转换成本与执行失败风险。先用小额、只读回测或模拟成交验证，切勿按界面方向直接交易。
