# TradingView 社区策略初筛（2026-07-19）

测试环境：TradingView Desktop，`BINANCE:BTCUSDT`，15分钟。这里记录的是社区脚本在 TradingView Strategy Tester 的初筛结果，不代表 Hyperliquid 可成交收益。

## 原始社区脚本初筛

| TradingView 社区脚本 | 交易数 | Profit Factor | TradingView 手续费 | 初筛结论 |
|---|---:|---:|---:|---|
| Crypto Turtle - Trend Following Strategy | 591 | 0.91 | 0 | 不通过 |
| Simple and efficient MACD crypto strategy with risk management | 2356 | 0.89 | 0 | 不通过 |
| Ichimoku TK Cross > EMA200 Crypto Strategy | 341 | 0.94 | 0 | 不通过 |
| CRYPTO 3EMA Strategy with TP/SL based on ATR | 0 | - | 0 | 当前市场/参数无交易 |
| Crypto Squeeze Strategy | 430 | 0.96 | 0 | 不通过 |
| OBV Accumulation / Distribution Strategy Crypto | 176 | 0.44 | 有 | 不通过 |
| Bollinger + RSI, Double Strategy (by ChartArt) v1.1 | 86 | 1.27 | 0 | 仅可继续复验 |
| MACD + SMA 200 Strategy (by ChartArt) | 117 | 1.30 | 0 | 仅可继续复验 |
| ADX+DI+SUPERTREND Strategy | 0 | - | 0 | 当前市场/参数无交易 |
| Stochastic RSI Strategy | 1337 | 1.09 | 0 | 边际太薄，成本后可疑 |
| Swing VWAP Weekly Stock and Crypto Strategy | 626 | 0.78 | 有 | 不通过 |
| Turtle trading strategy (Donchian/ATR) | 683 | 0.82 | 0 | 不通过 |
| Bollinger Bands Breakout Strategy | 854 | 1.02 | 0 | 边际太薄，成本后可疑 |

## 加入 HYPER 的方式

没有复制或绕过社区脚本的受保护 Pine 源码。HYPER 只根据公开、通用的技术规则独立实现代表性策略族，并在每条回测结果中保存 `reference`：

- MACD + SMA 趋势过滤；
- Bollinger + RSI 双确认；
- Stochastic RSI；
- Ichimoku 转换线/基准线 + EMA 趋势过滤；
- 三 EMA 排列；
- Bollinger/Keltner Squeeze 突破；
- OBV + EMA 量价过滤；
- Supertrend + ADX；
- Bollinger 突破；
- Donchian/Turtle + ATR 跟踪退出。

这些实现统一使用 Hyperliquid K线、本根收盘产生信号、下一根开盘成交，并扣除配置的往返成本。页面中的绿色状态只叫“单窗口通过・禁止实盘”；它不表示策略成功。

## Hyperliquid 复验结论

使用 BTC、ETH、SOL、HYPE、DOGE，12 bps 往返成本：

- 5分钟（实际约17.5天）：310组评估，7组单窗口通过，其中2组为社区参考；
- 15分钟（30天）：310组评估，9组单窗口通过，其中4组为社区参考；
- 1小时（90天）：310组评估，16组单窗口通过，其中9组为社区参考；
- 没有任何“相同币种 + 相同策略参数”跨两个周期同时通过。

因此当前只能进入持续回测和实时模拟，不能授权新的单腿真实策略。
