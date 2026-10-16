# 考慮篩選（GitHub Actions）

美股「考慮」條件篩選：已啟用關卡全部通過（**AND**）先列入。

## 預設關卡（全開）

**籌碼／能量（週／月）**
- 月／週 MCD：SMA(PC) ≥ SMA(LC)，N=10、SMA=10
- 月／週 MCDX：SMA(PC) ≥ SMA(LC)，N=8、SMA=8
- 月／週 主力能量：做多 ≥ 做空（通達信 9/36）

**日線**
- 收市 > $5
- 收市 > EMA200、收市 > EMA50
- 市值 > 1.5 億 USD（$150M；未知市值＝唔過）
- 成交量 > 300,000 股
- KDJ 紅／藍柱（J > 50）
- EMA9 > EMA50 > EMA200

名單預設剔 ETF／fund（Nasdaq Trader `ETF` 欄＋名稱啟發式）。

## 排程

- Cron：`30 22 * * 1-5`（UTC；約美股收市後）
- 亦可手動：Actions → **Daily Consider Screener** → Run workflow
- Job timeout：180 分鐘

## 下載結果

1. 打開該次 workflow run
2. Artifacts → **consider_symbols**
3. 解壓得 `consider_symbols.txt`（每行一個 ticker）
4. Artifact **保留 7 日**（`retention-days: 7`）

本地測試：

```bash
pip install -r requirements.txt
python run_consider.py --symbols AAPL,MSFT
```
