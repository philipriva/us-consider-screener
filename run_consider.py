#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""GitHub Actions headless: 考慮篩選（預設同 us_consider_screener_app.pyw）

AND：勾選關全部過先列入。預設全開：
  月/週 MCD(10/10) PC≥LC、MCDX(8/8) PC≥LC、主力 bull≥bear(9/36)
  日線：收市>$5、>EMA200、>EMA50、市值>1.5億、量>300k、KDJ J>50、EMA9>50>200
  剔 ETF/fund；宇宙＝Nasdaq Trader 全美股（非測試）
"""
from __future__ import annotations

import argparse
import io
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
import yfinance as yf

NASDAQ_LISTED = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
OTHER_LISTED = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"
BATCH = 150
PERIOD = "5y"
MCAP_WORKERS = 8

MCD_N, MCD_SMA = 10, 10
MCDX_N, MCDX_SMA = 8, 8
PRICE_MIN = 5.0
MCAP_YI = 1.5  # 億 → * 1e8
MCAP_MIN_USD = MCAP_YI * 1e8
VOL_MIN = 300_000

_FUND_NAME_RE = re.compile(
    r"\b(ETF|ETN|ETP|FUND|TRUST|INCOME FUND|BOND FUND)\b",
    re.IGNORECASE,
)


def _dir(url: str) -> pd.DataFrame:
    t = requests.get(url, timeout=60).text
    t = "\n".join(l for l in t.splitlines() if l and not l.startswith("File Creation"))
    return pd.read_csv(io.StringIO(t), sep="|", dtype=str)


def _norm_sym(s: str) -> str:
    return str(s).strip().upper().replace(".", "-")


def _is_etf_or_fund_row(row) -> bool:
    etf = str(row.get("ETF") or "").strip().upper()
    if etf == "Y":
        return True
    name = str(row.get("Security Name") or "")
    return bool(name and _FUND_NAME_RE.search(name))


def universe(exclude_etf_fund: bool = True) -> list[str]:
    """US listed equities; default drop ETF/fund (same as consider app)."""
    a = _dir(NASDAQ_LISTED).rename(columns={"Symbol": "sym"})
    b = _dir(OTHER_LISTED).rename(columns={"ACT Symbol": "sym"})
    pieces = []
    for df in (a, b):
        use = [c for c in ("sym", "Security Name", "Test Issue", "ETF") if c in df.columns]
        pieces.append(df[use])
    df = pd.concat(pieces, ignore_index=True).dropna(subset=["sym"])
    if "Test Issue" in df.columns:
        df = df[df["Test Issue"].fillna("N") == "N"]
    df = df[~df["sym"].astype(str).str.contains(r"[\$\^]", regex=True)]
    df = df.copy()
    df["sym"] = df["sym"].map(_norm_sym)
    df = df[df["sym"].astype(bool)]

    if not exclude_etf_fund:
        return sorted(set(df["sym"]))

    keep: set[str] = set()
    for _, row in df.iterrows():
        sym = row["sym"]
        if not _is_etf_or_fund_row(row):
            keep.add(sym)
    return sorted(keep)


def parse_symbols(text: str) -> list[str]:
    out, seen = [], set()
    for p in text.replace(",", " ").split():
        s = _norm_sym(p)
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def ny_now() -> datetime:
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("America/New_York")).replace(tzinfo=None)
    except Exception:
        return datetime.utcnow() - timedelta(hours=4)


def drop_forming(d: pd.DataFrame) -> pd.DataFrame:
    now = ny_now()
    last = pd.Timestamp(d.index[-1]).date()
    if last == now.date() and (now.hour, now.minute) < (16, 15):
        return d.iloc[:-1]
    return d


def drop_forming_weekly(w: pd.DataFrame) -> pd.DataFrame:
    """Legacy helper; weekly gates keep forming bars."""
    if w is None or w.empty:
        return w
    now = ny_now()
    last = pd.Timestamp(w.index[-1]).normalize()
    fri_close = datetime(last.year, last.month, last.day, 16, 15)
    if now < fri_close:
        return w.iloc[:-1]
    return w


def drop_forming_monthly(m: pd.DataFrame) -> pd.DataFrame:
    """Legacy helper; monthly gates keep forming bars."""
    if m is None or m.empty:
        return m
    now = ny_now()
    last = pd.Timestamp(m.index[-1])
    month_end = (last + pd.offsets.MonthEnd(0)).date()
    if now.date() < month_end:
        return m.iloc[:-1] if len(m) > 1 else m
    if now.date() == month_end and (now.hour, now.minute) < (16, 15):
        return m.iloc[:-1] if len(m) > 1 else m
    return m


def to_weekly(d: pd.DataFrame) -> pd.DataFrame:
    cols = {}
    for name, how in (
        ("Open", "first"),
        ("High", "max"),
        ("Low", "min"),
        ("Close", "last"),
        ("Volume", "sum"),
    ):
        if name in d.columns:
            cols[name] = how
    if "Close" not in cols:
        return pd.DataFrame()
    return d.resample("W-FRI").agg(cols).dropna(subset=["Close"])


def to_monthly(d: pd.DataFrame) -> pd.DataFrame:
    x = d.copy()
    x.index = pd.to_datetime(x.index)
    cols = {}
    for name, how in (
        ("Open", "first"),
        ("High", "max"),
        ("Low", "min"),
        ("Close", "last"),
        ("Volume", "sum"),
    ):
        if name in x.columns:
            cols[name] = how
    if "Close" not in cols:
        return pd.DataFrame()
    need = ["Close", "High", "Low"] if "High" in cols and "Low" in cols else ["Close"]
    return x.resample("ME").agg(cols).dropna(subset=need)


def _ohlc(d: pd.DataFrame):
    if isinstance(d.columns, pd.MultiIndex):
        d = d.copy()
        d.columns = d.columns.get_level_values(0)
    cols = {str(c).lower(): c for c in d.columns}
    for need in ("close", "high", "low"):
        if need not in cols:
            raise KeyError(need)
    return (
        pd.to_numeric(d[cols["close"]], errors="coerce"),
        pd.to_numeric(d[cols["high"]], errors="coerce"),
        pd.to_numeric(d[cols["low"]], errors="coerce"),
    )


def sma_cn(src: pd.Series, length: int) -> pd.Series:
    vals = src.astype(float).tolist()
    out: list[float] = []
    y = float("nan")
    for v in vals:
        if pd.isna(v):
            out.append(float("nan"))
            continue
        if pd.isna(y):
            y = float(v)
        else:
            y = (float(v) + (length - 1) * y) / length
        out.append(y)
    return pd.Series(out, index=src.index)


def kdj_j(df: pd.DataFrame) -> pd.Series:
    h = df["High"].astype(float)
    l = df["Low"].astype(float)
    c = df["Close"].astype(float)
    llv = l.rolling(9, min_periods=9).min()
    hhv = h.rolling(9, min_periods=9).max()
    rng = hhv - llv
    rsv = ((c - llv) / rng.replace(0, pd.NA) * 100).fillna(0)
    k = sma_cn(rsv, 3)
    d = sma_cn(k, 3)
    return 3 * k - 2 * d


def kdj_j_gt_50(df: pd.DataFrame) -> bool:
    if df is None or len(df) < 15:
        return False
    j = kdj_j(df)
    curr = j.iloc[-1]
    if pd.isna(curr):
        return False
    return float(curr) > 50.0


def fmcdx(price: pd.Series, h: pd.Series, l: pd.Series, n: int) -> pd.Series:
    lo = l.rolling(n, min_periods=n).min()
    hi = h.rolling(n, min_periods=n).max()
    rng = hi - lo
    step = (rng / 100.0).where(rng > 0, 1e-8)
    return ((price - lo) / step).clip(0, 100)


def chip_pc_ge_lc(df: pd.DataFrame, n: int, sma_len: int) -> bool:
    if df is None or df.empty or len(df) < n + sma_len:
        return False
    try:
        c, h, l = _ohlc(df)
    except KeyError:
        return False
    pc = fmcdx(c * 0.96, h, l, n)
    lc = 100.0 - fmcdx(c * 1.04, h, l, n)
    sp = float(pc.rolling(sma_len, min_periods=sma_len).mean().iloc[-1])
    sl = float(lc.rolling(sma_len, min_periods=sma_len).mean().iloc[-1])
    if pd.isna(sp) or pd.isna(sl):
        return False
    return sp >= sl


def zhuli_bull_ge_bear(ohlc: pd.DataFrame) -> bool:
    if ohlc is None or ohlc.empty or len(ohlc) < 36:
        return False
    try:
        c, h, l = _ohlc(ohlc)
    except KeyError:
        return False
    hh36 = h.rolling(36, min_periods=36).max()
    ll36 = l.rolling(36, min_periods=36).min()
    rng36 = hh36 - ll36
    var1 = (hh36 - c) / rng36 * 100.0
    # Keep NaN while HHV/LLV not ready; .where(rng>0,0) treats NaN as 0 and poisons SMA_CN
    var1 = var1.mask(rng36.eq(0) & rng36.notna(), 0.0)
    bear = sma_cn(var1, 3)
    hh9 = h.rolling(9, min_periods=9).max()
    ll9 = l.rolling(9, min_periods=9).min()
    rng9 = hh9 - ll9
    var2 = (c - ll9) / rng9 * 100.0
    var2 = var2.mask(rng9.eq(0) & rng9.notna(), 0.0)
    bull = sma_cn(var2, 5) - 8.0
    b, u = float(bear.iloc[-1]), float(bull.iloc[-1])
    if pd.isna(b) or pd.isna(u):
        return False
    return u >= b


def fetch_mcap(sym: str) -> float | None:
    try:
        t = yf.Ticker(sym)
        for getter in (
            lambda: getattr(t.fast_info, "market_cap", None)
            or getattr(t.fast_info, "marketCap", None),
            lambda: (t.info or {}).get("marketCap") or (t.info or {}).get("market_cap"),
        ):
            try:
                v = getter()
                if v is not None and float(v) > 0 and not pd.isna(float(v)):
                    return float(v)
            except Exception:
                pass
    except Exception:
        pass
    return None


def download(syms: list[str]) -> dict[str, pd.DataFrame]:
    out: dict[str, pd.DataFrame] = {}
    n = len(syms)
    for i in range(0, n, BATCH):
        bt = syms[i : i + BATCH]
        print(f"download {min(i + BATCH, n)}/{n}", flush=True)
        raw = None
        for a in range(3):
            try:
                raw = yf.download(
                    bt,
                    period=PERIOD,
                    interval="1d",
                    group_by="ticker",
                    auto_adjust=False,
                    threads=True,
                    progress=False,
                )
                break
            except Exception as e:
                print("retry", a, e, flush=True)
                time.sleep(1.5 * (a + 1))
        if raw is None or raw.empty:
            continue
        for s in bt:
            try:
                d = raw[s] if len(bt) > 1 else raw
                if isinstance(d.columns, pd.MultiIndex):
                    d.columns = d.columns.get_level_values(-1)
                d = d.dropna(subset=["Close", "High", "Low"])
                if not d.empty:
                    out[s] = d
            except Exception:
                pass
        time.sleep(0.15)
    return out


def eval_consider(
    d: pd.DataFrame, mcap: float | None
) -> tuple[bool, list[str], list[str]]:
    """AND all default gates. Returns (pass, passed_labels, failed_labels)."""
    passed: list[str] = []
    failed: list[str] = []

    def gate(label: str, ok: bool) -> None:
        if ok:
            passed.append(label)
        else:
            failed.append(label)

    if d is None or d.empty:
        return False, passed, ["無數據"]

    d = drop_forming(d)
    if d is None or d.empty:
        return False, passed, ["無已收市日線"]

    try:
        c, h, l = _ohlc(d)
        close = float(c.iloc[-1])
    except Exception:
        return False, passed, ["OHLC錯誤"]

    # Daily: price
    gate(f"價>${PRICE_MIN:g}", (not pd.isna(close)) and close > PRICE_MIN)

    # Daily: EMA200 / EMA50
    ok_e200 = False
    if not pd.isna(close) and len(d) >= 200:
        e200 = float(c.ewm(span=200, adjust=False, min_periods=200).mean().iloc[-1])
        ok_e200 = (not pd.isna(e200)) and close > e200
    gate("EMA200", ok_e200)

    ok_e50 = False
    if not pd.isna(close) and len(d) >= 50:
        e50 = float(c.ewm(span=50, adjust=False, min_periods=50).mean().iloc[-1])
        ok_e50 = (not pd.isna(e50)) and close > e50
    gate("EMA50", ok_e50)

    # Daily: mcap
    ok_mcap = mcap is not None and (not pd.isna(mcap)) and float(mcap) > MCAP_MIN_USD
    gate(f"市值>{MCAP_YI:g}億", ok_mcap)

    # Daily: volume
    ok_vol = False
    cols = {str(x).lower(): x for x in d.columns}
    if "volume" in cols:
        vol = pd.to_numeric(d[cols["volume"]], errors="coerce")
        last_vol = float(vol.iloc[-1]) if len(vol) else float("nan")
        ok_vol = (not pd.isna(last_vol)) and last_vol > VOL_MIN
    gate(f"量>{VOL_MIN}", ok_vol)

    # Daily: KDJ J>50
    gate("KDJ紅藍(J>50)", kdj_j_gt_50(d))

    # Daily: EMA9 > EMA50 > EMA200
    ok_stack = False
    if not pd.isna(close) and len(d) >= 200:
        e9 = float(c.ewm(span=9, adjust=False, min_periods=9).mean().iloc[-1])
        e50s = float(c.ewm(span=50, adjust=False, min_periods=50).mean().iloc[-1])
        e200s = float(c.ewm(span=200, adjust=False, min_periods=200).mean().iloc[-1])
        ok_stack = (
            not pd.isna(e9)
            and not pd.isna(e50s)
            and not pd.isna(e200s)
            and e9 > e50s > e200s
        )
    gate("EMA9>50>200", ok_stack)

    # Keep forming week/month bars (match TradingView current candle)
    w = to_weekly(d)
    m = to_monthly(d)

    gate("月MCD", bool(m is not None and not m.empty and chip_pc_ge_lc(m, MCD_N, MCD_SMA)))
    gate("月MCDX", bool(m is not None and not m.empty and chip_pc_ge_lc(m, MCDX_N, MCDX_SMA)))
    gate("月主力", bool(m is not None and not m.empty and zhuli_bull_ge_bear(m)))
    gate("週主力", bool(w is not None and not w.empty and zhuli_bull_ge_bear(w)))
    gate("週MCD", bool(w is not None and not w.empty and chip_pc_ge_lc(w, MCD_N, MCD_SMA)))
    gate("週MCDX", bool(w is not None and not w.empty and chip_pc_ge_lc(w, MCDX_N, MCDX_SMA)))

    ok = len(failed) == 0 and len(passed) > 0
    return ok, passed, failed


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="考慮篩選（headless）")
    ap.add_argument(
        "--symbols",
        default="",
        help="Comma/space symbols for smoke; empty = full US universe",
    )
    ap.add_argument(
        "--include-etf",
        action="store_true",
        help="Keep ETF/fund (default: exclude like the Tk app)",
    )
    ap.add_argument(
        "--no-report",
        action="store_true",
        help="Skip consider_report.csv",
    )
    ap.add_argument(
        "--out",
        default="consider_symbols.txt",
        help="Output symbols file (default consider_symbols.txt)",
    )
    args = ap.parse_args(argv)

    t0 = time.time()
    pasted = parse_symbols(args.symbols)
    if pasted:
        syms = pasted
        print(f"symbols={len(syms)} (cli)", flush=True)
    else:
        print("universe...", flush=True)
        syms = universe(exclude_etf_fund=not args.include_etf)
        note = "含ETF" if args.include_etf else "已剔ETF/fund"
        print(f"universe={len(syms)} ({note})", flush=True)

    data = download(syms)
    print(f"data={len(data)}", flush=True)

    print(f"mcap check {len(data)}...", flush=True)
    caps: dict[str, float | None] = {}
    with ThreadPoolExecutor(max_workers=MCAP_WORKERS) as ex:
        futs = {ex.submit(fetch_mcap, s): s for s in data}
        done = 0
        for fut in as_completed(futs):
            s = futs[fut]
            done += 1
            if done % 200 == 0:
                print(f"mcap {done}/{len(data)}", flush=True)
            try:
                caps[s] = fut.result()
            except Exception:
                caps[s] = None

    hits: list[str] = []
    rows: list[dict] = []
    for i, (s, d) in enumerate(data.items()):
        ok, passed, failed = eval_consider(d, caps.get(s))
        if ok:
            hits.append(s)
        if not args.no_report:
            rows.append(
                {
                    "symbol": s,
                    "pass": int(ok),
                    "passed": ",".join(passed),
                    "failed": ",".join(failed),
                    "mcap": caps.get(s),
                }
            )
        if i % 500 == 0:
            print(f"scan {i}/{len(data)} hits={len(hits)}", flush=True)

    hits = sorted(set(hits))
    asof = ny_now().strftime("%Y-%m-%d %H:%M ET")
    out = Path(args.out)
    out.write_text("\n".join(hits) + ("\n" if hits else ""), encoding="utf-8")

    if not args.no_report:
        rep = Path("consider_report.csv")
        pd.DataFrame(rows).sort_values(["pass", "symbol"], ascending=[False, True]).to_csv(
            rep, index=False
        )
        print(f"report={rep.resolve()}", flush=True)

    print("===== CONSIDER SYMBOLS BEGIN =====")
    print("\n".join(hits))
    print("===== CONSIDER SYMBOLS END =====")
    secs = int(time.time() - t0)
    print(
        f"count={len(hits)} universe={len(syms)} data={len(data)} "
        f"asof≈{asof} secs={secs} file={out.resolve()}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
