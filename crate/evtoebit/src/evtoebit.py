"""
Median EV/EBIT dan EV/EBITDA per sub-sektor IDX-IC (BEI).

Sumber data:
  1. Pemetaan emiten -> sub-sektor: file CSV/XLSX "Daftar Saham" yang diunduh dari
     https://www.idx.co.id/id/data-pasar/data-saham/daftar-saham
     (kolom minimal: Kode, Sektor, Sub Sektor). Bisa juga hasil GetCompanyProfiles.
  2. Harga, saham beredar, neraca, laba rugi: Yahoo Finance via yfinance (suffix .JK).

Pemakaian:
  pip install yfinance pandas openpyxl
  python median_ev_ebit_bei.py daftar_saham.xlsx --out median_ev_ebit.csv

Catatan metodologi:
  - EV = market cap + utang berbunga (termasuk lease) + NCI - kas & setara kas.
  - EBIT = Operating Income (TTM), EBITDA = EBIT + D&A (TTM dari arus kas).
  - Emiten dengan EBIT/EBITDA <= 0 dikeluarkan (multiple tak bermakna).
  - Bank, asuransi, multifinance dikeluarkan (EBIT tidak relevan).
  - Winsorize: multiple > 100x atau < 0 dibuang sebelum median.
  - Sub-sektor dengan < 3 emiten valid ditandai n<3 (median tidak andal).
"""

import argparse
import sys
import time

import pandas as pd
import yfinance as yf

FINANCIAL_KEYWORDS = ("bank", "asuransi", "insurance", "pembiayaan", "financ", "sekuritas")


def load_universe(path: str) -> pd.DataFrame:
    if path.lower().endswith((".xlsx", ".xls")):
        df = pd.read_excel(path)
    else:
        df = pd.read_csv(path)
    cols = {c.lower().strip(): c for c in df.columns}
    code_col = next((cols[c] for c in cols if c in ("kode", "code", "kode emiten", "ticker")), None)
    sub_col = next((cols[c] for c in cols if "sub" in c and "sektor" in c or c == "subsector"), None)
    sec_col = next((cols[c] for c in cols if c in ("sektor", "sector")), None)
    if code_col is None or sub_col is None:
        sys.exit(f"Kolom kode/sub-sektor tidak ditemukan. Kolom tersedia: {list(df.columns)}")
    out = pd.DataFrame({
        "kode": df[code_col].astype(str).str.strip().str.upper(),
        "sektor": df[sec_col] if sec_col else "",
        "sub_sektor": df[sub_col].astype(str).str.strip(),
    })
    mask = ~out["sub_sektor"].str.lower().str.contains("|".join(FINANCIAL_KEYWORDS))
    return out[mask].reset_index(drop=True)


def ttm_sum(df: pd.DataFrame, row_candidates: list[str]) -> float | None:
    """Jumlahkan 4 kuartal terakhir dari salah satu baris kandidat."""
    if df is None or df.empty:
        return None
    for name in row_candidates:
        if name in df.index:
            s = df.loc[name].dropna()
            if len(s) >= 4:
                return float(s.iloc[:4].sum())
            if len(s) > 0:
                return None  # data kuartalan tidak lengkap
    return None


def latest(df: pd.DataFrame, row_candidates: list[str]) -> float:
    if df is None or df.empty:
        return 0.0
    for name in row_candidates:
        if name in df.index:
            s = df.loc[name].dropna()
            if len(s):
                return float(s.iloc[0])
    return 0.0


def fetch_one(code: str) -> dict | None:
    t = yf.Ticker(f"{code}.JK")
    try:
        info = t.info or {}
        q_is = t.quarterly_income_stmt
        q_cf = t.quarterly_cashflow
        q_bs = t.quarterly_balance_sheet
    except Exception as e:  # noqa: BLE001
        return {"kode": code, "error": str(e)[:80]}

    mcap = info.get("marketCap")
    if not mcap:
        return {"kode": code, "error": "no marketCap"}

    ebit = ttm_sum(q_is, ["Operating Income", "EBIT"])
    da = ttm_sum(q_cf, ["Depreciation And Amortization", "Depreciation Amortization Depletion",
                        "Depreciation"])
    if ebit is None:
        return {"kode": code, "error": "no EBIT TTM"}
    ebitda = ebit + (da or 0.0)

    debt = latest(q_bs, ["Total Debt"])
    if debt == 0.0:
        debt = (latest(q_bs, ["Long Term Debt And Capital Lease Obligation", "Long Term Debt"])
                + latest(q_bs, ["Current Debt And Capital Lease Obligation", "Current Debt"]))
    nci = latest(q_bs, ["Minority Interest"])
    cash = latest(q_bs, ["Cash Cash Equivalents And Short Term Investments",
                         "Cash And Cash Equivalents"])

    ev = mcap + debt + nci - cash
    return {
        "kode": code,
        "market_cap": mcap,
        "debt": debt,
        "nci": nci,
        "cash": cash,
        "ev": ev,
        "ebit_ttm": ebit,
        "ebitda_ttm": ebitda,
        "ev_ebit": ev / ebit if ebit > 0 else None,
        "ev_ebitda": ev / ebitda if ebitda > 0 else None,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("universe", help="CSV/XLSX daftar saham dari idx.co.id")
    ap.add_argument("--out", default="median_ev_ebit.csv")
    ap.add_argument("--detail", default="detail_ev_ebit.csv")
    ap.add_argument("--sleep", type=float, default=0.4, help="jeda antar request (detik)")
    ap.add_argument("--max-multiple", type=float, default=100.0)
    args = ap.parse_args()

    uni = load_universe(args.universe)
    print(f"{len(uni)} emiten non-keuangan akan diproses", file=sys.stderr)

    rows = []
    for i, r in uni.iterrows():
        res = fetch_one(r["kode"])
        if res:
            res.update({"sektor": r["sektor"], "sub_sektor": r["sub_sektor"]})
            rows.append(res)
        if (i + 1) % 25 == 0:
            print(f"  {i + 1}/{len(uni)}", file=sys.stderr)
        time.sleep(args.sleep)

    det = pd.DataFrame(rows)
    det.to_csv(args.detail, index=False)

    ok = det[det["error"].isna()] if "error" in det else det
    for col in ("ev_ebit", "ev_ebitda"):
        ok = ok[(ok[col].isna()) | ((ok[col] > 0) & (ok[col] <= args.max_multiple))]

    agg = (ok.groupby(["sektor", "sub_sektor"])
             .agg(n=("kode", "count"),
                  median_ev_ebit=("ev_ebit", "median"),
                  p25_ev_ebit=("ev_ebit", lambda s: s.quantile(0.25)),
                  p75_ev_ebit=("ev_ebit", lambda s: s.quantile(0.75)),
                  median_ev_ebitda=("ev_ebitda", "median"))
             .reset_index()
             .sort_values(["sektor", "sub_sektor"]))
    agg["flag"] = agg["n"].apply(lambda n: "n<3" if n < 3 else "")
    agg = agg.round(2)
    agg.to_csv(args.out, index=False)

    pd.set_option("display.width", 200)
    print(agg.to_string(index=False))


if __name__ == "__main__":
    main()