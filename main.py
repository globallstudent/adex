from pathlib import Path
import pandas as pd
import numpy as np
import re

FILE = Path("31.08.2025 просрочка.xlsx")
OUT_DIR = Path("reports/cleaned_csv"); OUT_DIR.mkdir(exist_ok=True, parents=True)

def _standardize_columns(cols):
    norm, seen = [], {}
    for c in cols:
        s = str(c).strip().replace("\n", " ")
        s = re.sub(r"\s+", " ", s)
        if s == "" or s.lower().startswith("unnamed:"):
            s = "col"
        base = s; i = 1
        while s in seen:
            i += 1; s = f"{base}.{i}"
        seen[s] = True; norm.append(s)
    return norm

NBSP = "\xa0"

def _to_numeric_series(s: pd.Series) -> pd.Series:
    s = s.astype(str)
    s = (s.str.replace(NBSP, "", regex=False)
           .str.replace(" ", "", regex=False)
           .str.replace("\u2009", "", regex=False)
           .str.replace(",", ".", regex=False)
           .str.replace("\u2212", "-", regex=False))
    s = s.replace({"": np.nan, "nan": np.nan, "None": np.nan})
    return pd.to_numeric(s, errors="coerce")

def detect_numeric_cols(df: pd.DataFrame, thresh: float = 0.6):
    return [c for c in df.columns if _to_numeric_series(df[c]).notna().mean() >= thresh]

def detect_date_cols(df: pd.DataFrame, thresh: float = 0.6):
    out = []
    for c in df.columns:
        sample = df[c].dropna().astype(str).head(200)
        if sample.empty: 
            continue
        parsed = pd.to_datetime(sample, errors="coerce", dayfirst=True, infer_datetime_format=True)
        if parsed.notna().mean() >= thresh:
            out.append(c)
    return out

def clean_sheet(df_raw: pd.DataFrame):
    df = df_raw.dropna(how="all").copy()
    df = df.loc[:, df.notna().any(axis=0)]
    df.columns = _standardize_columns(df.columns)
    date_cols = detect_date_cols(df)
    num_cols  = detect_numeric_cols(df)
    for c in date_cols:
        df[c] = pd.to_datetime(df[c], errors="coerce", dayfirst=True, infer_datetime_format=True)
    for c in num_cols:
        if c not in date_cols:
            df[c] = _to_numeric_series(df[c])
    return df, date_cols, num_cols

xls = pd.ExcelFile(FILE)
summary = []
for sheet in xls.sheet_names:
    raw = pd.read_excel(FILE, sheet_name=sheet, dtype=str)
    cleaned, date_cols, num_cols = clean_sheet(raw)
    safe = re.sub(r"[^\w\-]+", "_", sheet)
    out_csv = OUT_DIR / f"{safe}.clean.csv"
    cleaned.to_csv(out_csv, index=False)
    summary.append({
        "sheet": sheet,
        "rows": len(cleaned),
        "cols": len(cleaned.columns),
        "date_cols": date_cols[:10],
        "numeric_cols_sample": num_cols[:12],
        "csv_path": str(out_csv),
    })

pd.DataFrame(summary)
