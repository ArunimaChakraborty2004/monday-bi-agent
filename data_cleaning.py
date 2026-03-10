"""
Business data cleaning for monday.com board data.

Handles missing values, sector name normalization, and inconsistent date formats.
Designed to work with Deals and Work Orders boards with configurable column mapping.
"""

import json
import logging
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

# Common sector name variants mapped to canonical names
SECTOR_NORMALIZATION: dict[str, str] = {
    "energy": "Energy",
    "oil & gas": "Energy",
    "oil and gas": "Energy",
    "utilities": "Energy",
    "power": "Energy",
    "renewables": "Energy",
    "tech": "Technology",
    "technology": "Technology",
    "it": "Technology",
    "software": "Technology",
    "healthcare": "Healthcare",
    "health care": "Healthcare",
    "pharma": "Healthcare",
    "pharmaceuticals": "Healthcare",
    "finance": "Financial Services",
    "financial services": "Financial Services",
    "banking": "Financial Services",
    "manufacturing": "Manufacturing",
    "industrial": "Manufacturing",
    "retail": "Retail",
    "consumer": "Retail",
    "telecom": "Telecommunications",
    "telecommunications": "Telecommunications",
    "construction": "Construction",
    "real estate": "Real Estate",
    "real estate & property": "Real Estate",
    "government": "Government",
    "public sector": "Government",
    "education": "Education",
    "other": "Other",
    "unknown": "Other",
    "n/a": "Other",
    "-": "Other",
    "": "Other",
}

# Date format patterns (order matters; more specific first)
DATE_PATTERNS = [
    "%Y-%m-%d",
    "%d-%m-%Y",
    "%m-%d-%Y",
    "%d/%m/%Y",
    "%m/%d/%Y",
    "%Y/%m/%d",
    "%d %b %Y",
    "%d %B %Y",
    "%b %d, %Y",
    "%B %d, %Y",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%dT%H:%M:%S",
    "%d.%m.%Y",
    "%m.%d.%Y",
]


def _find_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """
    Return a best-effort matching column name from df.

    monday.com column titles often include punctuation, emojis, or extra words
    (e.g. "Sector / Industry", "Deal Value ($)", "Expected close date").
    This matcher tries:
    - exact case-insensitive match
    - normalized match (alnum-only)
    - substring match against normalized column names
    """

    def norm(s: str) -> str:
        return "".join(ch.lower() for ch in s if ch.isalnum())

    cols = list(df.columns)
    cols_lower = {str(c).lower(): c for c in cols}
    cols_norm = {norm(str(c)): c for c in cols}

    for cand in candidates:
        key = str(cand).lower()
        if key in cols_lower:
            return cols_lower[key]

    for cand in candidates:
        key = norm(str(cand))
        if key in cols_norm:
            return cols_norm[key]

    # Substring scan
    norm_cols_items = [(norm(str(c)), c) for c in cols]
    for cand in candidates:
        c_norm = norm(str(cand))
        if not c_norm:
            continue
        for col_norm, col in norm_cols_items:
            if c_norm in col_norm:
                return col

    return None


def _extract_numeric_from_monday_value(value: Any) -> float | None:
    """
    monday.com sometimes returns numbers/currency as JSON in `value` (stringified).
    Try to extract a numeric amount from common shapes.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None

    payload: Any = value
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        # If it's plain digits with symbols, let the caller handle it.
        if s[0] in "{[":
            try:
                payload = json.loads(s)
            except json.JSONDecodeError:
                return None
        else:
            return None

    if isinstance(payload, dict):
        for k in ("amount", "value", "number", "sum"):
            if k in payload:
                try:
                    v = payload[k]
                    # Sometimes nested like {"amount": {"value": 123}}
                    if isinstance(v, dict):
                        for kk in ("value", "amount", "number", "sum"):
                            if kk in v:
                                return float(v[kk])
                        return None
                    return float(v)
                except (TypeError, ValueError):
                    continue
        # Some columns nest values
        if "changed_at" in payload and len(payload) == 1:
            return None

    return None


def _parse_numeric_series(series: pd.Series) -> pd.Series:
    """
    Convert a mixed series (text numbers, currency strings, monday JSON) into float.
    """
    # First try: clean common currency formatting
    ser = series.copy()
    if ser.dtype == object or ser.dtype.name == "string":
        s = ser.astype(str).str.strip()
        # If it looks like JSON, try JSON extraction first
        extracted = s.apply(_extract_numeric_from_monday_value)
        if extracted.notna().any():
            return pd.to_numeric(extracted, errors="coerce")

        s = s.str.replace(r"[$€£,\s]", "", regex=True)
        return pd.to_numeric(s, errors="coerce")

    return pd.to_numeric(ser, errors="coerce")


def _parse_date_series(series: pd.Series) -> pd.Series:
    """Parse a series of date strings with multiple format attempts."""
    result = pd.Series(index=series.index, dtype="object")
    for idx, val in series.items():
        if pd.isna(val) or val == "" or (isinstance(val, str) and val.strip() == ""):
            result[idx] = pd.NaT
            continue
        val = str(val).strip()
        parsed = None
        for fmt in DATE_PATTERNS:
            try:
                parsed = pd.to_datetime(val, format=fmt)
                break
            except (ValueError, TypeError):
                continue
        if parsed is None:
            try:
                parsed = pd.to_datetime(val)
            except (ValueError, TypeError):
                result[idx] = pd.NaT
                continue
        result[idx] = parsed
    return pd.to_datetime(result, errors="coerce")


def normalize_sector(value: Any) -> str:
    """Normalize a single sector string to a canonical name."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "Other"
    s = str(value).strip().lower()
    if not s:
        return "Other"
    return SECTOR_NORMALIZATION.get(s, value.strip().title())


def clean_deals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the Deals board DataFrame.

    - Fills/drops missing values appropriately
    - Normalizes sector column (auto-detects name)
    - Parses date columns (created, close date, etc.)
    - Normalizes numeric columns (deal value, etc.)
    """
    if df.empty:
        return df

    df = df.copy()

    # Detect common column names (monday.com column titles may vary)
    sector_col = _find_column(df, ["Sector", "Industry", "Segment", "Customer sector"])
    if not sector_col:
        # Fuzzy: find any column that contains sector/industry wording
        sector_col = _find_column(df, ["sector", "industry"])
    date_cols_candidates = [
        "Create date",
        "Created",
        "Close date",
        "Closing date",
        "Date",
        "Deal date",
        "Expected close",
        "Expected close date",
        "created_at",
        "close_date",
    ]
    value_col = _find_column(
        df,
        [
            "Deal value",
            "Deal Value",
            "Value",
            "Amount",
            "Revenue",
            "Contract value",
            "deal_value",
        ],
    )
    if not value_col:
        value_col = _find_column(df, ["value", "amount", "revenue"])

    # If column-name matching fails, infer from data distribution.
    if not sector_col:
        best = None
        best_score = 0.0
        for col in df.columns:
            if col in ("id", "name"):
                continue
            try:
                normalized = df[col].apply(normalize_sector)
            except Exception:
                continue
            # Score: how many values look like real sectors (not Other)
            non_other = (normalized != "Other").mean()
            unique = normalized.nunique(dropna=True)
            score = float(non_other) * float(min(unique, 10))
            if score > best_score:
                best_score = score
                best = col
        if best_score >= 0.5:  # conservative threshold
            sector_col = best

    if not value_col:
        best = None
        best_score = 0.0
        for col in df.columns:
            if col in ("id", "name", "sector"):
                continue
            # Skip parsed date helper columns
            if str(col).lower().endswith("_parsed"):
                continue
            try:
                parsed = _parse_numeric_series(df[col])
            except Exception:
                continue

            non_na = float(parsed.notna().mean())
            if non_na < 0.05:
                continue

            positive = float((parsed.fillna(0) > 0).mean())
            magnitude = float(parsed.fillna(0).abs().quantile(0.9))

            # Favor columns that parse frequently, have positive values,
            # and look like meaningful magnitudes (not 0/1 flags).
            score = non_na * 0.6 + positive * 0.3 + (1.0 if magnitude >= 100 else 0.0) * 0.1

            if score > best_score:
                best_score = score
                best = col

        if best_score >= 0.25:
            value_col = best

    # Normalize sector
    if sector_col:
        df["sector"] = df[sector_col].apply(normalize_sector)
    else:
        df["sector"] = "Other"

    # Parse date columns
    for cand in date_cols_candidates:
        col = _find_column(df, [cand])
        if col and col != "sector":
            try:
                df[col + "_parsed"] = _parse_date_series(df[col].astype(str))
            except Exception as e:
                logger.warning("Could not parse dates for column %s: %s", col, e)

    # Normalize deal value to numeric
    if value_col:
        df["deal_value_numeric"] = _parse_numeric_series(df[value_col])

    # Missing values: drop rows where name is missing; fill sector with "Other" (already done)
    name_col = _find_column(df, ["name", "Name", "Title", "Deal name"])
    if name_col:
        df = df.dropna(subset=[name_col], how="all")
    df = df.fillna({"sector": "Other"})

    return df


def clean_work_orders(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the Work Orders board DataFrame.

    - Handles missing values
    - Normalizes sector/industry if present
    - Parses date columns
    """
    if df.empty:
        return df

    df = df.copy()

    sector_col = _find_column(df, ["Sector", "Industry", "Customer sector"])
    if not sector_col:
        sector_col = _find_column(df, ["sector", "industry"])
    if sector_col:
        df["sector"] = df[sector_col].apply(normalize_sector)
    else:
        df["sector"] = "Other"

    date_cols_candidates = [
        "Create date",
        "Created",
        "Due date",
        "Start date",
        "Date",
        "Completion date",
    ]
    for cand in date_cols_candidates:
        col = _find_column(df, [cand])
        if col:
            try:
                df[col + "_parsed"] = _parse_date_series(df[col].astype(str))
            except Exception as e:
                logger.warning("Could not parse dates for column %s: %s", col, e)

    name_col = _find_column(df, ["name", "Name", "Title", "Work order"])
    if name_col:
        df = df.dropna(subset=[name_col], how="all")
    df = df.fillna({"sector": "Other"})

    return df


def load_and_clean_deals(raw_items: list[dict[str, Any]]) -> pd.DataFrame:
    """Load raw Deals board items into a DataFrame and clean them."""
    df = pd.DataFrame(raw_items)
    return clean_deals(df)


def load_and_clean_work_orders(raw_items: list[dict[str, Any]]) -> pd.DataFrame:
    """Load raw Work Orders board items into a DataFrame and clean them."""
    df = pd.DataFrame(raw_items)
    return clean_work_orders(df)
