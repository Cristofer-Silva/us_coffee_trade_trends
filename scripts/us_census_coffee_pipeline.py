#!/usr/bin/env python3
"""Build a cleaned U.S. coffee trade dataset from U.S. Census trade APIs.

This script fetches monthly bilateral trade data for coffee HS codes, standardizes
imports and exports into a common schema, creates a complete yearly panel, and
emits missing-data reports.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


IMPORTS_ENDPOINT = "https://api.census.gov/data/timeseries/intltrade/imports/hs"
EXPORTS_ENDPOINT = "https://api.census.gov/data/timeseries/intltrade/exports/hs"


def _now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _to_snake_case(value: str) -> str:
    value = re.sub(r"[^A-Za-z0-9]+", "_", value.strip())
    value = re.sub(r"_+", "_", value)
    return value.strip("_").lower()


def _sum_min_count(series: pd.Series) -> float:
    return series.sum(min_count=1)


def _latest_non_empty(series: pd.Series) -> Optional[str]:
    cleaned = [x for x in series.tolist() if pd.notna(x) and str(x).strip() != ""]
    return cleaned[-1] if cleaned else None


def _coalesce_columns(df: pd.DataFrame, candidates: Sequence[str]) -> pd.Series:
    out = pd.Series(pd.NA, index=df.index, dtype="object")
    for col in candidates:
        if col not in df.columns:
            continue
        candidate = df[col]
        valid = candidate.notna()
        if candidate.dtype == object:
            valid = valid & candidate.astype(str).str.strip().ne("")
        mask = out.isna() & valid
        out.loc[mask] = candidate.loc[mask]
    return out


@dataclass(frozen=True)
class QueryProfile:
    """Candidate request shape for handling endpoint/schema differences."""

    name: str
    fields: Tuple[str, ...]
    time_mode: str  # "time" or "year_month"
    commodity_param: str


class CensusTradeClient:
    """Thin Census API client with retries and fallback query profiles."""

    def __init__(
        self,
        api_key: Optional[str],
        timeout_seconds: int,
        max_retries: int,
        backoff_factor: float,
        user_agent: str,
    ) -> None:
        self.api_key = api_key
        self.timeout_seconds = timeout_seconds
        self.calls_attempted = 0
        self.calls_successful = 0
        self.calls_no_content = 0
        self.calls_failed = 0

        retry = Retry(
            total=max_retries,
            connect=max_retries,
            read=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session = requests.Session()
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        self.session.headers.update(
            {
                "Accept": "application/json",
                "User-Agent": user_agent,
            }
        )

    def close(self) -> None:
        self.session.close()

    def fetch_records(
        self,
        endpoint: str,
        flow: str,
        hs_code: str,
        hs_level: str,
        year: int,
        month: int,
    ) -> List[Dict[str, str]]:
        """Fetch one month of data using profile fallback."""
        profiles = self._profiles_for_flow(flow)
        errors: List[str] = []

        for profile in profiles:
            params: Dict[str, str] = {
                "get": ",".join(profile.fields),
                "COMM_LVL": hs_level,
                profile.commodity_param: hs_code,
            }
            if profile.time_mode == "time":
                params["time"] = f"{year}-{month:02d}"
            else:
                params["YEAR"] = f"{year}"
                params["MONTH"] = f"{month:02d}"
            if self.api_key:
                params["key"] = self.api_key

            self.calls_attempted += 1
            response = self.session.get(endpoint, params=params, timeout=self.timeout_seconds)
            status = response.status_code

            if status == 204:
                self.calls_no_content += 1
                self.calls_successful += 1
                return []
            if status >= 400:
                self.calls_failed += 1
                msg = response.text.strip().replace("\n", " ")
                errors.append(
                    f"profile={profile.name}, status={status}, year={year}, "
                    f"month={month:02d}, hs={hs_code}, error={msg[:240]}"
                )
                continue

            try:
                payload = response.json()
            except json.JSONDecodeError as exc:
                self.calls_failed += 1
                errors.append(
                    f"profile={profile.name}, status={status}, invalid_json={exc}, "
                    f"year={year}, month={month:02d}, hs={hs_code}"
                )
                continue

            if not isinstance(payload, list) or not payload:
                self.calls_successful += 1
                return []

            headers = payload[0]
            rows = payload[1:]
            records: List[Dict[str, str]] = []
            for row in rows:
                if not isinstance(row, list):
                    continue
                record = {str(headers[i]): row[i] for i in range(min(len(headers), len(row)))}
                records.append(record)
            self.calls_successful += 1
            return records

        raise RuntimeError(
            "All request profiles failed for "
            f"{flow=} {hs_code=} {year=} {month=}. "
            + " | ".join(errors[:4])
        )

    @staticmethod
    def _profiles_for_flow(flow: str) -> Tuple[QueryProfile, ...]:
        if flow == "import":
            return (
                QueryProfile(
                    name="imports_current_time",
                    fields=(
                        "CTY_CODE",
                        "CTY_NAME",
                        "I_COMMODITY",
                        "COMM_LVL",
                        "GEN_VAL_MO",
                        "GEN_QY1_MO",
                        "GEN_QY1_MO_FLAG",
                        "UNIT_QY1",
                    ),
                    time_mode="time",
                    commodity_param="I_COMMODITY",
                ),
                QueryProfile(
                    name="imports_legacy_time",
                    fields=(
                        "CTY_CODE",
                        "CTY_NAME",
                        "COMM_CODE",
                        "COMM_LVL",
                        "GEN_VAL_MO",
                        "GEN_QY1_MO",
                        "GEN_QY1_FLAG",
                        "UNIT_QY1",
                    ),
                    time_mode="time",
                    commodity_param="COMM_CODE",
                ),
                QueryProfile(
                    name="imports_current_year_month",
                    fields=(
                        "CTY_CODE",
                        "CTY_NAME",
                        "I_COMMODITY",
                        "COMM_LVL",
                        "GEN_VAL_MO",
                        "GEN_QY1_MO",
                        "GEN_QY1_MO_FLAG",
                        "UNIT_QY1",
                        "YEAR",
                        "MONTH",
                    ),
                    time_mode="year_month",
                    commodity_param="I_COMMODITY",
                ),
            )
        return (
            QueryProfile(
                name="exports_current_time",
                fields=(
                    "CTY_CODE",
                    "CTY_NAME",
                    "E_COMMODITY",
                    "COMM_LVL",
                    "ALL_VAL_MO",
                    "QTY_1_MO",
                    "QTY_1_MO_FLAG",
                    "UNIT_QY1",
                ),
                time_mode="time",
                commodity_param="E_COMMODITY",
            ),
            QueryProfile(
                name="exports_legacy_time",
                fields=(
                    "CTY_CODE",
                    "CTY_NAME",
                    "COMM_CODE",
                    "COMM_LVL",
                    "ALL_VAL_MO",
                    "ALL_QY1_MO",
                    "ALL_QY1_FLAG",
                    "UNIT_QY1",
                ),
                time_mode="time",
                commodity_param="COMM_CODE",
            ),
            QueryProfile(
                name="exports_current_year_month",
                fields=(
                    "CTY_CODE",
                    "CTY_NAME",
                    "E_COMMODITY",
                    "COMM_LVL",
                    "ALL_VAL_MO",
                    "QTY_1_MO",
                    "QTY_1_MO_FLAG",
                    "UNIT_QY1",
                    "YEAR",
                    "MONTH",
                ),
                time_mode="year_month",
                commodity_param="E_COMMODITY",
            ),
        )


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch and clean U.S. Census coffee trade data (imports/exports)."
    )
    parser.add_argument("--start-year", type=int, required=True, help="First year to collect.")
    parser.add_argument("--end-year", type=int, required=True, help="Last year to collect.")
    parser.add_argument(
        "--coffee-codes",
        nargs="+",
        default=["0901"],
        help=(
            "HS code(s) to pull. Example: 0901 (HS4 all coffee) or "
            "090111 090112 090121 090122 090190 (HS6 detail)."
        ),
    )
    parser.add_argument(
        "--hs-level",
        default="HS4",
        help="Commodity level for COMM_LVL (default: HS4). Use HS6 for 6-digit codes.",
    )
    parser.add_argument(
        "--api-key",
        default=os.environ.get("CENSUS_API_KEY"),
        help="Census API key (optional, or set CENSUS_API_KEY env var).",
    )
    parser.add_argument(
        "--output-root",
        default="data",
        help="Root output directory (default: data).",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.05,
        help="Delay between API calls to reduce rate pressure.",
    )
    parser.add_argument("--timeout-seconds", type=int, default=30, help="HTTP timeout.")
    parser.add_argument("--max-retries", type=int, default=5, help="Max HTTP retries.")
    parser.add_argument(
        "--backoff-factor",
        type=float,
        default=0.5,
        help="Retry exponential backoff factor.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level.",
    )
    parser.add_argument(
        "--include-current-year",
        action="store_true",
        help="Allow running with end-year equal to current year (partial year risk).",
    )
    return parser.parse_args(argv)


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def validate_args(args: argparse.Namespace) -> None:
    current_year = datetime.now(timezone.utc).year
    if args.start_year > args.end_year:
        raise ValueError("start-year must be <= end-year.")
    if args.end_year > current_year:
        raise ValueError(f"end-year cannot exceed current year ({current_year}).")
    if args.end_year == current_year and not args.include_current_year:
        raise ValueError(
            "end-year is current year. Re-run with --include-current-year to allow partial-year data."
        )

    normalized_codes = []
    for code in args.coffee_codes:
        code_clean = str(code).strip()
        if not code_clean.isdigit():
            raise ValueError(f"Invalid HS code: {code!r}. HS codes must be digits.")
        normalized_codes.append(code_clean)
    args.coffee_codes = normalized_codes

    estimated_calls = (args.end_year - args.start_year + 1) * 12 * len(args.coffee_codes) * 2
    if estimated_calls > 500 and not args.api_key:
        logging.warning(
            "Estimated %s API calls and no API key provided. Census may enforce a 500/day limit.",
            estimated_calls,
        )


def ensure_output_dirs(output_root: Path) -> Dict[str, Path]:
    paths = {
        "raw": output_root / "raw",
        "processed": output_root / "processed",
        "reports": output_root / "reports",
    }
    for path in paths.values():
        path.mkdir(parents=True, exist_ok=True)
    return paths


def iter_year_month(start_year: int, end_year: int) -> Iterable[Tuple[int, int]]:
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            yield year, month


def fetch_all_monthly_records(args: argparse.Namespace) -> pd.DataFrame:
    client = CensusTradeClient(
        api_key=args.api_key,
        timeout_seconds=args.timeout_seconds,
        max_retries=args.max_retries,
        backoff_factor=args.backoff_factor,
        user_agent="global-coffee-trade-analytics/1.0",
    )

    records: List[Dict[str, str]] = []
    started_at = time.time()
    try:
        for flow, endpoint in (("import", IMPORTS_ENDPOINT), ("export", EXPORTS_ENDPOINT)):
            for hs_code in args.coffee_codes:
                logging.info("Pulling %s data for HS code %s", flow, hs_code)
                for year, month in iter_year_month(args.start_year, args.end_year):
                    month_records = client.fetch_records(
                        endpoint=endpoint,
                        flow=flow,
                        hs_code=hs_code,
                        hs_level=args.hs_level,
                        year=year,
                        month=month,
                    )
                    for record in month_records:
                        record["FLOW"] = flow
                        record["REQUEST_HS_CODE"] = hs_code
                        record["REQUEST_HS_LEVEL"] = args.hs_level
                        record["REQUEST_YEAR"] = str(year)
                        record["REQUEST_MONTH"] = f"{month:02d}"
                    records.extend(month_records)
                    if args.sleep_seconds > 0:
                        time.sleep(args.sleep_seconds)
    finally:
        elapsed = time.time() - started_at
        logging.info(
            "API summary: attempted=%s successful=%s no_content=%s failed=%s elapsed_sec=%.1f",
            client.calls_attempted,
            client.calls_successful,
            client.calls_no_content,
            client.calls_failed,
            elapsed,
        )
        client.close()

    if not records:
        raise RuntimeError("No records were returned. Check years, codes, and API accessibility.")
    return pd.DataFrame.from_records(records)


def drop_fully_empty_columns(
    df: pd.DataFrame, preserve: Optional[Sequence[str]] = None
) -> Tuple[pd.DataFrame, List[str]]:
    preserve_set = set(preserve or [])
    empty_cols: List[str] = []
    for col in df.columns:
        if col in preserve_set:
            continue
        series = df[col]
        if series.isna().all():
            empty_cols.append(col)
            continue
        if series.dtype == object:
            if series.fillna("").astype(str).str.strip().eq("").all():
                empty_cols.append(col)
    return df.drop(columns=empty_cols), empty_cols


def first_existing_column(df: pd.DataFrame, candidates: Sequence[str]) -> Optional[str]:
    candidate_set = {c.lower() for c in candidates}
    for col in df.columns:
        if col.lower() in candidate_set:
            return col
    return None


def build_monthly_clean(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = df_raw.copy()
    df.columns = [_to_snake_case(c) for c in df.columns]

    # Resolve date from either "time" or YEAR/MONTH fields.
    if "time" in df.columns:
        date_series = pd.to_datetime(df["time"], format="%Y-%m", errors="coerce")
    elif "year" in df.columns and "month" in df.columns:
        date_series = pd.to_datetime(
            df["year"].astype(str).str.zfill(4) + "-" + df["month"].astype(str).str.zfill(2),
            format="%Y-%m",
            errors="coerce",
        )
    elif "request_year" in df.columns and "request_month" in df.columns:
        date_series = pd.to_datetime(
            df["request_year"].astype(str).str.zfill(4)
            + "-"
            + df["request_month"].astype(str).str.zfill(2),
            format="%Y-%m",
            errors="coerce",
        )
    else:
        raise ValueError("Could not infer date columns. Expected time or YEAR/MONTH fields.")

    unit_col = first_existing_column(df, candidates=["unit_qy1", "unit"])

    required = ["flow", "cty_code", "cty_name", "request_hs_code"]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"Required columns missing from API payload: {missing}")

    flow = df["flow"].astype(str).str.strip().str.lower()
    is_import = flow.eq("import")
    is_export = flow.eq("export")

    import_hs = _coalesce_columns(df, ["i_commodity", "comm_code", "commodity", "request_hs_code"])
    export_hs = _coalesce_columns(df, ["e_commodity", "comm_code", "commodity", "request_hs_code"])
    hs_raw = pd.Series(pd.NA, index=df.index, dtype="object")
    hs_raw.loc[is_import] = import_hs.loc[is_import]
    hs_raw.loc[is_export] = export_hs.loc[is_export]
    hs_raw = hs_raw.fillna(df["request_hs_code"])

    import_value = _coalesce_columns(df, ["gen_val_mo", "gen_val_yr", "val"])
    export_value = _coalesce_columns(df, ["all_val_mo", "all_val_yr", "val"])
    value_raw = pd.Series(pd.NA, index=df.index, dtype="object")
    value_raw.loc[is_import] = import_value.loc[is_import]
    value_raw.loc[is_export] = export_value.loc[is_export]

    import_qty = _coalesce_columns(df, ["gen_qy1_mo", "gen_qy1_yr", "qy1"])
    export_qty = _coalesce_columns(df, ["qty_1_mo", "all_qy1_mo", "qty_1_yr", "all_qy1_yr", "qy1"])
    qty_raw = pd.Series(pd.NA, index=df.index, dtype="object")
    qty_raw.loc[is_import] = import_qty.loc[is_import]
    qty_raw.loc[is_export] = export_qty.loc[is_export]

    import_qty_flag = _coalesce_columns(df, ["gen_qy1_mo_flag", "gen_qy1_flag", "qy1_flag"])
    export_qty_flag = _coalesce_columns(
        df, ["qty_1_mo_flag", "all_qy1_mo_flag", "qty_1_flag", "all_qy1_flag", "qy1_flag"]
    )
    qty_flag_raw = pd.Series(pd.NA, index=df.index, dtype="object")
    qty_flag_raw.loc[is_import] = import_qty_flag.loc[is_import]
    qty_flag_raw.loc[is_export] = export_qty_flag.loc[is_export]

    hs_clean = hs_raw.astype("string").str.strip()
    hs_clean = hs_clean.str.replace(r"\.0$", "", regex=True)
    hs_clean = hs_clean.str.replace(r"[^0-9]", "", regex=True)
    hs_clean = hs_clean.mask(hs_clean.eq(""), pd.NA)

    clean = pd.DataFrame(
        {
            "flow": flow,
            "date": date_series,
            "year": date_series.dt.year,
            "month": date_series.dt.month,
            "partner_code": df["cty_code"].astype(str).str.strip(),
            "partner_name": df["cty_name"].astype(str).str.strip(),
            "hs_code": hs_clean,
            "hs_level": df.get("comm_lvl", df.get("request_hs_level", None)),
            "value_usd": pd.to_numeric(value_raw, errors="coerce"),
            "quantity_1": pd.to_numeric(qty_raw, errors="coerce"),
            "quantity_1_flag": qty_flag_raw.astype("string").str.strip(),
            "unit_qy1": df[unit_col].astype(str).str.strip() if unit_col else pd.NA,
            "source": "us_census_api",
        }
    )

    # Remove obvious aggregate/invalid partner rows.
    clean["partner_code"] = clean["partner_code"].str.replace(r"\.0$", "", regex=True)
    clean["partner_code"] = clean["partner_code"].str.zfill(4)
    drop_mask = (
        clean["partner_code"].isin({"0000", "000", "00", "-", ""})
        | clean["partner_name"].str.contains(r"^total|all countries", case=False, na=False)
        | clean["hs_code"].isna()
        | clean["date"].isna()
    )
    clean = clean.loc[~drop_mask].copy()

    clean, dropped_empty_cols = drop_fully_empty_columns(
        clean,
        preserve=[
            "flow",
            "date",
            "year",
            "month",
            "partner_code",
            "partner_name",
            "hs_code",
            "hs_level",
            "value_usd",
            "quantity_1",
            "quantity_1_flag",
            "unit_qy1",
            "source",
        ],
    )
    if dropped_empty_cols:
        logging.info("Dropped fully empty columns: %s", dropped_empty_cols)

    dedupe_keys = ["flow", "date", "partner_code", "hs_code"]
    before = len(clean)
    clean = clean.drop_duplicates(subset=dedupe_keys, keep="last")
    removed = before - len(clean)
    if removed > 0:
        logging.warning("Removed %s duplicate rows by keys %s", removed, dedupe_keys)

    clean = clean.sort_values(["flow", "hs_code", "partner_code", "date"]).reset_index(drop=True)
    return clean


def build_yearly_panel(monthly_clean: pd.DataFrame, start_year: int, end_year: int) -> pd.DataFrame:
    if "quantity_1_flag" not in monthly_clean.columns:
        monthly_clean = monthly_clean.copy()
        monthly_clean["quantity_1_flag"] = pd.NA

    agg = (
        monthly_clean.groupby(["flow", "hs_code", "partner_code", "year"], as_index=False)
        .agg(
            value_usd=("value_usd", _sum_min_count),
            quantity_1=("quantity_1", _sum_min_count),
            months_present=("month", "nunique"),
            quantity_flag_nonempty=(
                "quantity_1_flag",
                lambda s: int(s.fillna("").astype(str).str.strip().ne("").any()),
            ),
        )
        .reset_index(drop=True)
    )

    partner_lookup = (
        monthly_clean.groupby(["flow", "hs_code", "partner_code"], as_index=False)
        .agg(partner_name=("partner_name", _latest_non_empty))
        .reset_index(drop=True)
    )

    years_df = pd.DataFrame({"year": list(range(start_year, end_year + 1))})
    panel = partner_lookup.assign(_k=1).merge(years_df.assign(_k=1), on="_k").drop(columns="_k")
    panel = panel.merge(
        agg,
        on=["flow", "hs_code", "partner_code", "year"],
        how="left",
        validate="1:1",
    )

    panel["months_present"] = panel["months_present"].fillna(0).astype(int)
    panel["months_missing"] = (12 - panel["months_present"]).clip(lower=0)
    panel["has_full_year_data"] = panel["months_present"].eq(12).astype(int)

    panel["is_imputed_zero_value"] = panel["value_usd"].isna().astype(int)
    panel["value_usd"] = panel["value_usd"].fillna(0.0)

    panel["quantity_1_missing"] = panel["quantity_1"].isna().astype(int)
    panel["quantity_flag_nonempty"] = panel["quantity_flag_nonempty"].fillna(0).astype(int)

    panel = panel.sort_values(["flow", "hs_code", "partner_code", "year"]).reset_index(drop=True)
    return panel


def build_column_missingness_report(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    total = len(df)
    for col in df.columns:
        missing_count = int(df[col].isna().sum())
        rows.append(
            {
                "column": col,
                "missing_count": missing_count,
                "missing_pct": round(missing_count / total, 6) if total else 0.0,
            }
        )
    return pd.DataFrame(rows).sort_values("missing_pct", ascending=False).reset_index(drop=True)


def build_yearly_coverage_report(yearly_panel: pd.DataFrame) -> pd.DataFrame:
    coverage = (
        yearly_panel.groupby(["flow", "hs_code", "year"], as_index=False)
        .agg(
            partner_rows=("partner_code", "count"),
            partners_full_year=("has_full_year_data", "sum"),
            partners_with_any_months=("months_present", lambda s: int((s > 0).sum())),
            partners_imputed_zero=("is_imputed_zero_value", "sum"),
            avg_months_present=("months_present", "mean"),
        )
        .reset_index(drop=True)
    )
    coverage["pct_full_year"] = (
        coverage["partners_full_year"] / coverage["partner_rows"]
    ).round(6)
    coverage["pct_with_any_months"] = (
        coverage["partners_with_any_months"] / coverage["partner_rows"]
    ).round(6)
    return coverage


def write_outputs(
    output_dirs: Dict[str, Path],
    raw_df: pd.DataFrame,
    monthly_clean: pd.DataFrame,
    yearly_panel: pd.DataFrame,
    missing_columns: pd.DataFrame,
    yearly_coverage: pd.DataFrame,
    args: argparse.Namespace,
    run_started_utc: str,
) -> Dict[str, str]:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    raw_path = output_dirs["raw"] / "us_census_coffee_trade_raw.csv"
    monthly_path = output_dirs["processed"] / "us_coffee_trade_monthly_clean.csv"
    yearly_path = output_dirs["processed"] / "us_coffee_trade_yearly_panel.csv"
    missing_path = output_dirs["reports"] / "missingness_by_column.csv"
    coverage_path = output_dirs["reports"] / "yearly_panel_coverage.csv"
    summary_path = output_dirs["reports"] / f"run_summary_{timestamp}.json"

    raw_df.to_csv(raw_path, index=False)
    monthly_clean.to_csv(monthly_path, index=False)
    yearly_panel.to_csv(yearly_path, index=False)
    missing_columns.to_csv(missing_path, index=False)
    yearly_coverage.to_csv(coverage_path, index=False)

    summary = {
        "run_started_utc": run_started_utc,
        "run_finished_utc": _now_utc(),
        "args": {
            "start_year": args.start_year,
            "end_year": args.end_year,
            "coffee_codes": args.coffee_codes,
            "hs_level": args.hs_level,
            "output_root": str(args.output_root),
        },
        "row_counts": {
            "raw_rows": int(len(raw_df)),
            "monthly_clean_rows": int(len(monthly_clean)),
            "yearly_panel_rows": int(len(yearly_panel)),
        },
        "files": {
            "raw": str(raw_path),
            "monthly_clean": str(monthly_path),
            "yearly_panel": str(yearly_path),
            "missingness_by_column": str(missing_path),
            "yearly_panel_coverage": str(coverage_path),
        },
    }
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return {
        "raw": str(raw_path),
        "monthly": str(monthly_path),
        "yearly": str(yearly_path),
        "missing": str(missing_path),
        "coverage": str(coverage_path),
        "summary": str(summary_path),
    }


def main(argv: Optional[Sequence[str]] = None) -> int:
    run_started_utc = _now_utc()
    args = parse_args(argv)
    configure_logging(args.log_level)

    try:
        validate_args(args)
        output_root = Path(args.output_root)
        output_dirs = ensure_output_dirs(output_root)

        raw_df = fetch_all_monthly_records(args)
        monthly_clean = build_monthly_clean(raw_df)
        yearly_panel = build_yearly_panel(
            monthly_clean=monthly_clean,
            start_year=args.start_year,
            end_year=args.end_year,
        )
        missing_columns = build_column_missingness_report(monthly_clean)
        yearly_coverage = build_yearly_coverage_report(yearly_panel)
        paths = write_outputs(
            output_dirs=output_dirs,
            raw_df=raw_df,
            monthly_clean=monthly_clean,
            yearly_panel=yearly_panel,
            missing_columns=missing_columns,
            yearly_coverage=yearly_coverage,
            args=args,
            run_started_utc=run_started_utc,
        )

        logging.info("Finished successfully.")
        logging.info("Monthly clean: %s rows", len(monthly_clean))
        logging.info("Yearly panel: %s rows", len(yearly_panel))
        logging.info("Output files: %s", paths)
        return 0
    except Exception as exc:  # pragma: no cover
        logging.exception("Pipeline failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
