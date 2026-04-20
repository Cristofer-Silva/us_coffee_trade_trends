# global-coffee-trade-analytics
Collaborative data analytics project examining global coffee trade flows, pricing dynamics, and country-level import/export patterns.

## U.S. Coffee Trade Pipeline (Census API)

Production script:

- `/Users/lukemorrison/Documents/Project/global-coffee-trade-analytics/scripts/us_census_coffee_pipeline.py`

### 1) Install dependencies

```bash
cd /Users/lukemorrison/Documents/Project/global-coffee-trade-analytics
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2) Run for yearly-consistent history

The command below pulls monthly Census imports + exports and builds a complete yearly panel.

```bash
python scripts/us_census_coffee_pipeline.py \
  --start-year 2013 \
  --end-year 2025 \
  --coffee-codes 0901 \
  --hs-level HS4 \
  --output-root data
```

Notes:

- `0901` at `HS4` is all coffee.
- For more detail, use HS6 codes (for example `090111 090112 090121 090122 090190`) and set `--hs-level HS6`.
- For high call volumes, set `CENSUS_API_KEY` in your environment before running.

### 3) Outputs

The pipeline writes:

- `data/raw/us_census_coffee_trade_raw.csv`
- `data/processed/us_coffee_trade_monthly_clean.csv`
- `data/processed/us_coffee_trade_yearly_panel.csv`
- `data/reports/missingness_by_column.csv`
- `data/reports/yearly_panel_coverage.csv`
- `data/reports/run_summary_<timestamp>.json`

### Data quality and consistency rules

- Harmonizes imports/exports into one schema.
- Normalizes dates, partner codes, HS codes, and numeric types.
- Drops fully empty columns.
- Removes aggregate/total partner rows.
- De-duplicates at `flow + date + partner_code + hs_code`.
- Reports missingness by column.
- Builds a full partner-year panel for consistent yearly time-series points.
- Flags imputed zeros (`is_imputed_zero_value`) and missing quantity fields.
