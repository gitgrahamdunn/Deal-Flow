# Deal Flow MVP

Deal Flow builds a professional Excel workbook that ranks Alberta oil and gas **licensees/operators** by production intensity using public/free datasets.

## Features

- One-command CLI run with defaults for last 365 days.
- Dry-run mode using bundled sample data (no network required).
- Source connectors (modular): AER, Open Alberta, Petrinex, optional non-gov datasets.
- Download caching with metadata (checksum + ETag/Last-Modified when present).
- Canonical tables (`wells`, `facilities` placeholder, `production`) and operator metrics.
- Excel report with README, Summary, Operator Detail, Production (Monthly), Wells, and Data Quality.
- Terms summary included in workbook README sheet.

## Installation

```bash
python -m pip install -e .
python -m pip install -e .[dev]
```

## Run

```bash
python -m deal_flow run --dry-run
python -m deal_flow run --start 2025-01-01 --end 2026-02-01 --out ./output
```

### CLI options

- `--refresh`: force re-download.
- `--geo "TWP=xx RNG=yy MER=5"`: reserved for geo parser extension.
- `--bbox "minlon,minlat,maxlon,maxlat"`: reserved for bbox filtering extension.
- `--top N`: top-N operators in Summary.
- `--config configs/sources.yaml`: source config path.
- `--dry-run`: use bundled test fixtures.

## Data/terms safety

- Uses only configured public/free endpoints.
- Optional `other_free` sources are disabled by default and require clear terms.
- Workbook README includes source page and license notes from config.

## Project layout

```text
deal_flow/
  deal_flow/
    cli.py
    config.py
    io/
    sources/
    transform/
    excel/
    utils/
  tests/
  configs/sources.yaml
```

## Next enhancements

1. True mineral title holder joins (if a free downloadable registry becomes available).
2. ARO/liability signals by operator.
3. Decline metrics and PDP/PUD proxy analytics.
4. Play/formation tagging from reliable public formation mapping.
5. Outlier/anomaly detection on monthly production swings.
