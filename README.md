# Noterminal – Portfolio/Risk/Ensemble Canon v1

## Goal
A versioned, production-safe pipeline from:
signals → ensemble (v1 frozen / v2 extensible) → risk-capped sizing → paper trading → forward-test metrics.

## Modules

### 1) Signals
- Table: `signals.signal_daily`
- One row per (signal_name, signal_version, asof_date, symbol_id)
- Values should be standardized (z-score preferred)

### 2) Ensemble (versioned)
- Registry: `models.ensemble_versions`
- Members: `models.ensemble_members`
- Output: `models.ensemble_output_daily`
- Rule: `ensemble_v1` is frozen. New signals go into `ensemble_v2+`.

### 3) Earnings Overlay (optional)
- Calendar: `events.earnings_calendar`
- Surprise: `events.earnings_surprise`
- Overlayed score: `models.ensemble_with_earnings_overlay`

### 4) Portfolio Targets + Sizing
- Targets: `portfolio.target_weights` (raw_score + target_weight)
- Risk settings: `risk.portfolio_risk_settings`
- Sizing functions:
  - `risk.size_weights_basic(portfolio_id, asof_date, model_version)`
  - `risk.apply_sizing_basic(portfolio_id, asof_date, model_version)`

### 5) Live Paper / Forward Test
- Fills: `live.paper_fills`
- Metrics: `live.forwardtest_daily`

## Daily Runbook (Canonical)
1. Load prices + events
2. Compute signals (signal_daily)
3. Compute ensemble score (choose ensemble_v2)
4. Write portfolio.target_weights.raw_score for (portfolio_id, today, ensemble_v2)
5. Apply sizing + caps (apply_sizing_basic)
6. Create paper orders + simulate fills
7. Write forwardtest_daily metrics

## Invariants
- No edits to ensemble_v1 after freeze.
- Every portfolio must have a row in risk.portfolio_risk_settings.
- target_weight always respects: max position, gross cap, net cap.
flowchart TD
  A[prices_daily] --> B[signals.signal_daily]
  E[events.earnings_calendar] --> F[events.earnings_surprise]
  B --> C[models.ensemble_output_daily]
  F --> D[models.ensemble_with_earnings_overlay]
  C --> D

  D --> G[portfolio.target_weights.raw_score]
  H[risk.portfolio_risk_settings] --> I[risk.size_weights_basic]
  G --> I --> J[portfolio.target_weights.target_weight]

  J --> K[live.paper_fills]
  K --> L[live.forwardtest_daily]


