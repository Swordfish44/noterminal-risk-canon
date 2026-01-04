# Noterminal Market Canon — v1 (FROZEN)

**Status:** 🔒 FROZEN  
**Canon Tag:** market-canon-v1  
**Effective Date:** 2026-01-04  

## Purpose
Defines the authoritative market data sources and truth hierarchy
used across Noterminal analytics, models, execution systems,
and historical backtests.

This Canon establishes **what data is allowed to be true**.

---

## Asset Coverage & Primary Providers

| Asset Class | Symbols (Examples) | Primary Provider |
|------------|-------------------|------------------|
| Crypto | BTC, ETH | Coinbase |
| Equities | AAPL, SPY | Yahoo Finance |
| FX | EUR/USD | ECB |
| Rates | ^TNX | Yahoo Finance |
| Metals | XAU, XAG | MetalPriceAPI |

---

## Provider Truth Hierarchy

1. **Primary Provider** (listed above)
2. **Secondary Provider** (only if explicitly added in a future Canon)
3. ❌ **No synthetic, inferred, or blended prices allowed**

If primary data is unavailable:
- The system **halts**
- No backfilling
- No substitution

---

## Data Requirements

- Daily OHLC required
- UTC-normalized timestamps
- One row per symbol per trading day
- Corporate actions handled at ingestion
- Missing days = **hard failure**
- No forward-filling across gaps

---

## Prohibited Behaviors

- ❌ Price interpolation
- ❌ Provider switching without Canon upgrade
- ❌ Retroactive symbol remapping
- ❌ Model-side data correction

---

## Canon Authority Rules

- This Canon supersedes all downstream assumptions
- Analytics must defer to Canon-listed providers
- Execution engines must reject non-canonical data
- Historical data must match Canon rules at time of ingestion

---

## Change Policy

Any change to:
- Data providers
- Asset coverage
- Truth hierarchy
- Data semantics

➡️ **REQUIRES a new Canon version and tag**

---

🧠 *Market Canon v1 is the single source of market truth for Noterminal.*
