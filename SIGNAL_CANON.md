# Noterminal Signal Canon — v1 (FROZEN)

**Status:** 🔒 FROZEN  
**Canon Tag:** signal-canon-v1  
**Effective Date:** 2026-01-04  

## Purpose
Defines how market intelligence is transformed into executable intent
through regime-aware signal generation and weighting.

This Canon governs **how models are allowed to influence capital**.

---

## Signal Types

- Momentum
- Mean Reversion
- Volatility
- Macro Overlay

Signals may be enabled or disabled, but **never altered in semantics**
without a Canon upgrade.

---

## Market Regimes

- **BULL**
- **BEAR**
- **NEUTRAL**
- **HIGH_VOL**

Regime classification precedes all signal weighting.

---

## Model Flow

Market Data  
→ Indicators  
→ Regime Classification  
→ Signal Weighting  
→ Allocation Intent  

No step may be skipped or bypassed.

---

## Weighting Rules

- Weights are regime-dependent
- Sum of active signal weights MUST equal **1.0**
- Disabled signals have **zero weight**
- No stochastic or adaptive weighting in production

---

## Enforcement Rules

- Signals produce intent, not execution
- Risk Canon may override or block intent
- Portfolio Canon governs execution eligibility

---

## Prohibited Behaviors

- ❌ Black-box overrides
- ❌ Manual weight intervention
- ❌ Live tuning in production
- ❌ Model-side capital control

---

## Canon Authority Rules

- This Canon supersedes model-specific logic
- All ensemble logic must comply with weight rules
- Regime changes must be auditable
- Signal output must be reproducible

---

## Change Policy

Any change to:
- Signal types
- Regime definitions
- Weighting semantics
- Model flow

➡️ **REQUIRES a new Canon version and tag**

---

🧠 *Signal Canon v1 defines the lawful path from data to decision in Noterminal.*
