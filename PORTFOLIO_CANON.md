# Noterminal Portfolio Canon — v1 (FROZEN)

**Status:** 🔒 FROZEN  
**Canon Tag:** portfolio-canon-v1  
**Effective Date:** 2026-01-04  

## Purpose
Defines the lifecycle, structural invariants, and operational rules
for portfolios within the Noterminal system.

This Canon governs **how capital containers are created, operated,
and terminated**.

---

## Core Entities

- Portfolio
- Portfolio Asset
- Allocation State
- Risk Settings (bound to Risk Canon v1)

Each Portfolio represents a discrete, auditable capital container.

---

## Portfolio Lifecycle

1. **CREATED**
2. **SEEDED** (risk settings attached)
3. **FUNDED**
4. **ACTIVE**
5. **PAUSED**
6. **CLOSED** (terminal state)

Lifecycle transitions are **monotonic** and irreversible.

---

## Schema Invariants

- A Portfolio MUST have exactly one risk profile
- Allocations MUST sum to ≤ 100%
- No asset exists outside a Portfolio
- CLOSED portfolios are immutable
- Portfolio ID is the primary execution boundary

---

## Operational Rules

- Risk Canon constraints supersede Portfolio settings
- Execution is allowed ONLY in ACTIVE state
- PAUSED portfolios retain state but block execution
- Allocation changes require audit attribution

---

## Prohibited Behaviors

- ❌ Execution without risk settings
- ❌ Asset reassignment across portfolios
- ❌ Post-closure modification
- ❌ Retroactive lifecycle mutation

---

## Canon Authority Rules

- This Canon supersedes model-level assumptions
- Execution engines MUST enforce lifecycle state
- Analytics MUST respect CLOSED immutability
- Risk enforcement is mandatory, not advisory

---

## Change Policy

Any change to:
- Portfolio lifecycle states
- Schema invariants
- Execution permissions
- Risk binding semantics

➡️ **REQUIRES a new Canon version and tag**

---

🧠 *Portfolio Canon v1 defines the legal structure of capital in Noterminal.*
