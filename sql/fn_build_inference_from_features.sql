CREATE OR REPLACE FUNCTION regime.fn_build_inference_from_features()
RETURNS uuid
LANGUAGE plpgsql
AS $fn$
/*
  Reads the last 5 minutes of market.micro_features_1s_v1 (all symbols),
  derives GLOBAL regime scores from order-flow micro-structure, inserts a
  row into regime.inference_run_v1, and returns the new inference_run_id.

  Score derivations (all clamped to [0,1]):
  ─────────────────────────────────────────
  rel_ofi           = ofi / avg_trade_size  ≈ signed trade direction
                      (scale-invariant across BTC/ETH/SOL units)
  directionality    = SUM(rel_ofi) / SUM(ABS(rel_ofi))  ∈ [-1, +1]
                      +1 = all buying, -1 = all selling

  trend_score       = ABS(directionality)
  volatility_score  = (1 - ABS(directionality)) * 0.70   (choppiness proxy)
  liquidity_score   = 1 - LEAST(1, rows / (symbols * 30))  sparse = illiquid
  shock_score       = LEAST(1, STDDEV(tc)/AVG(tc)/2)        trade-rate CV
  risk_off_score    = sell_frac if available, else OFI-based fallback
*/
DECLARE
  v_n_rows         int;
  v_n_symbols      int;
  v_directionality numeric;
  v_avg_tc         float8;
  v_stddev_tc      float8;
  v_sell_frac      numeric;

  v_trend_score    numeric;
  v_vol_score      numeric;
  v_liq_score      numeric;
  v_shock_score    numeric;
  v_risk_off_score numeric;
  v_composite      numeric;
  v_confidence     numeric;
  v_regime_code    text;
  v_labels         text[];
  v_id             uuid;
BEGIN

  SELECT
    COUNT(*)::int,
    COUNT(DISTINCT symbol_id)::int,
    (SUM(ofi / avg_trade_size) /
       NULLIF(SUM(ABS(ofi / avg_trade_size)), 0))::numeric,
    AVG(trade_count)::float8,
    STDDEV(trade_count)::float8,
    CASE
      WHEN SUM(COALESCE(buy_vol, 0) + COALESCE(sell_vol, 0)) > 0
      THEN (SUM(COALESCE(sell_vol, 0)) /
              SUM(COALESCE(buy_vol, 0) + COALESCE(sell_vol, 0)))::numeric
      ELSE NULL
    END
  INTO
    v_n_rows, v_n_symbols, v_directionality,
    v_avg_tc, v_stddev_tc, v_sell_frac
  FROM market.micro_features_1s_v1
  WHERE bucket_ts  >= NOW() - INTERVAL '5 minutes'
    AND avg_trade_size > 0;

  IF COALESCE(v_n_rows, 0) < 5 THEN
    RAISE NOTICE 'fn_build_inference_from_features: insufficient data (rows=%)', v_n_rows;
    RETURN NULL;
  END IF;

  v_directionality := GREATEST(-1.0, LEAST(1.0, COALESCE(v_directionality, 0.0)));

  -- Scores
  v_trend_score := regime.fn_clamp01(ABS(v_directionality));

  v_vol_score := regime.fn_clamp01((1.0 - ABS(v_directionality)) * 0.70);

  -- 30 ticks per 5 min per symbol = 1 every 10 s = baseline "normal"
  v_liq_score := regime.fn_clamp01(
    1.0 - LEAST(1.0, v_n_rows::numeric / GREATEST(1, v_n_symbols * 30))
  );

  v_shock_score := regime.fn_clamp01(
    COALESCE(v_stddev_tc::numeric / NULLIF(v_avg_tc::numeric, 0), 0) / 2.0
  );

  v_risk_off_score := CASE
    WHEN v_sell_frac IS NOT NULL
      THEN regime.fn_clamp01((v_sell_frac - 0.30) / 0.70)
    ELSE
      regime.fn_clamp01(-v_directionality * 0.50 + 0.10)
  END;

  v_composite := regime.fn_clamp01(
      v_trend_score    * 0.35
    + v_vol_score      * 0.25
    + v_liq_score      * 0.15
    + v_shock_score    * 0.15
    + v_risk_off_score * 0.10
  );

  v_confidence := regime.fn_clamp01(
      v_trend_score         * 0.50
    + (1.0 - v_vol_score)   * 0.25
    + (1.0 - v_shock_score) * 0.25
  );

  v_regime_code := CASE
    WHEN v_shock_score    > 0.60                         THEN 'SHOCK'
    WHEN v_risk_off_score > 0.50 AND v_vol_score > 0.40  THEN 'RISK_OFF'
    WHEN v_trend_score    > 0.50 AND v_vol_score < 0.50  THEN 'TREND'
    WHEN v_vol_score      > 0.50                         THEN 'HIGH_VOL'
    WHEN v_liq_score      > 0.60                         THEN 'ILLIQUID'
    ELSE                                                      'NEUTRAL'
  END;

  v_labels := ARRAY[]::text[];
  v_labels := v_labels
    || CASE WHEN v_trend_score > 0.50 THEN 'TRENDING' ELSE 'RANGING' END
    || CASE WHEN v_vol_score   > 0.40 THEN 'HIGH_VOL'  ELSE 'LOW_VOL' END;
  IF v_shock_score    > 0.50 THEN v_labels := array_append(v_labels, 'SHOCK');    END IF;
  IF v_liq_score      > 0.60 THEN v_labels := array_append(v_labels, 'ILLIQUID'); END IF;
  IF v_risk_off_score > 0.40 THEN v_labels := array_append(v_labels, 'RISK_OFF'); END IF;

  INSERT INTO regime.inference_run_v1 (
    as_of_ts, asset_scope, regime_code,
    confidence_score, composite_score,
    trend_score, volatility_score, liquidity_score, shock_score, risk_off_score,
    labels, meta
  ) VALUES (
    NOW(), 'GLOBAL', v_regime_code,
    v_confidence, v_composite,
    v_trend_score, v_vol_score, v_liq_score, v_shock_score, v_risk_off_score,
    v_labels,
    jsonb_build_object(
      'source',         'fn_build_inference_from_features',
      'feature_rows',   v_n_rows,
      'symbols',        v_n_symbols,
      'directionality', v_directionality,
      'sell_frac',      v_sell_frac
    )
  )
  RETURNING inference_run_id INTO v_id;

  RETURN v_id;
END;
$fn$;
