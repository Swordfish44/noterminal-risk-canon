CREATE OR REPLACE FUNCTION regime.fn_run_opportunity_cycle()
RETURNS uuid
LANGUAGE plpgsql
AS $fn$
/*
  Chains the four regime pipeline steps in sequence:
    1. fn_build_inference_from_features  → writes regime.inference_run_v1
    2. fn_compute_regime_pressure        → writes pressure_run_v1 / pressure_state_v1
    3. fn_detect_market_shock            → writes shock_run_v1 / shock_state_v1
    4. fn_generate_opportunities         → writes opportunity_run_v1 / opportunity_signal_v1

  Returns the inference_run_id, or NULL if there was insufficient feature data.
  Safe to call repeatedly; each invocation produces a fresh timestamped set of rows.
*/
DECLARE
  v_inference_id uuid;
BEGIN

  v_inference_id := regime.fn_build_inference_from_features();

  IF v_inference_id IS NULL THEN
    RAISE NOTICE 'fn_run_opportunity_cycle: skipping — insufficient feature data';
    RETURN NULL;
  END IF;

  PERFORM regime.fn_compute_regime_pressure(v_inference_id);
  PERFORM regime.fn_detect_market_shock(v_inference_id);
  PERFORM regime.fn_generate_opportunities(v_inference_id);

  RETURN v_inference_id;
END;
$fn$;
