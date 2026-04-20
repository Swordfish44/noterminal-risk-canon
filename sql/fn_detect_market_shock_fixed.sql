CREATE OR REPLACE FUNCTION regime.fn_detect_market_shock(p_inference_id uuid)
 RETURNS uuid
 LANGUAGE plpgsql
AS $function$
declare

  v_run uuid;

  v_inf record;

  v_vol numeric;
  v_liq numeric;
  v_acc numeric;
  v_score numeric;
  v_prob numeric;

  v_labels text[];

begin

  select *
  into v_inf
  from regime.inference_run_v1
  where inference_run_id = p_inference_id;

  if v_inf is null then
    raise exception 'Inference row not found';
  end if;

  ------------------------------------------------
  -- Shock components
  ------------------------------------------------

  v_vol :=
      v_inf.volatility_score;

  v_liq :=
      abs(v_inf.liquidity_score);

  v_acc :=
      abs(v_inf.trend_score * v_inf.composite_score);

  ------------------------------------------------
  -- Shock score
  ------------------------------------------------

  v_score :=
      (v_vol * 0.40)
    + (v_liq * 0.30)
    + (v_acc * 0.30);

  v_prob := greatest(0, least(1, v_score));

  ------------------------------------------------
  -- Labels
  ------------------------------------------------

  v_labels := array[]::text[];

  if v_vol > 0.65 then
    v_labels := array_append(v_labels, 'VOL_SPIKE');
  end if;

  if v_liq > 0.60 then
    v_labels := array_append(v_labels, 'LIQUIDITY_VACUUM');
  end if;

  if v_acc > 0.55 then
    v_labels := array_append(v_labels, 'PRICE_ACCELERATION');
  end if;

  if v_prob > 0.70 then
    v_labels := array_append(v_labels, 'MARKET_SHOCK');
  end if;

  ------------------------------------------------
  -- Insert run
  ------------------------------------------------

  insert into regime.shock_run_v1(
    inference_run_id,
    as_of_ts,
    asset_scope,
    regime_code
  )
  values(
    v_inf.inference_run_id,
    v_inf.as_of_ts,
    v_inf.asset_scope,
    v_inf.regime_code
  )
  returning shock_run_id
  into v_run;

  ------------------------------------------------
  -- Insert state
  ------------------------------------------------

  insert into regime.shock_state_v1(
    shock_run_id,
    volatility_spike,
    liquidity_vacuum,
    price_acceleration,
    shock_score,
    shock_probability,
    shock_labels
  )
  values(
    v_run,
    v_vol,
    v_liq,
    v_acc,
    v_score,
    v_prob,
    v_labels
  );

  return v_run;

end;
$function$
