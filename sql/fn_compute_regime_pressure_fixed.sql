CREATE OR REPLACE FUNCTION regime.fn_compute_regime_pressure(p_inference_id uuid)
 RETURNS uuid
 LANGUAGE plpgsql
AS $function$
declare

  v_run_id uuid;

  v_trend numeric;
  v_vol numeric;
  v_liq numeric;
  v_shock numeric;
  v_risk numeric;

  v_break numeric;

  v_labels text[];

  v_inf record;

begin

  select *
  into v_inf
  from regime.inference_run_v1
  where inference_run_id = p_inference_id;

  if v_inf is null then
    raise exception 'Inference row not found';
  end if;


  -- Pressure calculations
  v_trend :=
      abs(v_inf.trend_score - v_inf.composite_score);

  v_vol :=
      v_inf.volatility_score;

  v_liq :=
      abs(v_inf.liquidity_score);

  v_shock :=
      v_inf.shock_score;

  v_risk :=
      v_inf.risk_off_score;


  -- Break probability
  v_break :=
        (v_trend * 0.20)
      + (v_vol * 0.25)
      + (v_liq * 0.20)
      + (v_shock * 0.20)
      + (v_risk * 0.15);


  v_break := greatest(0, least(1, v_break));


  -- Label logic
  v_labels := array[]::text[];

  if v_vol > 0.6 then
    v_labels := array_append(v_labels, 'VOL_PRESSURE');
  end if;

  if v_shock > 0.6 then
    v_labels := array_append(v_labels, 'SHOCK_PRESSURE');
  end if;

  if v_risk > 0.5 then
    v_labels := array_append(v_labels, 'RISK_OFF_PRESSURE');
  end if;

  if v_trend > 0.5 then
    v_labels := array_append(v_labels, 'TREND_EXHAUSTION');
  end if;

  if v_liq > 0.5 then
    v_labels := array_append(v_labels, 'LIQUIDITY_STRESS');
  end if;



  insert into regime.pressure_run_v1(
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
  returning pressure_run_id
  into v_run_id;


  insert into regime.pressure_state_v1(
    pressure_run_id,
    trend_pressure,
    volatility_pressure,
    liquidity_pressure,
    shock_pressure,
    risk_off_pressure,
    regime_break_probability,
    pressure_labels
  )
  values(
    v_run_id,
    v_trend,
    v_vol,
    v_liq,
    v_shock,
    v_risk,
    v_break,
    v_labels
  );

  return v_run_id;

end;
$function$
