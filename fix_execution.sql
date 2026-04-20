BEGIN;

CREATE SCHEMA IF NOT EXISTS execution;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

SET LOCAL client_min_messages = notice;
SET LOCAL lock_timeout = '10s';
SET LOCAL statement_timeout = '0';

DO $$
BEGIN
  IF to_regclass('execution.edge_cost_observation_v1') IS NULL THEN
    RAISE EXCEPTION 'execution.edge_cost_observation_v1 does not exist';
  END IF;

  IF to_regclass('execution.edge_execution_fill_v1') IS NULL THEN
    RAISE EXCEPTION 'execution.edge_execution_fill_v1 does not exist';
  END IF;

  IF to_regclass('execution.v_execution_fill_clean_v1') IS NULL THEN
    RAISE EXCEPTION 'execution.v_execution_fill_clean_v1 does not exist';
  END IF;

  IF to_regclass('execution.venue_cost_model_v1') IS NULL THEN
    RAISE EXCEPTION 'execution.venue_cost_model_v1 does not exist';
  END IF;
END
$$;

DO $$
DECLARE
  backup_name text := format('edge_cost_observation_v1_backup_%s', to_char(clock_timestamp(), 'YYYYMMDD_HH24MISS'));
BEGIN
  EXECUTE format(
    'CREATE TABLE IF NOT EXISTS execution.%I AS TABLE execution.edge_cost_observation_v1 WITH DATA',
    backup_name
  );
END
$$;

DROP TABLE IF EXISTS pg_temp._exec_saved_views;
CREATE TEMP TABLE _exec_saved_views (
  schemaname text,
  viewname text,
  definition text,
  depth int
) ON COMMIT DROP;

TRUNCATE _exec_saved_views;

DO $$
DECLARE
  target_oid oid;
BEGIN
  SELECT c.oid
    INTO target_oid
  FROM pg_class c
  JOIN pg_namespace n
    ON n.oid = c.relnamespace
  WHERE n.nspname = 'execution'
    AND c.relname = 'edge_cost_observation_v1'
    AND c.relkind IN ('r','p');

  IF target_oid IS NULL THEN
    RETURN;
  END IF;

  WITH RECURSIVE dep_views AS (
    SELECT c.oid, n.nspname AS schemaname, c.relname AS viewname, 1 AS depth
    FROM pg_depend d
    JOIN pg_rewrite r
      ON r.oid = d.objid
    JOIN pg_class c
      ON c.oid = r.ev_class
    JOIN pg_namespace n
      ON n.oid = c.relnamespace
    WHERE d.refobjid = target_oid
      AND c.relkind = 'v'
      AND n.nspname = 'execution'
    UNION ALL
    SELECT c2.oid, n2.nspname, c2.relname, dv.depth + 1
    FROM dep_views dv
    JOIN pg_depend d2
      ON d2.refobjid = dv.oid
    JOIN pg_rewrite r2
      ON r2.oid = d2.objid
    JOIN pg_class c2
      ON c2.oid = r2.ev_class
    JOIN pg_namespace n2
      ON n2.oid = c2.relnamespace
    WHERE c2.relkind = 'v'
      AND n2.nspname = 'execution'
  )
  INSERT INTO _exec_saved_views (schemaname, viewname, definition, depth)
  SELECT v.schemaname, v.viewname, v.definition, max(dv.depth)
  FROM dep_views dv
  JOIN pg_views v
    ON v.schemaname = dv.schemaname
   AND v.viewname = dv.viewname
  GROUP BY v.schemaname, v.viewname, v.definition;
END
$$;

DO $$
DECLARE
  r record;
BEGIN
  FOR r IN
    SELECT DISTINCT schemaname, viewname
    FROM _exec_saved_views
  LOOP
    EXECUTE format('DROP VIEW IF EXISTS %I.%I CASCADE', r.schemaname, r.viewname);
  END LOOP;
END
$$;

DO $$
DECLARE
  c record;
BEGIN
  FOR c IN
    SELECT tg.tgname
    FROM pg_trigger tg
    JOIN pg_class cls
      ON cls.oid = tg.tgrelid
    JOIN pg_namespace ns
      ON ns.oid = cls.relnamespace
    JOIN pg_proc p
      ON p.oid = tg.tgfoid
    WHERE ns.nspname = 'execution'
      AND cls.relname = 'edge_cost_observation_v1'
      AND NOT tg.tgisinternal
      AND (
           upper(pg_get_triggerdef(tg.oid, true)) LIKE '%BEFORE INSERT%'
        OR upper(pg_get_triggerdef(tg.oid, true)) LIKE '%BEFORE UPDATE%'
        OR upper(pg_get_triggerdef(tg.oid, true)) LIKE '%INSERT OR UPDATE%'
      )
  LOOP
    EXECUTE format('ALTER TABLE execution.edge_cost_observation_v1 DISABLE TRIGGER %I', c.tgname);
  END LOOP;
END
$$;

DROP FUNCTION IF EXISTS execution.fn_upsert_edge_cost_observation_v1();
DROP FUNCTION IF EXISTS execution.fn_refresh_venue_cost_model_v1();
DROP FUNCTION IF EXISTS execution.fn_normalize_edge_cost_observation_v1();

DO $$
DECLARE
  coltype text;
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'execution'
      AND table_name   = 'edge_cost_observation_v1'
      AND column_name  = 'observation_id'
  ) THEN
    EXECUTE 'ALTER TABLE execution.edge_cost_observation_v1 ALTER COLUMN observation_id SET DEFAULT gen_random_uuid()';
    EXECUTE 'UPDATE execution.edge_cost_observation_v1 SET observation_id = gen_random_uuid() WHERE observation_id IS NULL';
    EXECUTE 'ALTER TABLE execution.edge_cost_observation_v1 ALTER COLUMN observation_id SET NOT NULL';
  END IF;

  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'execution'
      AND table_name   = 'edge_cost_observation_v1'
      AND column_name  = 'fill_id'
  ) THEN
    SELECT data_type
      INTO coltype
    FROM information_schema.columns
    WHERE table_schema = 'execution'
      AND table_name   = 'edge_cost_observation_v1'
      AND column_name  = 'fill_id';

    IF coltype = 'uuid' THEN
      EXECUTE 'UPDATE execution.edge_cost_observation_v1 SET fill_id = gen_random_uuid() WHERE fill_id IS NULL';
      EXECUTE 'ALTER TABLE execution.edge_cost_observation_v1 ALTER COLUMN fill_id SET NOT NULL';
    END IF;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'execution'
      AND table_name   = 'edge_cost_observation_v1'
      AND column_name  = 'observed_at'
  ) THEN
    EXECUTE 'ALTER TABLE execution.edge_cost_observation_v1 ALTER COLUMN observed_at DROP DEFAULT';
    EXECUTE 'ALTER TABLE execution.edge_cost_observation_v1 ALTER COLUMN observed_at TYPE timestamptz USING observed_at::timestamptz';
    EXECUTE 'ALTER TABLE execution.edge_cost_observation_v1 ALTER COLUMN observed_at SET DEFAULT now()';
    EXECUTE 'UPDATE execution.edge_cost_observation_v1 SET observed_at = now() WHERE observed_at IS NULL';
    EXECUTE 'ALTER TABLE execution.edge_cost_observation_v1 ALTER COLUMN observed_at SET NOT NULL';
  END IF;

  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'execution'
      AND table_name   = 'edge_cost_observation_v1'
      AND column_name  = 'instrument_symbol'
  ) THEN
    EXECUTE 'UPDATE execution.edge_cost_observation_v1 SET instrument_symbol = ''UNKNOWN'' WHERE instrument_symbol IS NULL OR btrim(instrument_symbol) = ''''''';
    EXECUTE 'ALTER TABLE execution.edge_cost_observation_v1 ALTER COLUMN instrument_symbol SET NOT NULL';
  END IF;

  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'execution'
      AND table_name   = 'edge_cost_observation_v1'
      AND column_name  = 'venue'
  ) THEN
    EXECUTE 'UPDATE execution.edge_cost_observation_v1 SET venue = ''UNKNOWN'' WHERE venue IS NULL OR btrim(venue) = ''''''';
    EXECUTE 'ALTER TABLE execution.edge_cost_observation_v1 ALTER COLUMN venue SET NOT NULL';
  END IF;

  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'execution'
      AND table_name   = 'edge_cost_observation_v1'
      AND column_name  = 'total_cost_bps'
  ) THEN
    BEGIN
      EXECUTE 'ALTER TABLE execution.edge_cost_observation_v1 ALTER COLUMN total_cost_bps DROP EXPRESSION';
    EXCEPTION
      WHEN others THEN
        NULL;
    END;

    EXECUTE 'ALTER TABLE execution.edge_cost_observation_v1 ALTER COLUMN total_cost_bps DROP DEFAULT';
    EXECUTE $q$
      ALTER TABLE execution.edge_cost_observation_v1
      ALTER COLUMN total_cost_bps TYPE numeric
      USING CASE
        WHEN total_cost_bps IS NULL THEN NULL
        WHEN total_cost_bps::text ~ '^\s*[-+]?\d+(\.\d+)?\s*$' THEN total_cost_bps::numeric
        ELSE NULL
      END
    $q$;
    EXECUTE 'ALTER TABLE execution.edge_cost_observation_v1 ALTER COLUMN total_cost_bps DROP NOT NULL';
  END IF;
END
$$;

DO $$
DECLARE
  c record;
BEGIN
  FOR c IN
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'execution'
      AND table_name   = 'edge_cost_observation_v1'
      AND column_name IN (
        'spread_bps',
        'slippage_bps',
        'fee_bps',
        'commission_bps',
        'impact_bps',
        'rebate_bps',
        'shrinkage_bps',
        'shrinkage_applied'
      )
  LOOP
    EXECUTE format('ALTER TABLE execution.edge_cost_observation_v1 ALTER COLUMN %I DROP DEFAULT', c.column_name);
    EXECUTE format($f$
      ALTER TABLE execution.edge_cost_observation_v1
      ALTER COLUMN %I TYPE numeric
      USING CASE
        WHEN %I IS NULL THEN NULL
        WHEN %I::text IN ('true','false') THEN CASE WHEN %I::text = 'true' THEN 1 ELSE 0 END
        WHEN %I::text ~ '^\s*[-+]?\d+(\.\d+)?\s*$' THEN (%I::text)::numeric
        ELSE NULL
      END
    $f$, c.column_name, c.column_name, c.column_name, c.column_name, c.column_name, c.column_name);
  END LOOP;

  FOR c IN
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'execution'
      AND table_name   = 'edge_cost_observation_v1'
      AND (
           column_name LIKE '%\_ts' ESCAPE '\'
        OR column_name LIKE '%\_at' ESCAPE '\'
        OR column_name IN ('decision_ts', 'fill_ts')
      )
      AND column_name <> 'observed_at'
  LOOP
    EXECUTE format('ALTER TABLE execution.edge_cost_observation_v1 ALTER COLUMN %I DROP DEFAULT', c.column_name);
    EXECUTE format(
      'ALTER TABLE execution.edge_cost_observation_v1 ALTER COLUMN %I TYPE timestamptz USING %I::timestamptz',
      c.column_name,
      c.column_name
    );
  END LOOP;
END
$$;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'execution'
      AND table_name   = 'edge_cost_observation_v1'
      AND column_name  = 'fill_id'
  ) THEN
    IF NOT EXISTS (
      SELECT 1
      FROM pg_indexes
      WHERE schemaname = 'execution'
        AND tablename  = 'edge_cost_observation_v1'
        AND indexname  = 'ux_edge_cost_observation_v1_fill_id'
    ) THEN
      EXECUTE 'CREATE UNIQUE INDEX ux_edge_cost_observation_v1_fill_id ON execution.edge_cost_observation_v1(fill_id)';
    END IF;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'execution'
      AND table_name   = 'venue_cost_model_v1'
      AND column_name  = 'venue'
  )
  AND EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'execution'
      AND table_name   = 'venue_cost_model_v1'
      AND column_name  = 'instrument_symbol'
  ) THEN
    IF NOT EXISTS (
      SELECT 1
      FROM pg_indexes
      WHERE schemaname = 'execution'
        AND tablename  = 'venue_cost_model_v1'
        AND indexname  = 'ix_venue_cost_model_v1_venue_symbol'
    ) THEN
      EXECUTE 'CREATE INDEX ix_venue_cost_model_v1_venue_symbol ON execution.venue_cost_model_v1(venue, instrument_symbol)';
    END IF;
  END IF;
END
$$;

CREATE OR REPLACE FUNCTION execution.fn_upsert_edge_cost_observation_v1()
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  src_table text := 'v_execution_fill_clean_v1';
  rows_inserted integer := 0;
  has_fill_id boolean;
  has_observed_at boolean;
  has_decision_ts boolean;
  has_fill_ts boolean;
  has_instrument_symbol boolean;
  has_symbol boolean;
  has_venue boolean;
  has_exchange boolean;
  has_total_cost_bps boolean;
  has_cost_bps boolean;
  has_fee_bps boolean;
  has_slippage_bps boolean;
  has_rebate_bps boolean;
  sql text;
BEGIN
  IF to_regclass('execution.edge_cost_observation_v1') IS NULL THEN
    RETURN 0;
  END IF;

  IF to_regclass('execution.v_execution_fill_clean_v1') IS NULL THEN
    RETURN 0;
  END IF;

  SELECT EXISTS (
           SELECT 1
           FROM information_schema.columns
           WHERE table_schema='execution' AND table_name=src_table AND column_name='fill_id'
         ),
         EXISTS (
           SELECT 1
           FROM information_schema.columns
           WHERE table_schema='execution' AND table_name=src_table AND column_name='observed_at'
         ),
         EXISTS (
           SELECT 1
           FROM information_schema.columns
           WHERE table_schema='execution' AND table_name=src_table AND column_name='decision_ts'
         ),
         EXISTS (
           SELECT 1
           FROM information_schema.columns
           WHERE table_schema='execution' AND table_name=src_table AND column_name='fill_ts'
         ),
         EXISTS (
           SELECT 1
           FROM information_schema.columns
           WHERE table_schema='execution' AND table_name=src_table AND column_name='instrument_symbol'
         ),
         EXISTS (
           SELECT 1
           FROM information_schema.columns
           WHERE table_schema='execution' AND table_name=src_table AND column_name='symbol'
         ),
         EXISTS (
           SELECT 1
           FROM information_schema.columns
           WHERE table_schema='execution' AND table_name=src_table AND column_name='venue'
         ),
         EXISTS (
           SELECT 1
           FROM information_schema.columns
           WHERE table_schema='execution' AND table_name=src_table AND column_name='exchange'
         ),
         EXISTS (
           SELECT 1
           FROM information_schema.columns
           WHERE table_schema='execution' AND table_name=src_table AND column_name='total_cost_bps'
         ),
         EXISTS (
           SELECT 1
           FROM information_schema.columns
           WHERE table_schema='execution' AND table_name=src_table AND column_name='cost_bps'
         ),
         EXISTS (
           SELECT 1
           FROM information_schema.columns
           WHERE table_schema='execution' AND table_name=src_table AND column_name='fee_bps'
         ),
         EXISTS (
           SELECT 1
           FROM information_schema.columns
           WHERE table_schema='execution' AND table_name=src_table AND column_name='slippage_bps'
         ),
         EXISTS (
           SELECT 1
           FROM information_schema.columns
           WHERE table_schema='execution' AND table_name=src_table AND column_name='rebate_bps'
         )
    INTO has_fill_id,
         has_observed_at,
         has_decision_ts,
         has_fill_ts,
         has_instrument_symbol,
         has_symbol,
         has_venue,
         has_exchange,
         has_total_cost_bps,
         has_cost_bps,
         has_fee_bps,
         has_slippage_bps,
         has_rebate_bps;

  IF NOT has_fill_id THEN
    RETURN 0;
  END IF;

  sql := format($fmt$
    INSERT INTO execution.edge_cost_observation_v1 (
      observation_id,
      fill_id,
      observed_at,
      instrument_symbol,
      venue,
      total_cost_bps
    )
    SELECT
      gen_random_uuid(),
      s.fill_id,
      %s,
      %s,
      %s,
      %s
    FROM execution.v_execution_fill_clean_v1 s
    WHERE s.fill_id IS NOT NULL
      AND %s IS NOT NULL
      AND NOT EXISTS (
        SELECT 1
        FROM execution.edge_cost_observation_v1 t
        WHERE t.fill_id = s.fill_id
      )
  $fmt$,
    CASE
      WHEN has_observed_at THEN 'COALESCE(s.observed_at::timestamptz, now())'
      WHEN has_decision_ts THEN 'COALESCE(s.decision_ts::timestamptz, now())'
      WHEN has_fill_ts THEN 'COALESCE(s.fill_ts::timestamptz, now())'
      ELSE 'now()'
    END,
    CASE
      WHEN has_instrument_symbol THEN 'COALESCE(NULLIF(s.instrument_symbol::text, ''''), ''UNKNOWN'')'
      WHEN has_symbol THEN 'COALESCE(NULLIF(s.symbol::text, ''''), ''UNKNOWN'')'
      ELSE '''UNKNOWN'''
    END,
    CASE
      WHEN has_venue THEN 'COALESCE(NULLIF(s.venue::text, ''''), ''UNKNOWN'')'
      WHEN has_exchange THEN 'COALESCE(NULLIF(s.exchange::text, ''''), ''UNKNOWN'')'
      ELSE '''UNKNOWN'''
    END,
    CASE
      WHEN has_total_cost_bps THEN 's.total_cost_bps::numeric'
      WHEN has_cost_bps THEN 's.cost_bps::numeric'
      WHEN has_fee_bps OR has_slippage_bps OR has_rebate_bps THEN
        format(
          '%s + %s - %s',
          CASE WHEN has_fee_bps THEN 'COALESCE(s.fee_bps::numeric,0)' ELSE '0::numeric' END,
          CASE WHEN has_slippage_bps THEN 'COALESCE(s.slippage_bps::numeric,0)' ELSE '0::numeric' END,
          CASE WHEN has_rebate_bps THEN 'COALESCE(s.rebate_bps::numeric,0)' ELSE '0::numeric' END
        )
      ELSE 'NULL::numeric'
    END,
    CASE
      WHEN has_total_cost_bps THEN 's.total_cost_bps::numeric'
      WHEN has_cost_bps THEN 's.cost_bps::numeric'
      WHEN has_fee_bps OR has_slippage_bps OR has_rebate_bps THEN
        format(
          '%s + %s - %s',
          CASE WHEN has_fee_bps THEN 'COALESCE(s.fee_bps::numeric,0)' ELSE '0::numeric' END,
          CASE WHEN has_slippage_bps THEN 'COALESCE(s.slippage_bps::numeric,0)' ELSE '0::numeric' END,
          CASE WHEN has_rebate_bps THEN 'COALESCE(s.rebate_bps::numeric,0)' ELSE '0::numeric' END
        )
      ELSE 'NULL::numeric'
    END
  );

  EXECUTE sql;

  GET DIAGNOSTICS rows_inserted = ROW_COUNT;
  RETURN COALESCE(rows_inserted, 0);
END;
$$;

CREATE OR REPLACE FUNCTION execution.fn_refresh_venue_cost_model_v1()
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  rows_refreshed integer := 0;
  has_expected_total_cost_bps boolean;
BEGIN
  IF to_regclass('execution.venue_cost_model_v1') IS NULL
     OR to_regclass('execution.edge_cost_observation_v1') IS NULL THEN
    RETURN 0;
  END IF;

  SELECT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'execution'
      AND table_name   = 'venue_cost_model_v1'
      AND column_name  = 'expected_total_cost_bps'
  ) INTO has_expected_total_cost_bps;

  DELETE FROM execution.venue_cost_model_v1;

  IF has_expected_total_cost_bps THEN
    INSERT INTO execution.venue_cost_model_v1 (
      venue,
      instrument_symbol,
      sample_size,
      avg_cost_bps,
      min_cost_bps,
      max_cost_bps,
      p50_cost_bps,
      p90_cost_bps,
      expected_total_cost_bps,
      as_of
    )
    SELECT
      venue,
      instrument_symbol,
      COUNT(*)::bigint,
      AVG(total_cost_bps)::numeric,
      MIN(total_cost_bps)::numeric,
      MAX(total_cost_bps)::numeric,
      percentile_cont(0.5) WITHIN GROUP (ORDER BY total_cost_bps)::numeric,
      percentile_cont(0.9) WITHIN GROUP (ORDER BY total_cost_bps)::numeric,
      AVG(total_cost_bps)::numeric,
      now()
    FROM execution.edge_cost_observation_v1
    WHERE total_cost_bps IS NOT NULL
    GROUP BY venue, instrument_symbol;
  ELSE
    INSERT INTO execution.venue_cost_model_v1 (
      venue,
      instrument_symbol,
      sample_size,
      avg_cost_bps,
      min_cost_bps,
      max_cost_bps,
      p50_cost_bps,
      p90_cost_bps,
      as_of
    )
    SELECT
      venue,
      instrument_symbol,
      COUNT(*)::bigint,
      AVG(total_cost_bps)::numeric,
      MIN(total_cost_bps)::numeric,
      MAX(total_cost_bps)::numeric,
      percentile_cont(0.5) WITHIN GROUP (ORDER BY total_cost_bps)::numeric,
      percentile_cont(0.9) WITHIN GROUP (ORDER BY total_cost_bps)::numeric,
      now()
    FROM execution.edge_cost_observation_v1
    WHERE total_cost_bps IS NOT NULL
    GROUP BY venue, instrument_symbol;
  END IF;

  GET DIAGNOSTICS rows_refreshed = ROW_COUNT;
  RETURN COALESCE(rows_refreshed, 0);
END;
$$;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema='execution'
      AND table_name='edge_cost_observation_v1'
      AND column_name='fill_id'
  )
  AND EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema='execution'
      AND table_name='v_execution_fill_clean_v1'
      AND column_name='fill_id'
  )
  AND EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema='execution'
      AND table_name='v_execution_fill_clean_v1'
      AND column_name='total_cost_bps'
  ) THEN
    EXECUTE $q$
      UPDATE execution.edge_cost_observation_v1 o
         SET total_cost_bps = c.total_cost_bps::numeric
        FROM execution.v_execution_fill_clean_v1 c
       WHERE o.fill_id = c.fill_id
         AND c.total_cost_bps IS NOT NULL
         AND (
              o.total_cost_bps IS DISTINCT FROM c.total_cost_bps::numeric
           OR o.total_cost_bps IS NULL
         )
    $q$;
  END IF;
END
$$;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema='execution'
      AND table_name='edge_cost_observation_v1'
      AND column_name='fill_id'
  )
  AND EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema='execution'
      AND table_name='v_execution_fill_clean_v1'
      AND column_name='fill_id'
  )
  AND EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema='execution'
      AND table_name='v_execution_fill_clean_v1'
      AND column_name='total_cost_bps'
  ) THEN
    EXECUTE $q$
      DELETE FROM execution.edge_cost_observation_v1 o
      WHERE (
             o.total_cost_bps = 0
             OR o.total_cost_bps IS NULL
            )
        AND EXISTS (
          SELECT 1
          FROM execution.v_execution_fill_clean_v1 c
          WHERE c.fill_id = o.fill_id
            AND c.total_cost_bps IS NOT NULL
            AND c.total_cost_bps <> 0
        )
    $q$;
  END IF;
END
$$;

SELECT execution.fn_upsert_edge_cost_observation_v1();
SELECT execution.fn_refresh_venue_cost_model_v1();

DO $$
DECLARE
  r record;
BEGIN
  FOR r IN
    SELECT schemaname, viewname, definition
    FROM _exec_saved_views
    ORDER BY depth DESC, schemaname, viewname
  LOOP
    EXECUTE format('CREATE OR REPLACE VIEW %I.%I AS %s', r.schemaname, r.viewname, r.definition);
  END LOOP;
END
$$;

COMMIT;

SELECT
  venue,
  ROUND(AVG(total_cost_bps)::numeric, 6) AS avg_total_cost_bps,
  COUNT(*) AS rows_count
FROM execution.edge_cost_observation_v1
WHERE total_cost_bps IS NOT NULL
GROUP BY venue
ORDER BY AVG(total_cost_bps);

SELECT execution.fn_refresh_venue_cost_model_v1();

SELECT
  venue,
  ROUND(AVG(COALESCE(expected_total_cost_bps, avg_cost_bps))::numeric, 6) AS avg_expected_cost,
  COUNT(*) AS n
FROM execution.venue_cost_model_v1
GROUP BY venue
ORDER BY AVG(COALESCE(expected_total_cost_bps, avg_cost_bps));

SELECT *
FROM execution.v_top_venues_by_cost_v1
ORDER BY COALESCE(expected_total_cost_bps, avg_cost_bps), venue;

/*
Root issue removed here:
- total_cost_bps default 0 is dropped
- total_cost_bps NOT NULL is dropped
- model no longer averages COALESCE(total_cost_bps,0)
That was the corruption vector in the uploaded SQL. :contentReference[oaicite:0]{index=0}
*/