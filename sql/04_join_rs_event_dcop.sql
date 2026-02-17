-- sql/04_join_rs_event_dcop.sql

WITH params AS (
  SELECT
    now() - interval 10 days AS start_ts,
    now() - interval 0 days  AS end_ts
),

-- ===== rs_event (02와 동일) =====
pm_h AS (
  SELECT
    eqp_id,
    eqp_status,
    eqp_status_change_time AS t,
    LEAD(eqp_status, 1) OVER (PARTITION BY eqp_id ORDER BY eqp_status_change_time) AS s1,
    LEAD(eqp_status_change_time, 1) OVER (PARTITION BY eqp_id ORDER BY eqp_status_change_time) AS t1,
    LEAD(eqp_status, 2) OVER (PARTITION BY eqp_id ORDER BY eqp_status_change_time) AS s2,
    LEAD(eqp_status_change_time, 2) OVER (PARTITION BY eqp_id ORDER BY eqp_status_change_time) AS t2
  FROM mos_kh_smi.smimes_mi_eqp_hist
  WHERE eqp_id IN (
    SELECT m.STATION
    FROM fab.m_tpss_station_master m
    WHERE SUBSTR(m.room, -1) = '5'
      AND m.STATION_NAME IN ('VANTAGE-PLUS','VANTAGE_PLUS','VANTAGE')
      AND m.STATION LIKE '%-%'
  )
  AND eqp_status_change_time BETWEEN
        concat(from_unixtime(unix_timestamp((SELECT start_ts FROM params)), 'yyyyMMdd'), ' 000000')
    AND concat(from_unixtime(unix_timestamp((SELECT end_ts   FROM params)), 'yyyyMMdd'), ' 235959')
),

pm AS (
  SELECT
    eqp_id AS station,
    CAST(from_unixtime(unix_timestamp(t,  'yyyyMMdd HHmmss')) AS TIMESTAMP)  AS pm_start_ts,
    CAST(from_unixtime(unix_timestamp(t2, 'yyyyMMdd HHmmss')) AS TIMESTAMP) AS pm_end_ts,
    unix_timestamp(t2, 'yyyyMMdd HHmmss') - unix_timestamp(t, 'yyyyMMdd HHmmss') AS dur_sec
  FROM pm_h
  WHERE eqp_status = 'PM'
    AND s1 = 'LOCAL'
    AND s2 IN ('IDLE','RUN')
    AND t2 IS NOT NULL
),

pm_filtered AS (
  SELECT *
  FROM pm
  WHERE dur_sec BETWEEN 4*3600 AND 10*3600
),

master_vtg AS (
  SELECT DISTINCT
    upper(trim(m.STATION)) AS station,
    m.area AS area
  FROM fab.m_tpss_station_master m
  WHERE SUBSTR(m.room, -1) = '5'
    AND m.STATION_NAME IN ('VANTAGE-PLUS','VANTAGE_PLUS','VANTAGE')
    AND m.STATION LIKE '%-%'
),

rs_evt AS (
  SELECT
    mv.area,
    concat(upper(trim(met.prc_eqp_id)), '-', upper(trim(met.chamber_ids))) AS station,
    met.prc_tkin_time AS rs_time,
    met.prc_ppid,
    met.container_id,
    met.npw_wafer_id,
    met.prc_slot_id,
    COUNT(DISTINCT met.subitem_id) AS n_points
  FROM fab.m_fab_npw_met met
  JOIN master_vtg mv
    ON concat(upper(trim(met.prc_eqp_id)), '-', upper(trim(met.chamber_ids))) = mv.station
  WHERE met.item_id = 'RS1'
    AND met.subitem_id RLIKE '^S[0-9]+$'
    AND met.prc_tkin_time BETWEEN (SELECT start_ts FROM params) AND (SELECT end_ts FROM params)
  GROUP BY
    mv.area,
    concat(upper(trim(met.prc_eqp_id)), '-', upper(trim(met.chamber_ids))),
    met.prc_tkin_time,
    met.prc_ppid,
    met.container_id,
    met.npw_wafer_id,
    met.prc_slot_id
  HAVING COUNT(DISTINCT met.subitem_id) IN (49,81,151)
),

rs_in_pm AS (
  SELECT
    p.station,
    p.pm_start_ts,
    p.pm_end_ts,
    r.area,
    r.rs_time,
    r.prc_ppid,
    r.n_points,
    r.container_id,
    r.npw_wafer_id,
    r.prc_slot_id
  FROM pm_filtered p
  JOIN rs_evt r
    ON r.station = p.station
   AND r.rs_time BETWEEN p.pm_start_ts AND p.pm_end_ts
),

rs_event AS (
  SELECT
    area,
    station,
    pm_start_ts,
    pm_end_ts,
    rs_time,
    ROW_NUMBER() OVER (
      PARTITION BY station, pm_start_ts, pm_end_ts
      ORDER BY rs_time, container_id, npw_wafer_id, prc_slot_id
    ) AS test_seq,
    container_id,
    npw_wafer_id,
    prc_slot_id,
    prc_ppid,
    n_points
  FROM rs_in_pm
),

-- ===== dcop_wide (03과 동일) =====
dcop_wide AS (
  SELECT
    concat(upper(trim(d.prc_eqp_id)), '-', upper(trim(d.chamber_ids))) AS station,
    d.container_id,
    d.npw_wafer_id,
    d.prc_slot_id,

    MAX(CASE WHEN upper(trim(d.item_id)) = 'A_T1_OFFSET' THEN d.value END) AS t1,
    MAX(CASE WHEN upper(trim(d.item_id)) = 'A_T2_OFFSET' THEN d.value END) AS t2,
    MAX(CASE WHEN upper(trim(d.item_id)) = 'A_T3_OFFSET' THEN d.value END) AS t3,
    MAX(CASE WHEN upper(trim(d.item_id)) = 'A_T4_OFFSET' THEN d.value END) AS t4,
    MAX(CASE WHEN upper(trim(d.item_id)) = 'A_T5_OFFSET' THEN d.value END) AS t5,
    MAX(CASE WHEN upper(trim(d.item_id)) = 'A_T6_OFFSET' THEN d.value END) AS t6,
    MAX(CASE WHEN upper(trim(d.item_id)) = 'A_T7_OFFSET' THEN d.value END) AS t7

  FROM fab.m_fab_npw_dcop d
  WHERE d.container_id IS NOT NULL
    AND d.npw_wafer_id IS NOT NULL
    AND d.prc_slot_id IS NOT NULL
    AND d.prc_eqp_id IS NOT NULL
    AND d.chamber_ids IS NOT NULL
    AND upper(trim(d.item_id)) RLIKE '^[A-Z]_T[1-7]_OFFSET$'
  GROUP BY
    concat(upper(trim(d.prc_eqp_id)), '-', upper(trim(d.chamber_ids))),
    d.container_id,
    d.npw_wafer_id,
    d.prc_slot_id
)

-- ✅ 진단 1: station 제외 조인 (핵심)
SELECT
  e.station AS rs_station,
  w.station AS dcop_station,
  e.container_id,
  e.npw_wafer_id,
  e.prc_slot_id,
  e.rs_time,
  e.test_seq,
  w.t1, w.t2, w.t3, w.t4, w.t5, w.t6, w.t7
FROM rs_event e
LEFT JOIN dcop_wide w
  ON e.container_id = w.container_id
 AND e.npw_wafer_id = w.npw_wafer_id
 AND e.prc_slot_id  = w.prc_slot_id
WHERE w.t1 IS NOT NULL
LIMIT 200;
