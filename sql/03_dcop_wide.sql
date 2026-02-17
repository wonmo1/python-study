-- sql/03_dcop_wide.sql

WITH params AS (
  SELECT
    now() - interval 10 days AS start_ts,
    now() - interval 0 days  AS end_ts
),

-- VANTAGE station만 남기면 성능이 크게 좋아집니다(1년치 대비)
master_vtg AS (
  SELECT DISTINCT
    upper(trim(m.STATION)) AS station
  FROM fab.m_tpss_station_master m
  WHERE SUBSTR(m.room, -1) = '5'
    AND m.STATION_NAME IN ('VANTAGE-PLUS','VANTAGE_PLUS','VANTAGE')
    AND m.STATION LIKE '%-%'
)

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
JOIN master_vtg v
  ON concat(upper(trim(d.prc_eqp_id)), '-', upper(trim(d.chamber_ids))) = v.station
WHERE d.container_id IS NOT NULL
  AND d.npw_wafer_id IS NOT NULL
  AND d.prc_slot_id IS NOT NULL
  AND d.prc_eqp_id IS NOT NULL
  AND d.chamber_ids IS NOT NULL
  AND upper(trim(d.item_id)) RLIKE '^[A-Z]_T[1-7]_OFFSET$'
  -- ⚠️ DCOP 시간컬럼이 무엇인지에 따라 아래 조건은 수정 필요:
  -- AND d.<dcop_time_column> BETWEEN (SELECT start_ts FROM params) AND (SELECT end_ts FROM params)
GROUP BY
  concat(upper(trim(d.prc_eqp_id)), '-', upper(trim(d.chamber_ids))),
  d.container_id,
  d.npw_wafer_id,
  d.prc_slot_id
LIMIT 2000;
