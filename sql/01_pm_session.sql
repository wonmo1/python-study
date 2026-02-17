-- sql/01_pm_session.sql

WITH params AS (
  SELECT
    now() - interval 10 days AS start_ts,
    now() - interval 1 days  AS end_ts
),

pm_h AS (
  SELECT
    eqp_id,
    eqp_status,
    eqp_status_change_time AS t,

    LEAD(eqp_status, 1) OVER (
      PARTITION BY eqp_id
      ORDER BY eqp_status_change_time
    ) AS s1,
    LEAD(eqp_status_change_time, 1) OVER (
      PARTITION BY eqp_id
      ORDER BY eqp_status_change_time
    ) AS t1,

    LEAD(eqp_status, 2) OVER (
      PARTITION BY eqp_id
      ORDER BY eqp_status_change_time
    ) AS s2,
    LEAD(eqp_status_change_time, 2) OVER (
      PARTITION BY eqp_id
      ORDER BY eqp_status_change_time
    ) AS t2

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

    unix_timestamp(t2, 'yyyyMMdd HHmmss') - unix_timestamp(t, 'yyyyMMdd HHmmss') AS dur_sec,
    s2 AS pm_end_status
  FROM pm_h
  WHERE eqp_status = 'PM'
    AND s1 = 'LOCAL'
    AND s2 IN ('IDLE', 'RUN')
    AND t2 IS NOT NULL
),

pm_filtered AS (
  SELECT *
  FROM pm
  WHERE dur_sec BETWEEN 4*3600 AND 10*3600
)

SELECT
  station,
  pm_start_ts,
  pm_end_ts,
  dur_sec,
  pm_end_status
FROM pm_filtered
ORDER BY pm_start_ts DESC;