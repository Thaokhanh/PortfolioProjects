/*
 ============================================================================
 1. DATA CLEANING & VALIDATION ON STAGING TABLES
    – Check for NULL / missing
    – Identify duplicates
    – Clean / convert types
 ============================================================================
*/

/* 1.1 Count missing usr_id and bad timestamps in launch_resume */
    /*stg_launch_resume*/
SELECT
  SUM(CASE WHEN usr_id IS NULL THEN 1 ELSE 0 END)  AS missing_usr,
  SUM(CASE WHEN TRY_CAST(client_time AS datetime2) IS NULL THEN 1 ELSE 0 END) AS bad_timestamps
FROM dbo.launch_resume;
GO

    /*stg_installs*/
SELECT
  SUM(CASE WHEN usr_id IS    NULL THEN 1 ELSE 0 END) AS missing_usr,
  SUM(CASE WHEN TRY_CAST(install_time AS datetime2) IS NULL THEN 1 ELSE 0 END) AS bad_install_time
FROM dbo.installs;
GO

    /*stg_ftue*/
SELECT
  SUM(CASE WHEN usr_id       IS NULL THEN 1 ELSE 0 END) AS missing_usr,
  SUM(CASE WHEN TRY_CAST(client_time AS datetime2) IS NULL THEN 1 ELSE 0 END) AS bad_timestamps,
  SUM(CASE WHEN action_order IS NULL THEN 1 ELSE 0 END) AS missing_action_order
FROM dbo.ftue;
GO

    /*stg_toy_unlock*/
SELECT
  SUM(CASE WHEN usr_id         IS NULL THEN 1 ELSE 0 END) AS missing_usr,
  SUM(CASE WHEN TRY_CAST(client_time AS datetime2) IS NULL THEN 1 ELSE 0 END) AS bad_timestamps,
  SUM(CASE WHEN toy_amount    < 0    THEN 1 ELSE 0 END)   AS invalid_toy_amount
FROM dbo.toy_unlock
GO
/*======================*/

/* 1.2 Find duplicates in staging tables 
    – Identify any repeated keys or events that may need deduplication */

/* stg_launch_resume: duplicate sessions per user */
SELECT
  usr_id,
  session_id,
  COUNT(*) AS occurrences
FROM dbo.launch_resume
GROUP BY usr_id, session_id
HAVING COUNT(*) > 1;
GO

/* stg_installs: duplicate install records per user */
SELECT
  usr_id,
  COUNT(*) AS occurrences
FROM dbo.installs
GROUP BY usr_id
HAVING COUNT(*) > 1;
GO

/* stg_ftue: duplicate FTUE actions per user, step, and order */
SELECT
  usr_id,
  ftue_steps,
  action_order,
  COUNT(*) AS occurrences
FROM dbo.ftue
GROUP BY usr_id, ftue_steps, action_order
HAVING COUNT(*) > 1;
GO

/* stg_toy_unlock: duplicate toy unlock events per user/session/toy */
SELECT
  usr_id,
  session_id,
  toy_name,
  COUNT(*) AS occurrences
FROM dbo.toy_unlock
GROUP BY usr_id, session_id, toy_name
HAVING COUNT(*) > 1;
GO

/* --------- DEDUPE stg_launch_resume --------- */
WITH cte_launch AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY usr_id, session_id
      ORDER BY TRY_CAST(client_time AS datetime2)  -- giữ bản ghi sớm nhất
    ) AS rn
  FROM dbo.launch_resume
)
DELETE lr
FROM dbo.launch_resume lr
JOIN cte_launch c
  ON lr.usr_id      = c.usr_id
 AND lr.session_id  = c.session_id
 AND TRY_CAST(lr.client_time AS datetime2) = TRY_CAST(c.client_time AS datetime2)
WHERE c.rn > 1;
GO

/* --------- DEDUPE stg_ftue --------- */
/* 
   Partition theo usr_id + ftue_steps + action_order + ftue_stage 
   (start vs finish) và giữ bản ghi có event_time sớm nhất
*/
WITH cte_ftue AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY usr_id, ftue_steps, action_order, ftue_stage
      ORDER BY TRY_CAST(client_time AS datetime2)
    ) AS rn
  FROM dbo.ftue
)
DELETE f
FROM dbo.ftue f
JOIN cte_ftue c
  ON f.usr_id       = c.usr_id
 AND f.ftue_steps   = c.ftue_steps
 AND f.action_order = c.action_order
 AND f.ftue_stage   = c.ftue_stage
 AND TRY_CAST(f.client_time AS datetime2) = TRY_CAST(c.client_time AS datetime2)
WHERE c.rn > 1;
GO

/* --------- DEDUPE stg_toy_unlock --------- */
/* 
   Partition theo usr_id + session_id + toy_name 
   và giữ bản ghi có event_time sớm nhất
*/
WITH cte_toy AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY usr_id, session_id, toy_name
      ORDER BY TRY_CAST(client_time AS datetime2)
    ) AS rn
  FROM dbo.toy_unlock
)
DELETE t
FROM dbo.toy_unlock t
JOIN cte_toy c
  ON t.usr_id      = c.usr_id
 AND t.session_id  = c.session_id
 AND t.toy_name    = c.toy_name
 AND TRY_CAST(t.client_time AS datetime2) = TRY_CAST(c.client_time AS datetime2)
WHERE c.rn > 1;
GO

/* --------- VERIFY AGAIN --------- */
-- Chạy lại queries tìm duplicate để chắc là 0 row trả về
SELECT usr_id, session_id, COUNT(*) AS cnt
FROM dbo.launch_resume
GROUP BY usr_id, session_id
HAVING COUNT(*) > 1;

SELECT usr_id, ftue_steps, action_order, ftue_stage, COUNT(*) AS cnt
FROM dbo.ftue
GROUP BY usr_id, ftue_steps, action_order, ftue_stage
HAVING COUNT(*) > 1;

SELECT usr_id, session_id, toy_name, COUNT(*) AS cnt
FROM dbo.toy_unlock
GROUP BY usr_id, session_id, toy_name
HAVING COUNT(*) > 1;
GO
/*======================*/

/* 1.3 Clean out invalid row in staging tables 
     – Remove records with NULL keys, bad timestamps, negative/time_spent anomalies*/

/* stg_launch_resume */
DELETE FROM dbo.launch_resume
WHERE
  usr_id IS NULL
  OR TRY_CAST(client_time AS datetime2) IS NULL
  OR time_spent < 0;
GO

/* stg_installs */
DELETE FROM dbo.installs
WHERE
  usr_id IS NULL
  OR TRY_CAST(install_time AS datetime2) IS NULL;
GO

/* stg_ftue */
DELETE FROM dbo.ftue
WHERE
  usr_id IS NULL
  OR TRY_CAST(client_time AS datetime2) IS NULL
  OR action_order IS NULL
  OR time_spent < 0;
GO

/* stg_toy_unlock */
DELETE FROM dbo.toy_unlock
WHERE
  usr_id IS NULL
  OR TRY_CAST(client_time AS datetime2) IS NULL
  OR toy_amount < 0;
GO
/*======================*/

/* 1.4 Convert timestamp columns to datetime2 in all staging tables
     – Ensure client_time / install_time columns are proper DATETIME2 */
/* stg_launch_resume */
ALTER TABLE dbo.launch_resume
  ALTER COLUMN client_time datetime2 NOT NULL;
GO

ALTER TABLE dbo.launch_resume
  ALTER COLUMN event_client_time_local datetime2 NULL;
GO

/* stg_installs */
ALTER TABLE dbo.installs
  ALTER COLUMN install_time datetime2 NOT NULL;
GO

/* stg_ftue */
ALTER TABLE dbo.ftue
  ALTER COLUMN client_time datetime2 NOT NULL;
GO

/* stg_toy_unlock */
ALTER TABLE dbo.toy_unlock
  ALTER COLUMN client_time datetime2 NOT NULL;
GO



/*
 ============================================================================
 2. DESIGN DIMENSION & FACT SCHEMA
    – dim_users, dim_ftue_steps
    – fact_sessions, fact_ftue, fact_toy_unlock
 ============================================================================
*/

/* 2.1 Dimension: Users */
CREATE TABLE dbo.dim_users (
  usr_id       varchar(50)    PRIMARY KEY,
  platform     varchar(20)    NOT NULL,
  install_date date           NOT NULL
);
GO

/* 2.2 Dimension: FTUE Steps */
CREATE TABLE dbo.dim_ftue_steps (
  ftue_step_id   int IDENTITY(1,1) PRIMARY KEY,
  ftue_steps     varchar(100)      NOT NULL,
  action_order   tinyint           NOT NULL
);
GO

/* 2.3 Fact: Sessions */
CREATE TABLE dbo.fact_sessions (
  usr_id                 varchar(50) NOT NULL
    FOREIGN KEY REFERENCES dbo.dim_users(usr_id),
  session_id             int         NOT NULL,
  event_time             datetime2   NOT NULL,
  time_spent             int         NOT NULL,
  time_between_sessions  int         NULL
);
GO

/* 2.4 Fact: FTUE Events */
CREATE TABLE dbo.fact_ftue (
  usr_id       varchar(50)  NOT NULL
    FOREIGN KEY REFERENCES dbo.dim_users(usr_id),
  ftue_steps   varchar(100) NOT NULL,
  ftue_stage   varchar(50)  NOT NULL,
  action_order tinyint      NOT NULL,
  event_time   datetime2    NOT NULL,
  time_spent   int          NOT NULL
);
GO

/* 2.5 Fact: Toy Unlocks */
CREATE TABLE dbo.fact_toy_unlock (
  usr_id                varchar(50) NOT NULL
    FOREIGN KEY REFERENCES dbo.dim_users(usr_id),
  session_id            int         NOT NULL,
  event_time            datetime2   NOT NULL,
  toy_amount            smallint    NOT NULL,
  toy_name              varchar(100)NOT NULL,
  unlock_cause          varchar(100)NULL,
  is_new_toy            bit         NOT NULL,
  toy_unlocked_method   varchar(50) NULL
);
GO



/*
 ============================================================================
 3. ETL: LOAD DATA FROM STAGING INTO DIM / FACT
 ============================================================================
*/

/* 3.1 Load dim_users (incremental) */
INSERT INTO dbo.dim_users (usr_id, platform, install_date)
SELECT DISTINCT
  usr_id,
  platform,
  CAST(install_time AS date)
FROM dbo.installs s
WHERE NOT EXISTS (
  SELECT 1 FROM dbo.dim_users d
   WHERE d.usr_id = s.usr_id
);
GO

/* 3.2 Load dim_ftue_steps */
INSERT INTO dbo.dim_ftue_steps (ftue_steps, action_order)
SELECT DISTINCT
  ftue_steps,
  action_order
FROM dbo.ftue f
WHERE NOT EXISTS (
  SELECT 1
  FROM dbo.dim_ftue_steps d
  WHERE d.ftue_steps = f.ftue_steps
    AND d.action_order = f.action_order
);
GO

/* 3.3 Load fact_sessions */
INSERT INTO dbo.fact_sessions (usr_id, session_id, event_time, time_spent, time_between_sessions)
SELECT
  usr_id,
  session_id,
  CAST(client_time AS datetime2),
  time_spent,
  time_between_sessions
FROM dbo.launch_resume;
GO

/* 3.4 Load fact_ftue */
INSERT INTO dbo.fact_ftue (usr_id, ftue_steps, ftue_stage, action_order, event_time, time_spent)
SELECT
  usr_id,
  ftue_steps,
  ftue_stage,
  action_order,
  CAST(client_time AS datetime2),
  time_spent
FROM dbo.ftue;
GO

/* 3.5 Load fact_toy_unlock */
INSERT INTO dbo.fact_toy_unlock
  (usr_id, session_id, event_time, toy_amount, toy_name, unlock_cause, is_new_toy, toy_unlocked_method)
SELECT
  usr_id,
  session_id,
  CAST(client_time AS datetime2),
  toy_amount,
  toy_name,
  unlock_cause,
  isnewtoy,
  toy_unlocked_method
FROM dbo.toy_unlock;
GO



/*
 ============================================================================
 4. CREATE VIEWS FOR CORE METRICS
    – sessions/week-1, FTUE progression, retention cohorts, funnel conversion…
 ============================================================================
*/

/* 4.1 Sessions per day & total play time in Week 1 */
CREATE VIEW dbo.vw_sessions_week1 AS
SELECT
  u.usr_id,
  DATEDIFF(day, u.install_date, s.event_time) AS day_since_install,
  COUNT(DISTINCT s.session_id)            AS session_count,
  SUM(s.time_spent)                       AS total_play_time
FROM dbo.dim_users u
JOIN dbo.fact_sessions s
  ON u.usr_id = s.usr_id
WHERE DATEDIFF(day, u.install_date, s.event_time) BETWEEN 0 AND 6
GROUP BY
  u.usr_id,
  DATEDIFF(day, u.install_date, s.event_time);
GO

/* 4.2 FTUE progression times (start → finish per step) */
CREATE VIEW dbo.vw_ftue_progress AS
SELECT
  usr_id,
  ftue_steps,
  MIN(CASE WHEN ftue_stage = 'start'  THEN event_time END) AS start_time,
  MAX(CASE WHEN ftue_stage = 'finish' THEN event_time END) AS finish_time
FROM dbo.fact_ftue
GROUP BY usr_id, ftue_steps;
GO

/* 4.3 Retention cohort view (Day-1, Day-7, Day-14)
    – For each install_date, calculate how many users return on days 1, 7, and 14 */
CREATE VIEW dbo.vw_retention_cohort AS
WITH cohorts AS (
    -- Base cohort: users by install_date
    SELECT
        install_date,
        usr_id
    FROM dbo.dim_users
),
returns AS (
    -- Return events tagged with day offset
    SELECT
        u.install_date,
        DATEDIFF(day, u.install_date, s.event_time) AS day_since_install,
        s.usr_id
    FROM cohorts u
    JOIN dbo.fact_sessions s
      ON u.usr_id = s.usr_id
    WHERE DATEDIFF(day, u.install_date, s.event_time) IN (1, 7, 14)
)
SELECT
    c.install_date,
    r.day_since_install,
    COUNT(DISTINCT r.usr_id) AS retained_users,
    COUNT(DISTINCT c.usr_id) AS total_users,
    ROUND(
      100.0 * COUNT(DISTINCT r.usr_id)
            / NULLIF(COUNT(DISTINCT c.usr_id), 0),
      2
    ) AS retention_pct
FROM cohorts c
LEFT JOIN returns r
  ON c.install_date   = r.install_date
  AND c.usr_id        = r.usr_id
GROUP BY
    c.install_date,
    r.day_since_install
HAVING r.day_since_install IN (1, 7, 14);
GO


/* 4.4 Funnel conversion view
    – Count distinct users at each key stage: Install → Launch → FTUE start → FTUE finish → Toy Unlock*/

IF OBJECT_ID('dbo.vw_funnel_conversion', 'V') IS NOT NULL
  DROP VIEW dbo.vw_funnel_conversion;
GO

-- Tạo lại view funnel conversion
CREATE VIEW dbo.vw_funnel_conversion AS
WITH install_cte AS (
    -- Tổng số user cài đặt
    SELECT COUNT(DISTINCT usr_id) AS cnt_install
    FROM dbo.dim_users
),
stage_cte AS (
    -- Đếm distinct users ở mỗi giai đoạn
    SELECT 'Install'      AS stage, COUNT(DISTINCT usr_id) AS users FROM dbo.dim_users
    UNION ALL
    SELECT 'Launch',      COUNT(DISTINCT usr_id) FROM dbo.fact_sessions
    UNION ALL
    SELECT 'FTUE Start',  COUNT(DISTINCT usr_id) FROM dbo.fact_ftue WHERE ftue_stage = 'start'
    UNION ALL
    SELECT 'FTUE Finish', COUNT(DISTINCT usr_id) FROM dbo.fact_ftue WHERE ftue_stage = 'finish'
    UNION ALL
    SELECT 'Toy Unlock',  COUNT(DISTINCT usr_id) FROM dbo.fact_toy_unlock
)
SELECT
    s.stage,
    s.users,
    -- Tỷ lệ conversion trên tổng số install
    ROUND(100.0 * s.users / i.cnt_install, 2) AS conversion_pct
FROM stage_cte s
CROSS JOIN install_cte i;
GO


/*
 ============================================================================
 ADDITIONAL VIEWS FOR ENRICHED METRICS & VISUALIZATIONS
 ============================================================================
*/

/* 1. Daily sessions & play time per user, days 0–14 since install */
CREATE VIEW dbo.vw_daily_sessions AS
SELECT
  u.usr_id,
  DATEDIFF(day, u.install_date, s.event_time) AS day_since_install,
  COUNT(DISTINCT s.session_id)                 AS session_count,
  SUM(s.time_spent)                            AS total_play_time
FROM dbo.dim_users u
JOIN dbo.fact_sessions s
  ON u.usr_id = s.usr_id
WHERE DATEDIFF(day, u.install_date, s.event_time) BETWEEN 0 AND 14
GROUP BY
  u.usr_id,
  DATEDIFF(day, u.install_date, s.event_time);
GO

/* 2. Average session length per user per day (sec) */
CREATE VIEW dbo.vw_avg_session_length AS
SELECT
  usr_id,
  day_since_install,
  CASE
    WHEN session_count > 0 THEN CAST(total_play_time AS float) / session_count
    ELSE 0
  END AS avg_session_length
FROM dbo.vw_daily_sessions;
GO

/* 3. Toy unlock summary per user */
CREATE VIEW dbo.vw_toy_unlock_summary AS
SELECT
  u.usr_id,
  COUNT(t.toy_name)                          AS total_toys_unlocked,
  MIN(DATEDIFF(second, CAST(u.install_date AS datetime2), t.event_time)) AS time_to_first_unlock,
  AVG(CAST(t.toy_amount AS float))           AS avg_toy_amount
FROM dbo.dim_users u
LEFT JOIN dbo.fact_toy_unlock t
  ON u.usr_id = t.usr_id
GROUP BY
  u.usr_id;
GO

/* 4. Retention rates by FTUE completion flag */
-- ftue_complete = 1 if user finished all steps
CREATE VIEW dbo.vw_retention_by_ftue AS
WITH completed AS (
  SELECT
    usr_id,
    CASE WHEN COUNT(DISTINCT ftue_steps) = (SELECT COUNT(*) FROM dbo.dim_ftue_steps) THEN 1 ELSE 0 END AS ftue_complete
  FROM dbo.fact_ftue
  WHERE ftue_stage = 'finish'
  GROUP BY usr_id
), cohorts AS (
  SELECT
    u.usr_id,
    u.install_date,
    DATEDIFF(day, u.install_date, s.event_time) AS day_since_install
  FROM dbo.dim_users u
  JOIN dbo.fact_sessions s
    ON u.usr_id = s.usr_id
)
SELECT
  c.day_since_install,
  f.ftue_complete,
  COUNT(DISTINCT c.usr_id)                            AS users_active,
  COUNT(DISTINCT CASE WHEN c.day_since_install = 1 THEN c.usr_id END) AS day1_retained,
  COUNT(DISTINCT CASE WHEN c.day_since_install = 7 THEN c.usr_id END) AS day7_retained,
  COUNT(DISTINCT CASE WHEN c.day_since_install = 14 THEN c.usr_id END) AS day14_retained
FROM cohorts c
JOIN completed f
  ON c.usr_id = f.usr_id
WHERE c.day_since_install IN (1,7,14)
GROUP BY
  c.day_since_install,
  f.ftue_complete;
GO

/* 5. Platform comparison for key metrics */
CREATE VIEW dbo.vw_platform_comparison AS
SELECT
  u.platform,
  COUNT(DISTINCT u.usr_id)                                  AS total_users,
  SUM(s.session_count)                                      AS total_sessions,
  SUM(s.total_play_time)                                    AS total_play_time,
  AVG(d.avg_session_length)                                 AS avg_session_length,
  AVG(tu.total_toys_unlocked)                               AS avg_toys_per_user
FROM dbo.dim_users u
LEFT JOIN dbo.vw_daily_sessions s
  ON u.usr_id = s.usr_id
LEFT JOIN dbo.vw_avg_session_length d
  ON u.usr_id = d.usr_id AND s.day_since_install = d.day_since_install
LEFT JOIN dbo.vw_toy_unlock_summary tu
  ON u.usr_id = tu.usr_id
GROUP BY
  u.platform;
GO

/* 6. Hourly sessions heatmap data */
CREATE VIEW dbo.vw_hourly_sessions_heatmap AS
SELECT
  DATEPART(weekday, s.event_time) AS weekday,   -- 1=Sunday..7=Saturday
  DATEPART(hour, s.event_time)    AS hour_of_day,
  COUNT(DISTINCT s.session_id)     AS session_count
FROM dbo.fact_sessions s
GROUP BY
  DATEPART(weekday, s.event_time),
  DATEPART(hour, s.event_time);
GO

/* 7. User features for downstream modelling */
CREATE OR ALTER VIEW dbo.vw_user_features AS
SELECT
  u.usr_id,

  /* sessions & play time on day 1 */
  SUM(CASE WHEN DATEDIFF(day, u.install_date, s.event_time) = 1 
           THEN 1 ELSE 0 END) AS sessions_day1,
  SUM(CASE WHEN DATEDIFF(day, u.install_date, s.event_time) = 1 
           THEN s.time_spent ELSE 0 END) AS time_day1,

  /* FTUE complete flag (already grouped by c.ftue_complete) */
  COALESCE(c.ftue_complete, 0) AS ftue_completed_flag,

  /* toy_unlocked_flag: has the user ever unlocked any toy? */
  CASE 
    WHEN MAX(COALESCE(tu.total_toys_unlocked,0)) > 0 
    THEN 1 
    ELSE 0 
  END AS toy_unlocked_flag

FROM dbo.dim_users u

LEFT JOIN dbo.fact_sessions s
  ON u.usr_id = s.usr_id

LEFT JOIN (
  /* users who finished all FTUE steps */
  SELECT 
    usr_id,
    CASE 
      WHEN COUNT(DISTINCT ftue_steps) = (SELECT COUNT(*) FROM dbo.dim_ftue_steps)
      THEN 1 
      ELSE 0 
    END AS ftue_complete
  FROM dbo.fact_ftue
  WHERE ftue_stage = 'finish'
  GROUP BY usr_id
) c
  ON u.usr_id = c.usr_id

LEFT JOIN dbo.vw_toy_unlock_summary tu
  ON u.usr_id = tu.usr_id

GROUP BY
  u.usr_id,
  COALESCE(c.ftue_complete, 0);
GO



/*
 ============================================================================
 5. NEXT STEPS
    – Connect Power BI to these views
    – Build DAX Measures & Python visuals
    – Generate dashboards, export PDF report
 ============================================================================
*/