/***************************************************************
    *  CROUStillantData - backfill_stats.sql
    *  Created by: CROUStillant Développement
    *  Created on: 05/09/2026
    *  Description: Seed unique, a lancer manuellement.
    *
    *  Idempotent (TRUNCATE puis re-insertion), donc rejouable sans risque
    *  tant que requests_logs n'a pas encore ete purgee.
    *
    *  ATTENTION : une fois des partitions archivees (archiver.py), ce
    *  script ne doit plus etre rejoue tel quel — il repartirait des seules
    *  lignes encore locales et effacerait l'historique deja agrege. Pour
    *  ajouter des colonnes a une base deja en service, utiliser
    *  migrate_stats.sql, qui ne touche que ce qui est vide.
***************************************************************/

BEGIN;

-- Compteurs cumulatifs
TRUNCATE stats_counters;
INSERT INTO stats_counters (
    id, total_requests, status_requests, status_200, status_404, status_500, status_503,
    breakdown_302, breakdown_400, breakdown_405, breakdown_429, requests_with_key,
    requests_without_key,
    max_ratelimit_used, sum_ratelimit_used, count_ratelimit_used, max_ratelimit_limit,
    max_ratelimit_ratio, near_limit_count
)
SELECT
    1,
    COUNT(*),
    COUNT(*) FILTER (WHERE ratelimit_limit >= 0 AND path = '/v1/status'),
    COUNT(*) FILTER (WHERE ratelimit_limit >= 0 AND status = 200),
    COUNT(*) FILTER (WHERE ratelimit_limit >= 0 AND status = 404),
    COUNT(*) FILTER (WHERE ratelimit_limit >= 0 AND status = 500),
    COUNT(*) FILTER (WHERE ratelimit_limit >= 0 AND status = 503),
    COUNT(*) FILTER (WHERE ratelimit_limit >= 0 AND status = 302),
    COUNT(*) FILTER (WHERE ratelimit_limit >= 0 AND status = 400),
    COUNT(*) FILTER (WHERE ratelimit_limit >= 0 AND status = 405),
    COUNT(*) FILTER (WHERE ratelimit_limit >= 0 AND status = 429),
    COUNT(*) FILTER (WHERE ratelimit_limit >= 0 AND key IS NOT NULL),
    COUNT(*) FILTER (WHERE ratelimit_limit >= 0 AND key IS NULL),
    COALESCE(MAX(ratelimit_used), 0),
    COALESCE(SUM(ratelimit_used) FILTER (WHERE ratelimit_used >= 0), 0),
    COUNT(*) FILTER (WHERE ratelimit_used >= 0),
    COALESCE(MAX(ratelimit_limit) FILTER (WHERE ratelimit_limit >= 0), 0),
    COALESCE(MAX(ratelimit_used::numeric / ratelimit_limit) FILTER (WHERE ratelimit_limit > 0), 0),
    COUNT(*) FILTER (WHERE ratelimit_limit > 0 AND ratelimit_used >= 0.8 * ratelimit_limit)
FROM requests_logs;


-- Répartitions par dimension
TRUNCATE stats_by_method;
INSERT INTO stats_by_method (method, total)
SELECT method, COUNT(*)
FROM requests_logs
WHERE ratelimit_limit >= 0
GROUP BY method;

TRUNCATE stats_by_api_version;
INSERT INTO stats_by_api_version (version, total)
SELECT api_version, COUNT(*)
FROM requests_logs
WHERE ratelimit_limit >= 0
GROUP BY api_version;

TRUNCATE stats_by_route;
INSERT INTO stats_by_route (route, total, sum_process_time, count_process_time)
SELECT
    normalize_route(path),
    COUNT(*),
    COALESCE(SUM(process_time) FILTER (WHERE ratelimit_limit >= 0), 0),
    COUNT(*) FILTER (WHERE ratelimit_limit >= 0)
FROM requests_logs
GROUP BY 1;

TRUNCATE stats_by_param;
INSERT INTO stats_by_param (param, total)
SELECT p.key, COUNT(*)
FROM requests_logs, jsonb_each_text(params) AS p
GROUP BY p.key;


-- Dédoublonnage (remplace COUNT(DISTINCT ...))
TRUNCATE unique_hashed_ips;
INSERT INTO unique_hashed_ips (hashed_ip)
SELECT DISTINCT hashed_ip
FROM requests_logs
WHERE hashed_ip IS NOT NULL;

TRUNCATE unique_keys;
INSERT INTO unique_keys (key)
SELECT DISTINCT key
FROM requests_logs
WHERE ratelimit_limit >= 0 AND key IS NOT NULL;


-- Répartition par heure de la journée (0-23), cumulée sur tout l'historique
TRUNCATE stats_hour_of_day_24h;
INSERT INTO stats_hour_of_day_24h (hour_of_day, total)
SELECT EXTRACT(HOUR FROM created_at)::smallint, COUNT(*)
FROM requests_logs
WHERE ratelimit_limit >= 0
GROUP BY 1;

TRUNCATE unique_ips_by_hour_of_day;
INSERT INTO unique_ips_by_hour_of_day (hour_of_day, hashed_ip)
SELECT DISTINCT EXTRACT(HOUR FROM created_at)::smallint, hashed_ip
FROM requests_logs
WHERE ratelimit_limit >= 0 AND hashed_ip IS NOT NULL;


-- Rollups horaires/quotidiens : uniquement les périodes déjà entièrement
-- closes (la période en cours sera finalisée par StatsAggregator une fois
-- terminée)
TRUNCATE stats_hourly;
INSERT INTO stats_hourly (
    hour, requests, error_count, unique_visitors,
    sum_ratelimit_used, count_ratelimit_used,
    sum_process_time, count_process_time, under_200ms_count,
    sum_ratelimit_limit, count_ratelimit_limit,
    sum_ratelimit_ratio, count_ratelimit_ratio,
    max_ratelimit_ratio, near_limit_count
)
SELECT
    DATE_TRUNC('hour', created_at),
    COUNT(*),
    COUNT(*) FILTER (WHERE status >= 400),
    COUNT(DISTINCT hashed_ip),
    COALESCE(SUM(ratelimit_used) FILTER (WHERE ratelimit_limit >= 0), 0),
    COUNT(*) FILTER (WHERE ratelimit_limit >= 0),
    COALESCE(SUM(process_time) FILTER (WHERE ratelimit_limit >= 0), 0),
    COUNT(*) FILTER (WHERE ratelimit_limit >= 0),
    COUNT(*) FILTER (WHERE process_time < 200),
    COALESCE(SUM(ratelimit_limit) FILTER (WHERE ratelimit_limit != -1), 0),
    COUNT(*) FILTER (WHERE ratelimit_limit != -1),
    COALESCE(SUM(ratelimit_used::numeric / ratelimit_limit) FILTER (WHERE ratelimit_limit > 0), 0),
    COUNT(*) FILTER (WHERE ratelimit_limit > 0),
    COALESCE(MAX(ratelimit_used::numeric / ratelimit_limit) FILTER (WHERE ratelimit_limit > 0), 0),
    COUNT(*) FILTER (WHERE ratelimit_limit > 0 AND ratelimit_used >= 0.8 * ratelimit_limit)
FROM requests_logs
WHERE created_at < DATE_TRUNC('hour', NOW())
GROUP BY 1;

TRUNCATE stats_daily;
INSERT INTO stats_daily (
    day, status_200, status_302, status_400, status_404,
    status_405, status_429, status_500, status_503,
    unique_ips, errors_4xx, errors_5xx,
    p50_process_time, p95_process_time
)
SELECT
    DATE_TRUNC('day', created_at)::date,
    COUNT(*) FILTER (WHERE status = 200),
    COUNT(*) FILTER (WHERE status = 302),
    COUNT(*) FILTER (WHERE status = 400),
    COUNT(*) FILTER (WHERE status = 404),
    COUNT(*) FILTER (WHERE status = 405),
    COUNT(*) FILTER (WHERE status = 429),
    COUNT(*) FILTER (WHERE status = 500),
    COUNT(*) FILTER (WHERE status = 503),
    COUNT(DISTINCT hashed_ip),
    COUNT(*) FILTER (WHERE status BETWEEN 400 AND 499),
    COUNT(*) FILTER (WHERE status BETWEEN 500 AND 599),
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY process_time),
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY process_time)
FROM requests_logs
WHERE created_at < CURRENT_DATE AND ratelimit_limit >= 0
GROUP BY 1;


-- Curseur : tout ce qui existe deja au moment du backfill est couvert,
-- StatsAggregator ne reprendra qu'a partir de ce point
UPDATE stats_watermark
SET last_processed_at = (SELECT COALESCE(MAX(created_at), '1970-01-01'::timestamp) FROM requests_logs)
WHERE id = 1;

COMMIT;
