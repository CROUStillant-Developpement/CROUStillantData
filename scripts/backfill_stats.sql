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
-- terminée).
--
-- Génération via generate_series + LEFT JOIN (et non un simple GROUP BY) :
-- une heure/jour sans la moindre requête doit quand même produire une ligne
-- a zero, exactement comme le fait StatsAggregator.__finalize_hours /
-- __finalize_days en régime incrémental. Un GROUP BY nu omettrait ces
-- périodes silencieusement, et comme StatsAggregator ne revient jamais en
-- arrière de MAX(hour) / MAX(day), le trou resterait définitif (voir
-- backfill_stats_gaps.sql, qui comble ce cas s'il s'est déjà produit).
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
    h,
    COUNT(rl.id),
    COUNT(rl.id) FILTER (WHERE rl.status >= 400),
    COUNT(DISTINCT rl.hashed_ip),
    COALESCE(SUM(rl.ratelimit_used) FILTER (WHERE rl.ratelimit_limit >= 0), 0),
    COUNT(rl.id) FILTER (WHERE rl.ratelimit_limit >= 0),
    COALESCE(SUM(rl.process_time) FILTER (WHERE rl.ratelimit_limit >= 0), 0),
    COUNT(rl.id) FILTER (WHERE rl.ratelimit_limit >= 0),
    COUNT(rl.id) FILTER (WHERE rl.process_time < 200),
    COALESCE(SUM(rl.ratelimit_limit) FILTER (WHERE rl.ratelimit_limit != -1), 0),
    COUNT(rl.id) FILTER (WHERE rl.ratelimit_limit != -1),
    COALESCE(SUM(rl.ratelimit_used::numeric / rl.ratelimit_limit) FILTER (WHERE rl.ratelimit_limit > 0), 0),
    COUNT(rl.id) FILTER (WHERE rl.ratelimit_limit > 0),
    COALESCE(MAX(rl.ratelimit_used::numeric / rl.ratelimit_limit) FILTER (WHERE rl.ratelimit_limit > 0), 0),
    COUNT(rl.id) FILTER (WHERE rl.ratelimit_limit > 0 AND rl.ratelimit_used >= 0.8 * rl.ratelimit_limit)
FROM generate_series(
    (SELECT DATE_TRUNC('hour', MIN(created_at)) FROM requests_logs),
    DATE_TRUNC('hour', NOW()) - INTERVAL '1 hour',
    INTERVAL '1 hour'
) AS h
LEFT JOIN requests_logs rl ON DATE_TRUNC('hour', rl.created_at) = h
GROUP BY h;

TRUNCATE stats_daily;
INSERT INTO stats_daily (
    day, status_200, status_302, status_400, status_404,
    status_405, status_429, status_500, status_503,
    unique_ips, errors_4xx, errors_5xx,
    p50_process_time, p95_process_time
)
SELECT
    d::date,
    COUNT(*) FILTER (WHERE rl.status = 200),
    COUNT(*) FILTER (WHERE rl.status = 302),
    COUNT(*) FILTER (WHERE rl.status = 400),
    COUNT(*) FILTER (WHERE rl.status = 404),
    COUNT(*) FILTER (WHERE rl.status = 405),
    COUNT(*) FILTER (WHERE rl.status = 429),
    COUNT(*) FILTER (WHERE rl.status = 500),
    COUNT(*) FILTER (WHERE rl.status = 503),
    COUNT(DISTINCT rl.hashed_ip),
    COUNT(*) FILTER (WHERE rl.status BETWEEN 400 AND 499),
    COUNT(*) FILTER (WHERE rl.status BETWEEN 500 AND 599),
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY rl.process_time),
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY rl.process_time)
FROM generate_series(
    (SELECT DATE_TRUNC('day', MIN(created_at))::date FROM requests_logs),
    CURRENT_DATE - 1,
    INTERVAL '1 day'
) AS d
LEFT JOIN requests_logs rl
    ON DATE_TRUNC('day', rl.created_at)::date = d
    AND rl.ratelimit_limit >= 0
GROUP BY d;


-- Curseur : tout ce qui existe deja au moment du backfill est couvert,
-- StatsAggregator ne reprendra qu'a partir de ce point
UPDATE stats_watermark
SET last_processed_at = (SELECT COALESCE(MAX(created_at), '1970-01-01'::timestamp) FROM requests_logs)
WHERE id = 1;

COMMIT;
