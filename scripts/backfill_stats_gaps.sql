/***************************************************************
    *  CROUStillantData - backfill_stats_gaps.sql
    *  Created by: CROUStillant Développement
    *  Description: Comble les trous laisses dans stats_hourly / stats_daily
    *  par le seed initial (backfill_stats.sql), qui agrege via GROUP BY et
    *  donc n'ecrit aucune ligne pour les heures/jours a zero requete.
    *  StatsAggregator (__finalize_hours / __finalize_days) ne repart
    *  jamais en arriere de MAX(hour) / MAX(day) : ces trous restent
    *  definitifs tant qu'ils ne sont pas combles manuellement, et
    *  archiver.py refuse d'archiver toute partition qui les recouvre.
    *
    *  Contrairement a backfill_stats.sql, ce script est un complement
    *  ciblé : il n'ecrit QUE les heures/jours manquants (via NOT EXISTS),
    *  ne touche a aucune ligne existante, et peut donc etre rejoue sans
    *  risque meme apres que des partitions aient ete archivees (il se
    *  contente de ne rien trouver a combler pour les periodes deja
    *  archivees puisqu'elles ont deja leurs lignes).
    *
    *  Necessite que requests_logs couvre encore la periode a combler
    *  (partitions non encore archivees) — sinon la seule source encore
    *  vraie est le trou lui-meme, il n'y a plus rien a recalculer.
***************************************************************/

BEGIN;

WITH missing_hours AS (
    SELECT h
    FROM generate_series(
        (SELECT DATE_TRUNC('hour', MIN(created_at)) FROM requests_logs),
        DATE_TRUNC('hour', LOCALTIMESTAMP) - INTERVAL '1 hour',
        INTERVAL '1 hour'
    ) AS h
    WHERE NOT EXISTS (SELECT 1 FROM stats_hourly s WHERE s.hour = h)
)
INSERT INTO stats_hourly (
    hour, requests, error_count, unique_visitors,
    sum_ratelimit_used, count_ratelimit_used,
    sum_process_time, count_process_time, under_200ms_count,
    sum_ratelimit_limit, count_ratelimit_limit,
    sum_ratelimit_ratio, count_ratelimit_ratio,
    max_ratelimit_ratio, near_limit_count
)
SELECT
    mh.h,
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
FROM missing_hours mh
LEFT JOIN requests_logs rl ON DATE_TRUNC('hour', rl.created_at) = mh.h
GROUP BY mh.h
ON CONFLICT (hour) DO NOTHING;


WITH missing_days AS (
    SELECT d
    FROM generate_series(
        (SELECT DATE_TRUNC('day', MIN(created_at))::date FROM requests_logs),
        CURRENT_DATE - 1,
        INTERVAL '1 day'
    ) AS d
    WHERE NOT EXISTS (SELECT 1 FROM stats_daily s WHERE s.day = d::date)
)
INSERT INTO stats_daily (
    day, status_200, status_302, status_400, status_404,
    status_405, status_429, status_500, status_503,
    unique_ips, errors_4xx, errors_5xx,
    p50_process_time, p95_process_time
)
SELECT
    md.d::date,
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
FROM missing_days md
LEFT JOIN requests_logs rl
    ON DATE_TRUNC('day', rl.created_at)::date = md.d
    AND rl.ratelimit_limit >= 0
GROUP BY md.d
ON CONFLICT (day) DO NOTHING;

COMMIT;
