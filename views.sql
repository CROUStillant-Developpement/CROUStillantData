/***************************************************************
    *  CROUStillant - views.sql
    *  Created by: CROUStillant Développement
    *  Created on: 06/11/2025
    *  Updated on: 06/11/2025
    *  Description: SQL database scheme for the CROUStillant project
***************************************************************/


-- ================================================
-- TACHE • Vues pour les statistiques de tâches
-- ================================================

-- Vue pour les tâches avec des dates de début et de fin définies
CREATE OR REPLACE VIEW v_gf_tache_base AS
SELECT *
FROM TACHE
WHERE DEBUT IS NOT NULL AND FIN IS NOT NULL;


-- Vue matérialisée pour la durée moyenne des tâches par jour
CREATE MATERIALIZED VIEW IF NOT EXISTS v_gf_tache_duree_jour AS
SELECT
    DATE_TRUNC('day', DEBUT) AS "Jour",
    AVG(EXTRACT(EPOCH FROM (FIN - DEBUT))) AS "Durée"
FROM v_gf_tache_base
GROUP BY DATE_TRUNC('day', DEBUT)
WITH DATA;


-- Vue matérialisée pour le nombre de requêtes par jour
CREATE MATERIALIZED VIEW IF NOT EXISTS v_gf_tache_requetes_jour AS
SELECT
    DATE(FIN) AS "Date",
    SUM(REQUETES) AS "Requêtes"
FROM v_gf_tache_base
GROUP BY DATE(FIN)
WITH DATA;


-- Vue matérialisée pour le nombre de tâches par jour
CREATE MATERIALIZED VIEW IF NOT EXISTS v_gf_tache_nb_jour AS
SELECT
    DATE(FIN) AS "Date",
    COUNT(*) AS "Tâches"
FROM v_gf_tache_base
GROUP BY DATE(FIN)
WITH DATA;


-- Vue matérialisée pour le delta des entités par jour
CREATE MATERIALIZED VIEW IF NOT EXISTS v_gf_tache_delta_jour AS
SELECT
    DATE(FIN) AS "Date",
    SUM(FIN_REGIONS - DEBUT_REGIONS) AS "Régions",
    SUM(FIN_RESTAURANTS - DEBUT_RESTAURANTS) AS "Restaurants",
    SUM(FIN_TYPES_RESTAURANTS - DEBUT_TYPES_RESTAURANTS) AS "Types Restaurants",
    SUM(FIN_MENUS - DEBUT_MENUS) AS "Menus",
    SUM(FIN_REPAS - DEBUT_REPAS) AS "Repas",
    SUM(FIN_CATEGORIES - DEBUT_CATEGORIES) AS "Catégories",
    SUM(FIN_PLATS - DEBUT_PLATS) AS "Plats",
    SUM(FIN_COMPOSITIONS - DEBUT_COMPOSITIONS) AS "Compositions"
FROM v_gf_tache_base
GROUP BY DATE(FIN)
WITH DATA;


-- Index pour pouvoir faire des requêtes sur les vues lors de leur actualisation
CREATE UNIQUE INDEX IF NOT EXISTS v_gf_tache_duree_jour_idx ON v_gf_tache_duree_jour ("Jour");
CREATE UNIQUE INDEX IF NOT EXISTS v_gf_tache_requetes_jour_idx ON v_gf_tache_requetes_jour ("Date");
CREATE UNIQUE INDEX IF NOT EXISTS v_gf_tache_nb_jour_idx ON v_gf_tache_nb_jour ("Date");
CREATE UNIQUE INDEX IF NOT EXISTS v_gf_tache_delta_jour_idx ON v_gf_tache_delta_jour ("Date");


-- ================================================
-- LOGS • Vues pour les statistiques des logs
-- ================================================

-- Vue pour le total des requêtes dans les logs
CREATE MATERIALIZED VIEW IF NOT EXISTS v_gf_total_requests AS
SELECT 
    1 AS id,
    COUNT(*) AS "Total"
FROM requests_logs
WITH DATA;


-- Vue pour les requêtes vers /v1/status
CREATE MATERIALIZED VIEW IF NOT EXISTS v_gf_status_requests AS
SELECT
    1 AS id,
    COUNT(*) AS "Total"
FROM requests_logs
WHERE ratelimit_limit >= 0 AND path = '/v1/status'
WITH DATA;


-- Vue pour les IPs uniques
CREATE MATERIALIZED VIEW IF NOT EXISTS v_gf_unique_ips AS
SELECT
    1 AS id,
    COUNT(DISTINCT hashed_ip) AS "Total"
FROM requests_logs
WITH DATA;


-- Vue pour les status 200
CREATE MATERIALIZED VIEW IF NOT EXISTS v_gf_status_200 AS
SELECT 
    1 AS id,
    COUNT(*) AS "Total"
FROM requests_logs
WHERE ratelimit_limit >= 0 AND status = 200
WITH DATA;


-- Vue pour les status 404
CREATE MATERIALIZED VIEW IF NOT EXISTS v_gf_status_404 AS
SELECT 
    1 AS id,
    COUNT(*) AS "Total"
FROM requests_logs
WHERE ratelimit_limit >= 0 AND status = 404
WITH DATA;


-- Vue pour les status 500
CREATE MATERIALIZED VIEW IF NOT EXISTS v_gf_status_500 AS
SELECT 
    1 AS id,
    COUNT(*) AS "Total"
FROM requests_logs
WHERE ratelimit_limit >= 0 AND status = 500
WITH DATA;


-- Vue pour les status 503
CREATE MATERIALIZED VIEW IF NOT EXISTS v_gf_status_503 AS
SELECT 
    1 AS id,
    COUNT(*) AS "Total"
FROM requests_logs
WHERE ratelimit_limit >= 0 AND status = 503
WITH DATA;


-- Vue pour les requêtes avec une clé API
CREATE MATERIALIZED VIEW IF NOT EXISTS v_gf_requests_with_key AS
SELECT 
    1 AS id,
    COUNT(*) AS "Total"
FROM requests_logs
WHERE ratelimit_limit >= 0 AND key IS NOT NULL
WITH DATA;


-- Vue pour le max ratelimit used
CREATE MATERIALIZED VIEW IF NOT EXISTS v_gf_max_ratelimit_used AS
SELECT 
    1 AS id,
    MAX(ratelimit_used) AS "Max"
FROM requests_logs
WITH DATA;


-- Vue pour le avg ratelimit used
CREATE MATERIALIZED VIEW IF NOT EXISTS v_gf_avg_ratelimit_used AS
SELECT 
    1 AS id,
    AVG(ratelimit_used) AS "Moyenne"
FROM requests_logs
WHERE ratelimit_used >= 0
WITH DATA;


-- Vue pour le max ratelimit limit
CREATE MATERIALIZED VIEW IF NOT EXISTS v_gf_max_ratelimit_limit AS
SELECT 
    1 AS id,
    MAX(ratelimit_limit) AS "Max"
FROM requests_logs
WHERE ratelimit_limit >= 0
WITH DATA;


-- Vue pour le nombre de buckets
CREATE MATERIALIZED VIEW IF NOT EXISTS v_gf_bucket_count AS
SELECT 
    1 AS id,
    COUNT(*) AS "Total"
FROM bucket
WITH DATA;


-- Vue pour les clés API distinctes
CREATE MATERIALIZED VIEW IF NOT EXISTS v_gf_distinct_keys AS
SELECT 
    1 AS id,
    COUNT(DISTINCT key) AS "Total"
FROM requests_logs
WHERE ratelimit_limit >= 0
WITH DATA;


-- Vue pour la répartition des status codes
CREATE MATERIALIZED VIEW IF NOT EXISTS v_gf_status_breakdown AS
SELECT
    1 AS id,
    COUNT(*) FILTER (WHERE status = '200') AS "200",
    COUNT(*) FILTER (WHERE status = '302') AS "302",
    COUNT(*) FILTER (WHERE status = '400') AS "400",
    COUNT(*) FILTER (WHERE status = '404') AS "404",
    COUNT(*) FILTER (WHERE status = '405') AS "405",
    COUNT(*) FILTER (WHERE status = '429') AS "429",
    COUNT(*) FILTER (WHERE status = '500') AS "500",
    COUNT(*) FILTER (WHERE status = '503') AS "503"
FROM requests_logs
WHERE ratelimit_limit >= 0
WITH DATA;


-- Vue pour les requêtes par version d'API
CREATE MATERIALIZED VIEW IF NOT EXISTS v_gf_requests_by_api_version AS
SELECT 
    api_version AS "Version", 
    COUNT(*) AS "Total"
FROM requests_logs
WHERE ratelimit_limit >= 0
GROUP BY api_version
WITH DATA;


-- Vue pour le taux d'erreurs horaire
CREATE MATERIALIZED VIEW IF NOT EXISTS v_gf_hourly_error_rate AS
SELECT 
    DATE_TRUNC('hour', created_at) AS "Hour",
    100.0 * SUM(CASE WHEN status >= 400 THEN 1 ELSE 0 END)::float / COUNT(*) AS "Taux d'erreurs en %"
FROM requests_logs
GROUP BY DATE_TRUNC('hour', created_at)
WITH DATA;


-- Vue pour les requêtes horaires
CREATE MATERIALIZED VIEW IF NOT EXISTS v_gf_hourly_requests AS
SELECT 
    DATE_TRUNC('hour', created_at) AS "Heure",
    COUNT(*) AS "Requêtes"
FROM requests_logs
GROUP BY DATE_TRUNC('hour', created_at)
WITH DATA;


-- Vue pour les requêtes totales des dernières 24 heures
CREATE MATERIALIZED VIEW IF NOT EXISTS v_gf_hourly_total_24h AS
SELECT 
    EXTRACT(HOUR FROM created_at) AS hour,
    COUNT(*) AS "Total"
FROM requests_logs
WHERE ratelimit_limit >= 0
GROUP BY hour
LIMIT 24
WITH DATA;


-- Vue pour les visiteurs uniques horaires
CREATE MATERIALIZED VIEW IF NOT EXISTS v_gf_hourly_unique_visitors AS
SELECT 
    DATE_TRUNC('hour', created_at) AS "Hour",
    COUNT(DISTINCT hashed_ip) AS "Visiteurs uniques"
FROM requests_logs
GROUP BY DATE_TRUNC('hour', created_at)
WITH DATA;


-- Vue pour les IPs uniques des dernières 24 heures
CREATE MATERIALIZED VIEW IF NOT EXISTS v_gf_hourly_unique_ips_24h AS
SELECT 
    EXTRACT(HOUR FROM created_at) AS hour,
    COUNT(DISTINCT hashed_ip) AS "Total"
FROM requests_logs
WHERE ratelimit_limit >= 0
GROUP BY hour
LIMIT 24
WITH DATA;


-- Vue pour le ratelimit utilisé moyen horaire
CREATE MATERIALIZED VIEW IF NOT EXISTS v_gf_hourly_ratelimit_used AS
SELECT 
    DATE_TRUNC('hour', created_at) AS "Heure",
    AVG(ratelimit_used) AS "Ratelimit Utilisée"
FROM requests_logs
WHERE ratelimit_limit >= 0
GROUP BY DATE_TRUNC('hour', created_at)
WITH DATA;


-- Vue pour le status 200 quotidien
CREATE MATERIALIZED VIEW IF NOT EXISTS v_gf_daily_status_200 AS
SELECT 
    DATE_TRUNC('day', created_at) AS "Jour",
    COUNT(*) AS "200"
FROM requests_logs
WHERE ratelimit_limit >= 0 AND status = 200
GROUP BY DATE_TRUNC('day', created_at)
WITH DATA;


-- Vue pour le status 302 quotidien
CREATE MATERIALIZED VIEW IF NOT EXISTS v_gf_daily_status_302 AS
SELECT 
    DATE_TRUNC('day', created_at) AS "Jour",
    COUNT(*) AS "302"
FROM requests_logs
WHERE ratelimit_limit >= 0 AND status = 302
GROUP BY DATE_TRUNC('day', created_at)
WITH DATA;


-- Vue pour le status 400 quotidien
CREATE MATERIALIZED VIEW IF NOT EXISTS v_gf_daily_status_400 AS
SELECT 
    DATE_TRUNC('day', created_at) AS "Jour",
    COUNT(*) AS "400"
FROM requests_logs
WHERE ratelimit_limit >= 0 AND status = 400
GROUP BY DATE_TRUNC('day', created_at)
WITH DATA;


-- Vue pour le status 404 quotidien
CREATE MATERIALIZED VIEW IF NOT EXISTS v_gf_daily_status_404 AS
SELECT 
    DATE_TRUNC('day', created_at) AS "Jour",
    COUNT(*) AS "404"
FROM requests_logs
WHERE ratelimit_limit >= 0 AND status = 404
GROUP BY DATE_TRUNC('day', created_at)
WITH DATA;


-- Vue pour le status 405 quotidien
CREATE MATERIALIZED VIEW IF NOT EXISTS v_gf_daily_status_405 AS
SELECT 
    DATE_TRUNC('day', created_at) AS "Jour",
    COUNT(*) AS "405"
FROM requests_logs
WHERE ratelimit_limit >= 0 AND status = 405
GROUP BY DATE_TRUNC('day', created_at)
WITH DATA;


-- Vue pour le status 429 quotidien
CREATE MATERIALIZED VIEW IF NOT EXISTS v_gf_daily_status_429 AS
SELECT 
    DATE_TRUNC('day', created_at) AS "Jour",
    COUNT(*) AS "429"
FROM requests_logs
WHERE ratelimit_limit >= 0 AND status = 429
GROUP BY DATE_TRUNC('day', created_at)
WITH DATA;


-- Vue pour le status 500 quotidien
CREATE MATERIALIZED VIEW IF NOT EXISTS v_gf_daily_status_500 AS
SELECT 
    DATE_TRUNC('day', created_at) AS "Jour",
    COUNT(*) AS "500"
FROM requests_logs
WHERE ratelimit_limit >= 0 AND status = 500
GROUP BY DATE_TRUNC('day', created_at)
WITH DATA;


-- Vue pour le status 503 quotidien
CREATE MATERIALIZED VIEW IF NOT EXISTS v_gf_daily_status_503 AS
SELECT 
    DATE_TRUNC('day', created_at) AS "Jour",
    COUNT(*) AS "503"
FROM requests_logs
WHERE ratelimit_limit >= 0 AND status = 503
GROUP BY DATE_TRUNC('day', created_at)
WITH DATA;


-- Vue pour le temps de traitement moyen horaire
CREATE MATERIALIZED VIEW IF NOT EXISTS v_gf_hourly_process_time AS
SELECT 
    DATE_TRUNC('hour', created_at) AS "Heure",
    AVG(process_time) AS "Process Time"
FROM requests_logs
WHERE ratelimit_limit >= 0
GROUP BY DATE_TRUNC('hour', created_at)
WITH DATA;


-- Vue pour le pourcentage de requêtes sous 200ms horaire
CREATE MATERIALIZED VIEW IF NOT EXISTS v_gf_hourly_under_200ms AS
SELECT 
    DATE_TRUNC('hour', created_at) AS "Hour",
    100.0 * SUM(CASE WHEN process_time < 200 THEN 1 ELSE 0 END)::float / COUNT(*) AS "% < 200ms"
FROM requests_logs
GROUP BY DATE_TRUNC('hour', created_at)
WITH DATA;


-- Vue pour les requêtes par méthode HTTP
CREATE MATERIALIZED VIEW IF NOT EXISTS v_gf_requests_by_method AS
SELECT 
    method AS "Method",
    COUNT(*) AS "Total"
FROM requests_logs
WHERE ratelimit_limit >= 0
GROUP BY method
WITH DATA;


-- Vue pour les 10 principaux paramètres dans les requêtes
CREATE MATERIALIZED VIEW IF NOT EXISTS v_gf_top_params AS
SELECT 
    p.key AS "Param",
    COUNT(*) AS "Total"
FROM requests_logs, jsonb_each_text(params) AS p
GROUP BY p.key
LIMIT 10
WITH DATA;


-- Index pour pouvoir faire des requêtes sur les vues lors de leur actualisation
CREATE UNIQUE INDEX IF NOT EXISTS idx_v_gf_total_requests_id ON v_gf_total_requests (id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_v_gf_status_requests_id ON v_gf_status_requests (id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_v_gf_unique_ips_id ON v_gf_unique_ips (id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_v_gf_status_200_id ON v_gf_status_200 (id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_v_gf_status_404_id ON v_gf_status_404 (id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_v_gf_status_500_id ON v_gf_status_500 (id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_v_gf_status_503_id ON v_gf_status_503 (id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_v_gf_requests_with_key_id ON v_gf_requests_with_key (id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_v_gf_max_ratelimit_used_id ON v_gf_max_ratelimit_used (id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_v_gf_avg_ratelimit_used_id ON v_gf_avg_ratelimit_used (id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_v_gf_max_ratelimit_limit_id ON v_gf_max_ratelimit_limit (id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_v_gf_bucket_count_id ON v_gf_bucket_count (id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_v_gf_distinct_keys_id ON v_gf_distinct_keys (id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_v_gf_status_breakdown_id ON v_gf_status_breakdown (id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_v_gf_requests_by_api_version_version ON v_gf_requests_by_api_version ("Version");
CREATE UNIQUE INDEX IF NOT EXISTS idx_v_gf_hourly_error_rate_hour ON v_gf_hourly_error_rate ("Hour");
CREATE UNIQUE INDEX IF NOT EXISTS idx_v_gf_hourly_requests_heure ON v_gf_hourly_requests ("Heure");
CREATE UNIQUE INDEX IF NOT EXISTS idx_v_gf_hourly_total_24h_hour ON v_gf_hourly_total_24h (hour);
CREATE UNIQUE INDEX IF NOT EXISTS idx_v_gf_hourly_unique_visitors_hour ON v_gf_hourly_unique_visitors ("Hour");
CREATE UNIQUE INDEX IF NOT EXISTS idx_v_gf_hourly_unique_ips_24h_hour ON v_gf_hourly_unique_ips_24h (hour);
CREATE UNIQUE INDEX IF NOT EXISTS idx_v_gf_hourly_ratelimit_used_heure ON v_gf_hourly_ratelimit_used ("Heure");
CREATE UNIQUE INDEX IF NOT EXISTS idx_v_gf_daily_status_200_jour ON v_gf_daily_status_200 ("Jour");
CREATE UNIQUE INDEX IF NOT EXISTS idx_v_gf_daily_status_302_jour ON v_gf_daily_status_302 ("Jour");
CREATE UNIQUE INDEX IF NOT EXISTS idx_v_gf_daily_status_400_jour ON v_gf_daily_status_400 ("Jour");
CREATE UNIQUE INDEX IF NOT EXISTS idx_v_gf_daily_status_404_jour ON v_gf_daily_status_404 ("Jour");
CREATE UNIQUE INDEX IF NOT EXISTS idx_v_gf_daily_status_405_jour ON v_gf_daily_status_405 ("Jour");
CREATE UNIQUE INDEX IF NOT EXISTS idx_v_gf_daily_status_429_jour ON v_gf_daily_status_429 ("Jour");
CREATE UNIQUE INDEX IF NOT EXISTS idx_v_gf_daily_status_500_jour ON v_gf_daily_status_500 ("Jour");
CREATE UNIQUE INDEX IF NOT EXISTS idx_v_gf_daily_status_503_jour ON v_gf_daily_status_503 ("Jour");
CREATE UNIQUE INDEX IF NOT EXISTS idx_v_gf_hourly_process_time_heure ON v_gf_hourly_process_time ("Heure");
CREATE UNIQUE INDEX IF NOT EXISTS idx_v_gf_hourly_under_200ms_hour ON v_gf_hourly_under_200ms ("Hour");
CREATE UNIQUE INDEX IF NOT EXISTS idx_v_gf_requests_by_method_method ON v_gf_requests_by_method ("Method");
CREATE UNIQUE INDEX IF NOT EXISTS idx_v_gf_top_params_param ON v_gf_top_params ("Param");
