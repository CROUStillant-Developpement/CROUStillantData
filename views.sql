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
--
-- requests_logs est partitionnee par mois et vouee a etre archivee puis
-- purgee localement (voir CROUStillantData/archiver.py). Les vues ci-dessous
-- ne scannent donc plus requests_logs directement : elles lisent des tables
-- permanentes (schema.sql) maintenues de facon incrementale par
-- StatsAggregator (stats_aggregator.py), qui restent correctes meme apres
-- l'archivage des vieilles partitions. Ce sont de simples CREATE VIEW (pas
-- MATERIALIZED) : les tables sources sont deja la "materialisation", donc
-- aucun refresh ni entree dans referential.json n'est necessaire pour elles.

-- Vue pour le total des requêtes dans les logs
CREATE OR REPLACE VIEW v_gf_total_requests AS
SELECT
    1 AS id,
    TOTAL_REQUESTS AS "Total"
FROM stats_counters
WHERE ID = 1;


-- Vue pour les requêtes vers /v1/status
CREATE OR REPLACE VIEW v_gf_status_requests AS
SELECT
    1 AS id,
    STATUS_REQUESTS AS "Total"
FROM stats_counters
WHERE ID = 1;


-- Vue pour les IPs uniques
CREATE OR REPLACE VIEW v_gf_unique_ips AS
SELECT
    1 AS id,
    COUNT(*) AS "Total"
FROM unique_hashed_ips;


-- Vue pour les status 200
CREATE OR REPLACE VIEW v_gf_status_200 AS
SELECT
    1 AS id,
    STATUS_200 AS "Total"
FROM stats_counters
WHERE ID = 1;


-- Vue pour les status 404
CREATE OR REPLACE VIEW v_gf_status_404 AS
SELECT
    1 AS id,
    STATUS_404 AS "Total"
FROM stats_counters
WHERE ID = 1;


-- Vue pour les status 500
CREATE OR REPLACE VIEW v_gf_status_500 AS
SELECT
    1 AS id,
    STATUS_500 AS "Total"
FROM stats_counters
WHERE ID = 1;


-- Vue pour les status 503
CREATE OR REPLACE VIEW v_gf_status_503 AS
SELECT
    1 AS id,
    STATUS_503 AS "Total"
FROM stats_counters
WHERE ID = 1;


-- Vue pour les requêtes avec une clé API
CREATE OR REPLACE VIEW v_gf_requests_with_key AS
SELECT
    1 AS id,
    REQUESTS_WITH_KEY AS "Total"
FROM stats_counters
WHERE ID = 1;


-- Vue pour le max ratelimit used
CREATE OR REPLACE VIEW v_gf_max_ratelimit_used AS
SELECT
    1 AS id,
    MAX_RATELIMIT_USED AS "Max"
FROM stats_counters
WHERE ID = 1;


-- Vue pour le avg ratelimit used
CREATE OR REPLACE VIEW v_gf_avg_ratelimit_used AS
SELECT
    1 AS id,
    (SUM_RATELIMIT_USED::numeric / NULLIF(COUNT_RATELIMIT_USED, 0)) AS "Moyenne"
FROM stats_counters
WHERE ID = 1;


-- Vue pour le max ratelimit limit
CREATE OR REPLACE VIEW v_gf_max_ratelimit_limit AS
SELECT
    1 AS id,
    MAX_RATELIMIT_LIMIT AS "Max"
FROM stats_counters
WHERE ID = 1;


-- Vue pour le nombre de buckets (table de config, non concernee par
-- l'archivage : reste materialisee et rafraichie via referential.json)
CREATE MATERIALIZED VIEW IF NOT EXISTS v_gf_bucket_count AS
SELECT
    1 AS id,
    COUNT(*) AS "Total"
FROM bucket
WITH DATA;


-- Vue pour les clés API distinctes
CREATE OR REPLACE VIEW v_gf_distinct_keys AS
SELECT
    1 AS id,
    COUNT(*) AS "Total"
FROM unique_keys;


-- Vue pour la répartition des status codes
CREATE OR REPLACE VIEW v_gf_status_breakdown AS
SELECT
    1 AS id,
    STATUS_200 AS "200",
    BREAKDOWN_302 AS "302",
    BREAKDOWN_400 AS "400",
    STATUS_404 AS "404",
    BREAKDOWN_405 AS "405",
    BREAKDOWN_429 AS "429",
    STATUS_500 AS "500",
    STATUS_503 AS "503"
FROM stats_counters
WHERE ID = 1;


-- Vue pour les requêtes par version d'API
CREATE OR REPLACE VIEW v_gf_requests_by_api_version AS
SELECT
    VERSION AS "Version",
    TOTAL AS "Total"
FROM stats_by_api_version;


-- Vue pour le taux d'erreurs horaire
CREATE OR REPLACE VIEW v_gf_hourly_error_rate AS
SELECT
    HOUR AS "Hour",
    100.0 * ERROR_COUNT::float / NULLIF(REQUESTS, 0) AS "Taux d'erreurs en %"
FROM stats_hourly;


-- Vue pour les requêtes horaires
CREATE OR REPLACE VIEW v_gf_hourly_requests AS
SELECT
    HOUR AS "Heure",
    REQUESTS AS "Requêtes"
FROM stats_hourly;


-- Vue pour les requêtes totales par heure de la journée (0-23, cumulé sur
-- tout l'historique - comportement de l'ancienne vue préservé malgré le nom)
CREATE OR REPLACE VIEW v_gf_hourly_total_24h AS
SELECT
    HOUR_OF_DAY AS hour,
    TOTAL AS "Total"
FROM stats_hour_of_day_24h;


-- Vue pour les visiteurs uniques horaires
CREATE OR REPLACE VIEW v_gf_hourly_unique_visitors AS
SELECT
    HOUR AS "Hour",
    UNIQUE_VISITORS AS "Visiteurs uniques"
FROM stats_hourly;


-- Vue pour les IPs uniques par heure de la journée (0-23, cumulé sur tout
-- l'historique - comportement de l'ancienne vue préservé malgré le nom)
CREATE OR REPLACE VIEW v_gf_hourly_unique_ips_24h AS
SELECT
    HOUR_OF_DAY AS hour,
    COUNT(*) AS "Total"
FROM unique_ips_by_hour_of_day
GROUP BY HOUR_OF_DAY;


-- Vue pour le ratelimit utilisé moyen horaire
CREATE OR REPLACE VIEW v_gf_hourly_ratelimit_used AS
SELECT
    HOUR AS "Heure",
    (SUM_RATELIMIT_USED::numeric / NULLIF(COUNT_RATELIMIT_USED, 0)) AS "Ratelimit Utilisée"
FROM stats_hourly;


-- Vue pour le status 200 quotidien
CREATE OR REPLACE VIEW v_gf_daily_status_200 AS
SELECT DAY AS "Jour", STATUS_200 AS "200" FROM stats_daily;


-- Vue pour le status 302 quotidien
CREATE OR REPLACE VIEW v_gf_daily_status_302 AS
SELECT DAY AS "Jour", STATUS_302 AS "302" FROM stats_daily;


-- Vue pour le status 400 quotidien
CREATE OR REPLACE VIEW v_gf_daily_status_400 AS
SELECT DAY AS "Jour", STATUS_400 AS "400" FROM stats_daily;


-- Vue pour le status 404 quotidien
CREATE OR REPLACE VIEW v_gf_daily_status_404 AS
SELECT DAY AS "Jour", STATUS_404 AS "404" FROM stats_daily;


-- Vue pour le status 405 quotidien
CREATE OR REPLACE VIEW v_gf_daily_status_405 AS
SELECT DAY AS "Jour", STATUS_405 AS "405" FROM stats_daily;


-- Vue pour le status 429 quotidien
CREATE OR REPLACE VIEW v_gf_daily_status_429 AS
SELECT DAY AS "Jour", STATUS_429 AS "429" FROM stats_daily;


-- Vue pour le status 500 quotidien
CREATE OR REPLACE VIEW v_gf_daily_status_500 AS
SELECT DAY AS "Jour", STATUS_500 AS "500" FROM stats_daily;


-- Vue pour le status 503 quotidien
CREATE OR REPLACE VIEW v_gf_daily_status_503 AS
SELECT DAY AS "Jour", STATUS_503 AS "503" FROM stats_daily;


-- Vue pour le temps de traitement moyen horaire
CREATE OR REPLACE VIEW v_gf_hourly_process_time AS
SELECT
    HOUR AS "Heure",
    (SUM_PROCESS_TIME::numeric / NULLIF(COUNT_PROCESS_TIME, 0)) AS "Process Time"
FROM stats_hourly;


-- Vue pour le pourcentage de requêtes sous 200ms horaire
CREATE OR REPLACE VIEW v_gf_hourly_under_200ms AS
SELECT
    HOUR AS "Hour",
    100.0 * UNDER_200MS_COUNT::float / NULLIF(REQUESTS, 0) AS "% < 200ms"
FROM stats_hourly;


-- Vue pour les requêtes par méthode HTTP
CREATE OR REPLACE VIEW v_gf_requests_by_method AS
SELECT
    METHOD AS "Method",
    TOTAL AS "Total"
FROM stats_by_method;


-- Vue pour les 10 principaux paramètres dans les requêtes
CREATE OR REPLACE VIEW v_gf_top_params AS
SELECT
    PARAM AS "Param",
    TOTAL AS "Total"
FROM stats_by_param
ORDER BY TOTAL DESC
LIMIT 10;


-- Index pour la seule vue LOGS restee materialisee
CREATE UNIQUE INDEX IF NOT EXISTS idx_v_gf_bucket_count_id ON v_gf_bucket_count (id);
