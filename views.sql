/***************************************************************
    *  CROUStillant - views.sql
    *  Created by: CROUStillant Développement
    *  Created on: 06/11/2025
    *  Updated on: 09/09/2026
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
--
-- Deux vues font exception et lisent encore requests_logs :
-- v_gf_hourly_pending et v_gf_daily_pending. Elles ne couvrent que la
-- tranche pas encore figee par StatsAggregator (l'heure et le jour en
-- cours), pour que les graphes horaires et quotidiens n'attendent pas la
-- cloture de la periode. Voir le commentaire detaille au-dessus de
-- v_gf_daily_pending pour les garanties (pas de doublon, pas de scan de
-- l'historique, compatible avec l'archivage).

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


-- Heure(s) pas encore figee(s) dans stats_hourly : meme principe que
-- v_gf_daily_pending, applique a l'heure calendaire en cours (voir
-- __finalize_hours). Sans elle, tous les graphes horaires accusaient
-- jusqu'a une heure de retard.
CREATE OR REPLACE VIEW v_gf_hourly_pending AS
SELECT
    DATE_TRUNC('hour', CREATED_AT) AS HOUR,
    COUNT(*) AS REQUESTS,
    COUNT(*) FILTER (WHERE STATUS >= 400) AS ERROR_COUNT,
    COUNT(DISTINCT HASHED_IP) AS UNIQUE_VISITORS,
    COALESCE(SUM(RATELIMIT_USED) FILTER (WHERE RATELIMIT_LIMIT >= 0), 0) AS SUM_RATELIMIT_USED,
    COUNT(*) FILTER (WHERE RATELIMIT_LIMIT >= 0) AS COUNT_RATELIMIT_USED,
    COALESCE(SUM(PROCESS_TIME) FILTER (WHERE RATELIMIT_LIMIT >= 0), 0) AS SUM_PROCESS_TIME,
    COUNT(*) FILTER (WHERE RATELIMIT_LIMIT >= 0) AS COUNT_PROCESS_TIME,
    COUNT(*) FILTER (WHERE PROCESS_TIME < 200) AS UNDER_200MS_COUNT,
    COALESCE(SUM(RATELIMIT_LIMIT) FILTER (WHERE RATELIMIT_LIMIT != -1), 0) AS SUM_RATELIMIT_LIMIT,
    COUNT(*) FILTER (WHERE RATELIMIT_LIMIT != -1) AS COUNT_RATELIMIT_LIMIT,
    COALESCE(SUM(RATELIMIT_USED::numeric / RATELIMIT_LIMIT) FILTER (WHERE RATELIMIT_LIMIT > 0), 0) AS SUM_RATELIMIT_RATIO,
    COUNT(*) FILTER (WHERE RATELIMIT_LIMIT > 0) AS COUNT_RATELIMIT_RATIO,
    COALESCE(MAX(RATELIMIT_USED::numeric / RATELIMIT_LIMIT) FILTER (WHERE RATELIMIT_LIMIT > 0), 0) AS MAX_RATELIMIT_RATIO,
    COUNT(*) FILTER (WHERE RATELIMIT_LIMIT > 0 AND RATELIMIT_USED >= 0.8 * RATELIMIT_LIMIT) AS NEAR_LIMIT_COUNT
FROM requests_logs
WHERE CREATED_AT >= (
    SELECT COALESCE(MAX(HOUR) + INTERVAL '1 hour', DATE_TRUNC('hour', LOCALTIMESTAMP))
    FROM stats_hourly
)
GROUP BY 1;


-- Historique fige + heure(s) en cours : source unique des v_gf_hourly_*
CREATE OR REPLACE VIEW v_gf_hourly AS
SELECT
    HOUR, REQUESTS, ERROR_COUNT, UNIQUE_VISITORS,
    SUM_RATELIMIT_USED, COUNT_RATELIMIT_USED,
    SUM_PROCESS_TIME, COUNT_PROCESS_TIME, UNDER_200MS_COUNT,
    SUM_RATELIMIT_LIMIT, COUNT_RATELIMIT_LIMIT,
    SUM_RATELIMIT_RATIO, COUNT_RATELIMIT_RATIO,
    MAX_RATELIMIT_RATIO, NEAR_LIMIT_COUNT
FROM stats_hourly
UNION ALL
SELECT
    HOUR, REQUESTS, ERROR_COUNT, UNIQUE_VISITORS,
    SUM_RATELIMIT_USED, COUNT_RATELIMIT_USED,
    SUM_PROCESS_TIME, COUNT_PROCESS_TIME, UNDER_200MS_COUNT,
    SUM_RATELIMIT_LIMIT, COUNT_RATELIMIT_LIMIT,
    SUM_RATELIMIT_RATIO, COUNT_RATELIMIT_RATIO,
    MAX_RATELIMIT_RATIO, NEAR_LIMIT_COUNT
FROM v_gf_hourly_pending;


-- Vue pour le taux d'erreurs horaire
CREATE OR REPLACE VIEW v_gf_hourly_error_rate AS
SELECT
    HOUR AS "Hour",
    100.0 * ERROR_COUNT::float / NULLIF(REQUESTS, 0) AS "Taux d'erreurs en %"
FROM v_gf_hourly;


-- Vue pour les requêtes horaires
CREATE OR REPLACE VIEW v_gf_hourly_requests AS
SELECT
    HOUR AS "Heure",
    REQUESTS AS "Requêtes"
FROM v_gf_hourly;


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
FROM v_gf_hourly;


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
FROM v_gf_hourly;


-- Vue pour les jours pas encore figes dans stats_daily : StatsAggregator
-- n'y ecrit une ligne qu'une fois le jour calendaire entierement clos
-- (voir __finalize_days), donc le jour en cours n'y apparaitrait qu'au
-- lendemain. On recalcule ces jours-la a la volee depuis requests_logs.
--
-- Seule exception a la regle "les vues LOGS ne scannent plus
-- requests_logs" : la borne basse est le lendemain du dernier jour deja
-- fige, donc ce scan ne couvre jamais que la queue non finalisee (en
-- pratique le jour en cours), servie par idx_requests_logs_created_at et
-- l'elagage de partitions. Les jours anterieurs restent lus depuis
-- stats_daily, y compris apres archivage des partitions : l'archivage ne
-- supprime une partition qu'une fois ses jours finalises (voir
-- archiver.py, __stats_are_ready), donc jamais une periode que cette vue
-- aurait encore a couvrir.
--
-- Memes filtres que __finalize_days (ratelimit_limit >= 0) pour que le
-- jour en cours reste comparable aux jours deja figes.
CREATE OR REPLACE VIEW v_gf_daily_pending AS
SELECT
    CREATED_AT::date AS DAY,
    COUNT(*) FILTER (WHERE STATUS = 200) AS STATUS_200,
    COUNT(*) FILTER (WHERE STATUS = 302) AS STATUS_302,
    COUNT(*) FILTER (WHERE STATUS = 400) AS STATUS_400,
    COUNT(*) FILTER (WHERE STATUS = 404) AS STATUS_404,
    COUNT(*) FILTER (WHERE STATUS = 405) AS STATUS_405,
    COUNT(*) FILTER (WHERE STATUS = 429) AS STATUS_429,
    COUNT(*) FILTER (WHERE STATUS = 500) AS STATUS_500,
    COUNT(*) FILTER (WHERE STATUS = 503) AS STATUS_503,
    COUNT(DISTINCT HASHED_IP) AS UNIQUE_IPS,
    COUNT(*) FILTER (WHERE STATUS BETWEEN 400 AND 499) AS ERRORS_4XX,
    COUNT(*) FILTER (WHERE STATUS BETWEEN 500 AND 599) AS ERRORS_5XX,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY PROCESS_TIME) AS P50_PROCESS_TIME,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY PROCESS_TIME) AS P95_PROCESS_TIME
FROM requests_logs
WHERE CREATED_AT >= (SELECT COALESCE(MAX(DAY) + 1, CURRENT_DATE) FROM stats_daily)
  AND RATELIMIT_LIMIT >= 0
GROUP BY 1;


-- Historique fige + jour(s) en cours : source unique des vues quotidiennes
CREATE OR REPLACE VIEW v_gf_daily AS
SELECT
    DAY, STATUS_200, STATUS_302, STATUS_400, STATUS_404,
    STATUS_405, STATUS_429, STATUS_500, STATUS_503,
    UNIQUE_IPS, ERRORS_4XX, ERRORS_5XX,
    P50_PROCESS_TIME, P95_PROCESS_TIME
FROM stats_daily
UNION ALL
SELECT
    DAY, STATUS_200, STATUS_302, STATUS_400, STATUS_404,
    STATUS_405, STATUS_429, STATUS_500, STATUS_503,
    UNIQUE_IPS, ERRORS_4XX, ERRORS_5XX,
    P50_PROCESS_TIME, P95_PROCESS_TIME
FROM v_gf_daily_pending;


-- Vue pour le status 200 quotidien
CREATE OR REPLACE VIEW v_gf_daily_status_200 AS
SELECT DAY AS "Jour", STATUS_200 AS "200" FROM v_gf_daily;


-- Vue pour le status 302 quotidien
CREATE OR REPLACE VIEW v_gf_daily_status_302 AS
SELECT DAY AS "Jour", STATUS_302 AS "302" FROM v_gf_daily;


-- Vue pour le status 400 quotidien
CREATE OR REPLACE VIEW v_gf_daily_status_400 AS
SELECT DAY AS "Jour", STATUS_400 AS "400" FROM v_gf_daily;


-- Vue pour le status 404 quotidien
CREATE OR REPLACE VIEW v_gf_daily_status_404 AS
SELECT DAY AS "Jour", STATUS_404 AS "404" FROM v_gf_daily;


-- Vue pour le status 405 quotidien
CREATE OR REPLACE VIEW v_gf_daily_status_405 AS
SELECT DAY AS "Jour", STATUS_405 AS "405" FROM v_gf_daily;


-- Vue pour le status 429 quotidien
CREATE OR REPLACE VIEW v_gf_daily_status_429 AS
SELECT DAY AS "Jour", STATUS_429 AS "429" FROM v_gf_daily;


-- Vue pour le status 500 quotidien
CREATE OR REPLACE VIEW v_gf_daily_status_500 AS
SELECT DAY AS "Jour", STATUS_500 AS "500" FROM v_gf_daily;


-- Vue pour le status 503 quotidien
CREATE OR REPLACE VIEW v_gf_daily_status_503 AS
SELECT DAY AS "Jour", STATUS_503 AS "503" FROM v_gf_daily;


-- Vue pour le temps de traitement moyen horaire
CREATE OR REPLACE VIEW v_gf_hourly_process_time AS
SELECT
    HOUR AS "Heure",
    (SUM_PROCESS_TIME::numeric / NULLIF(COUNT_PROCESS_TIME, 0)) AS "Process Time"
FROM v_gf_hourly;


-- Vue pour le pourcentage de requêtes sous 200ms horaire
CREATE OR REPLACE VIEW v_gf_hourly_under_200ms AS
SELECT
    HOUR AS "Hour",
    100.0 * UNDER_200MS_COUNT::float / NULLIF(REQUESTS, 0) AS "% < 200ms"
FROM v_gf_hourly;


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


-- ================================================
-- LOGS • Vues des panels sortis de requests_logs
-- ================================================
--
-- Ces vues remplacent des requetes Grafana qui agregeaient requests_logs
-- directement : correctes tant que les logs etaient la, mais vouees a
-- perdre leur historique au fil de l'archivage des partitions.

-- Top 15 des endpoints les plus appeles (par route normalisee, pas par
-- path brut : voir normalize_route dans schema.sql). Sans filtre sur
-- ratelimit_limit, comme la requete d'origine.
CREATE OR REPLACE VIEW v_gf_top_endpoints AS
SELECT
    ROUTE AS "Endpoint",
    TOTAL AS "Total"
FROM stats_by_route
WHERE ROUTE NOT IN ('/v1/status', '/metrics')
ORDER BY TOTAL DESC
LIMIT 15;


-- Top 15 des endpoints les plus lents (temps de traitement moyen)
CREATE OR REPLACE VIEW v_gf_slowest_endpoints AS
SELECT
    ROUTE AS "Endpoint",
    ROUND(SUM_PROCESS_TIME::numeric / NULLIF(COUNT_PROCESS_TIME, 0), 1) AS "Moy process time (ms)"
FROM stats_by_route
WHERE COUNT_PROCESS_TIME > 0
ORDER BY 2 DESC
LIMIT 15;


-- Repartition des requetes authentifiees vs anonymes
CREATE OR REPLACE VIEW v_gf_requests_by_key_presence AS
SELECT
    REQUESTS_WITH_KEY AS "Avec clé",
    REQUESTS_WITHOUT_KEY AS "Sans clé"
FROM stats_counters
WHERE ID = 1;


-- IPs uniques par jour
CREATE OR REPLACE VIEW v_gf_daily_unique_ips AS
SELECT
    DAY::timestamp AS "time",
    UNIQUE_IPS AS "IPs uniques"
FROM v_gf_daily;


-- Erreurs 4xx vs 5xx par jour (plages completes, la ou v_gf_daily_status_*
-- ne couvre que les codes pris un par un)
CREATE OR REPLACE VIEW v_gf_daily_errors AS
SELECT
    DAY::timestamp AS "time",
    ERRORS_4XX AS "4xx (client)",
    ERRORS_5XX AS "5xx (serveur)"
FROM v_gf_daily;


-- Percentiles du temps de traitement par jour
CREATE OR REPLACE VIEW v_gf_daily_process_time AS
SELECT
    DAY::timestamp AS "time",
    P50_PROCESS_TIME AS "p50 (ms)",
    P95_PROCESS_TIME AS "p95 (ms)"
FROM v_gf_daily;


-- Ratelimit limite moyen par heure (filtre != -1, celui du panel d'origine)
CREATE OR REPLACE VIEW v_gf_hourly_ratelimit_limit AS
SELECT
    HOUR AS "Heure",
    (SUM_RATELIMIT_LIMIT::numeric / NULLIF(COUNT_RATELIMIT_LIMIT, 0)) AS "Ratelimit Limite"
FROM v_gf_hourly;


-- ================================================
-- LOGS • Saturation du ratelimit
-- ================================================
--
-- ratelimit_used seul est illisible : c'est le compteur de la cle dans la
-- fenetre de 60 s en cours, une dent de scie qui remonte a chaque fenetre.
-- Rapporte a la limite de son bucket, il devient un pourcentage lisible,
-- comparable entre cles, et superieur a 100 % exactement pour les requetes
-- refusees en 429 (remaining passe en negatif avant l'exception, cote API).

-- Saturation moyenne et pic par heure
CREATE OR REPLACE VIEW v_gf_hourly_ratelimit_saturation AS
SELECT
    HOUR AS "Heure",
    100.0 * (SUM_RATELIMIT_RATIO / NULLIF(COUNT_RATELIMIT_RATIO, 0)) AS "Saturation moyenne",
    100.0 * MAX_RATELIMIT_RATIO AS "Saturation max"
FROM v_gf_hourly;


-- Pic de saturation depuis toujours
CREATE OR REPLACE VIEW v_gf_max_ratelimit_saturation AS
SELECT
    1 AS id,
    100.0 * MAX_RATELIMIT_RATIO AS "Max"
FROM stats_counters
WHERE ID = 1;


-- Requetes arrivees a 80 % ou plus de leur limite
CREATE OR REPLACE VIEW v_gf_near_limit_requests AS
SELECT
    1 AS id,
    NEAR_LIMIT_COUNT AS "Total"
FROM stats_counters
WHERE ID = 1;


-- Anciens noms (v1 du correctif "jour en cours") : remplaces par
-- v_gf_daily / v_gf_daily_pending, qui portent aussi les colonnes
-- non liees aux status codes. Supprimes ici pour ne pas laisser trainer
-- deux sources de verite ; no-op si la base ne les a jamais eus.
DROP VIEW IF EXISTS v_gf_daily_status;
DROP VIEW IF EXISTS v_gf_daily_status_pending;
