/***************************************************************
    *  CROUStillantData - schema.sql
    *  Created by: CROUStillant Développement
    *  Created on: 05/09/2026
    *  Updated on: 05/09/2026
    *  Description: Tables permanentes pour les statistiques de requests_logs.
    *
    *  requests_logs est partitionnee par mois et vouee a etre archivee
    *  (voir CROUStillantData/CROUStillantData/archiver.py) : les vues de
    *  views.sql ne doivent plus jamais scanner cette table directement pour
    *  des totaux "depuis toujours". A la place, ces tables sont maintenues de
    *  facon incrementale par StatsAggregator (stats_aggregator.py) et les
    *  vues deviennent de simples lectures de ces tables (rapides, bornees,
    *  independantes du volume de requests_logs).
    *
    *  Chaque colonne reprend EXACTEMENT le filtre WHERE de la vue
    *  materialisee qu'elle remplace dans l'ancien views.sql (certaines
    *  filtrent ratelimit_limit >= 0, d'autres non - ce n'est pas uniformise
    *  ici, l'incoherence est preservee volontairement).
***************************************************************/


-- Curseur d'avancement de StatsAggregator sur requests_logs.created_at
CREATE TABLE IF NOT EXISTS stats_watermark(
    ID INT PRIMARY KEY DEFAULT 1,
    LAST_PROCESSED_AT TIMESTAMP NOT NULL DEFAULT '1970-01-01',
    CONSTRAINT CK_STATS_WATERMARK_SINGLETON CHECK (ID = 1)
);

INSERT INTO stats_watermark (ID) VALUES (1) ON CONFLICT (ID) DO NOTHING;


-- Compteurs cumulatifs "depuis toujours" (remplace v_gf_total_requests,
-- v_gf_status_requests, v_gf_status_200/404/500/503, v_gf_requests_with_key,
-- v_gf_max_ratelimit_used, v_gf_avg_ratelimit_used, v_gf_max_ratelimit_limit,
-- v_gf_status_breakdown)
CREATE TABLE IF NOT EXISTS stats_counters(
    ID INT PRIMARY KEY DEFAULT 1,

    -- v_gf_total_requests : COUNT(*), sans filtre
    TOTAL_REQUESTS BIGINT NOT NULL DEFAULT 0,

    -- v_gf_status_requests : WHERE ratelimit_limit >= 0 AND path = '/v1/status'
    STATUS_REQUESTS BIGINT NOT NULL DEFAULT 0,

    -- v_gf_status_200/404/500/503 ET v_gf_status_breakdown (memes filtres,
    -- reutilises pour les deux) : WHERE ratelimit_limit >= 0 AND status = X
    STATUS_200 BIGINT NOT NULL DEFAULT 0,
    STATUS_404 BIGINT NOT NULL DEFAULT 0,
    STATUS_500 BIGINT NOT NULL DEFAULT 0,
    STATUS_503 BIGINT NOT NULL DEFAULT 0,

    -- v_gf_status_breakdown uniquement (memes filtres que ci-dessus)
    BREAKDOWN_302 BIGINT NOT NULL DEFAULT 0,
    BREAKDOWN_400 BIGINT NOT NULL DEFAULT 0,
    BREAKDOWN_405 BIGINT NOT NULL DEFAULT 0,
    BREAKDOWN_429 BIGINT NOT NULL DEFAULT 0,

    -- v_gf_requests_with_key : WHERE ratelimit_limit >= 0 AND key IS NOT NULL
    REQUESTS_WITH_KEY BIGINT NOT NULL DEFAULT 0,

    -- v_gf_max_ratelimit_used : MAX(ratelimit_used), sans filtre
    MAX_RATELIMIT_USED INT NOT NULL DEFAULT 0,

    -- v_gf_avg_ratelimit_used : WHERE ratelimit_used >= 0 (filtre sur la
    -- colonne elle-meme, pas sur ratelimit_limit)
    SUM_RATELIMIT_USED BIGINT NOT NULL DEFAULT 0,
    COUNT_RATELIMIT_USED BIGINT NOT NULL DEFAULT 0,

    -- v_gf_max_ratelimit_limit : MAX(ratelimit_limit) WHERE ratelimit_limit >= 0
    MAX_RATELIMIT_LIMIT INT NOT NULL DEFAULT 0
);

INSERT INTO stats_counters (ID) VALUES (1) ON CONFLICT (ID) DO NOTHING;


-- v_gf_requests_by_method : GROUP BY method WHERE ratelimit_limit >= 0
CREATE TABLE IF NOT EXISTS stats_by_method(
    METHOD VARCHAR(500) PRIMARY KEY,
    TOTAL BIGINT NOT NULL DEFAULT 0
);


-- v_gf_requests_by_api_version : GROUP BY api_version WHERE ratelimit_limit >= 0
CREATE TABLE IF NOT EXISTS stats_by_api_version(
    VERSION VARCHAR(50) PRIMARY KEY,
    TOTAL BIGINT NOT NULL DEFAULT 0
);


-- v_gf_top_params : jsonb_each_text(params), GROUP BY key, sans filtre
CREATE TABLE IF NOT EXISTS stats_by_param(
    PARAM TEXT PRIMARY KEY,
    TOTAL BIGINT NOT NULL DEFAULT 0
);


-- v_gf_unique_ips : remplace COUNT(DISTINCT hashed_ip), sans filtre.
-- COUNT(*) sur cette table == COUNT(DISTINCT hashed_ip) sur requests_logs.
CREATE TABLE IF NOT EXISTS unique_hashed_ips(
    HASHED_IP VARCHAR(40) PRIMARY KEY,
    FIRST_SEEN TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);


-- v_gf_distinct_keys : remplace COUNT(DISTINCT key), WHERE ratelimit_limit >= 0
-- AND key IS NOT NULL implicitement (on n'insere que les cles non nulles).
CREATE TABLE IF NOT EXISTS unique_keys(
    KEY VARCHAR(50) PRIMARY KEY,
    FIRST_SEEN TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);


-- v_gf_hourly_total_24h : EXTRACT(HOUR FROM created_at), WHERE ratelimit_limit >= 0.
-- Regroupe par heure de la journee (0-23) sur TOUTE l'historique, pas les
-- dernieres 24h malgre le nom (comportement de l'ancienne vue preserve).
CREATE TABLE IF NOT EXISTS stats_hour_of_day_24h(
    HOUR_OF_DAY SMALLINT PRIMARY KEY,
    TOTAL BIGINT NOT NULL DEFAULT 0
);


-- v_gf_hourly_unique_ips_24h : meme regroupement par heure de la journee,
-- mais distinct(hashed_ip) -> necessite un set dedupe par bucket, pas juste
-- un compteur additif.
CREATE TABLE IF NOT EXISTS unique_ips_by_hour_of_day(
    HOUR_OF_DAY SMALLINT NOT NULL,
    HASHED_IP VARCHAR(40) NOT NULL,
    CONSTRAINT PK_UNIQUE_IPS_BY_HOUR_OF_DAY PRIMARY KEY (HOUR_OF_DAY, HASHED_IP)
);


-- Une ligne par HEURE CALENDAIRE deja terminee (ecrite une seule fois,
-- jamais modifiee ensuite). Remplace v_gf_hourly_requests, v_gf_hourly_error_rate,
-- v_gf_hourly_unique_visitors, v_gf_hourly_ratelimit_used, v_gf_hourly_process_time,
-- v_gf_hourly_under_200ms.
CREATE TABLE IF NOT EXISTS stats_hourly(
    HOUR TIMESTAMP PRIMARY KEY,

    -- v_gf_hourly_requests ET denominateur de v_gf_hourly_under_200ms : sans filtre
    REQUESTS BIGINT NOT NULL DEFAULT 0,

    -- v_gf_hourly_error_rate : SUM(status >= 400), sans filtre
    ERROR_COUNT BIGINT NOT NULL DEFAULT 0,

    -- v_gf_hourly_unique_visitors : COUNT(DISTINCT hashed_ip), sans filtre,
    -- calcule une seule fois pour l'heure (exact, pas de table de dedupe
    -- necessaire puisqu'on ne le calcule plus jamais une fois ecrit)
    UNIQUE_VISITORS BIGINT NOT NULL DEFAULT 0,

    -- v_gf_hourly_ratelimit_used : WHERE ratelimit_limit >= 0
    SUM_RATELIMIT_USED BIGINT NOT NULL DEFAULT 0,
    COUNT_RATELIMIT_USED BIGINT NOT NULL DEFAULT 0,

    -- v_gf_hourly_process_time : WHERE ratelimit_limit >= 0
    SUM_PROCESS_TIME BIGINT NOT NULL DEFAULT 0,
    COUNT_PROCESS_TIME BIGINT NOT NULL DEFAULT 0,

    -- v_gf_hourly_under_200ms : SUM(process_time < 200), sans filtre
    UNDER_200MS_COUNT BIGINT NOT NULL DEFAULT 0
);


-- Une ligne par JOUR CALENDAIRE deja termine (ecrite une seule fois).
-- Remplace v_gf_daily_status_200/302/400/404/405/429/500/503.
CREATE TABLE IF NOT EXISTS stats_daily(
    DAY DATE PRIMARY KEY,
    STATUS_200 BIGINT NOT NULL DEFAULT 0,
    STATUS_302 BIGINT NOT NULL DEFAULT 0,
    STATUS_400 BIGINT NOT NULL DEFAULT 0,
    STATUS_404 BIGINT NOT NULL DEFAULT 0,
    STATUS_405 BIGINT NOT NULL DEFAULT 0,
    STATUS_429 BIGINT NOT NULL DEFAULT 0,
    STATUS_500 BIGINT NOT NULL DEFAULT 0,
    STATUS_503 BIGINT NOT NULL DEFAULT 0
);


-- Journal d'archivage : une ligne par partition mensuelle de requests_logs
-- deplacee vers la base distante (voir archiver.py).
CREATE TABLE IF NOT EXISTS archive_log(
    PARTITION_NAME VARCHAR(50) PRIMARY KEY,
    ARCHIVED_AT TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ROW_COUNT BIGINT NOT NULL
);
