/***************************************************************
    *  CROUStillantData - schema.sql
    *  Created by: CROUStillant Développement
    *  Created on: 05/09/2026
    *  Updated on: 09/09/2026
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


-- Curseur d'avancement de Analytics (analytics.py) sur la colonne
-- created_at de la table "session" (base ANALYTICS_POSTGRES_*, schema
-- Umami). Cette table session est rechargee integralement a chaque
-- execution (pas de fenetre SQL cote analytics_pool), mais le watermark
-- borne en Python les lignes deja comptees dans GEO_USAGE.TOTAL : sans lui,
-- chaque execution recompterait tout l'historique des sessions.
CREATE TABLE IF NOT EXISTS geo_usage_watermark(
    ID INT PRIMARY KEY DEFAULT 1,
    LAST_PROCESSED_AT TIMESTAMP NOT NULL DEFAULT '1970-01-01',
    CONSTRAINT CK_GEO_USAGE_WATERMARK_SINGLETON CHECK (ID = 1)
);

INSERT INTO geo_usage_watermark (ID) VALUES (1) ON CONFLICT (ID) DO NOTHING;


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

    -- v_gf_requests_by_key_presence : WHERE ratelimit_limit >= 0 AND key IS NULL
    -- (le pendant de REQUESTS_WITH_KEY ; ne pas deduire de TOTAL_REQUESTS,
    -- qui lui compte AUSSI les requetes internes ratelimit_limit < 0)
    REQUESTS_WITHOUT_KEY BIGINT NOT NULL DEFAULT 0,

    -- v_gf_max_ratelimit_used : MAX(ratelimit_used), sans filtre
    MAX_RATELIMIT_USED INT NOT NULL DEFAULT 0,

    -- v_gf_avg_ratelimit_used : WHERE ratelimit_used >= 0 (filtre sur la
    -- colonne elle-meme, pas sur ratelimit_limit)
    SUM_RATELIMIT_USED BIGINT NOT NULL DEFAULT 0,
    COUNT_RATELIMIT_USED BIGINT NOT NULL DEFAULT 0,

    -- v_gf_max_ratelimit_limit : MAX(ratelimit_limit) WHERE ratelimit_limit >= 0
    MAX_RATELIMIT_LIMIT INT NOT NULL DEFAULT 0,

    -- SATURATION DU RATELIMIT (ratelimit_used / ratelimit_limit).
    -- ratelimit_used est le compteur de la cle dans la fenetre en cours
    -- (limit - remaining, voir components/ratelimit.py cote API) : brut il
    -- ne veut rien dire, rapporte a la limite il donne un pourcentage
    -- directement lisible, comparable entre cles de buckets differents, et
    -- qui depasse 100 % exactement pour les requetes refusees en 429.
    -- Filtre commun : WHERE ratelimit_limit > 0 (exclut les requetes
    -- internes a -1 et protege de la division par zero).

    -- v_gf_max_ratelimit_saturation : MAX(ratelimit_used / ratelimit_limit)
    MAX_RATELIMIT_RATIO NUMERIC NOT NULL DEFAULT 0,

    -- v_gf_near_limit_requests : COUNT(*) WHERE ratelimit_used >= 0.8 * ratelimit_limit
    NEAR_LIMIT_COUNT BIGINT NOT NULL DEFAULT 0
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


-- v_gf_top_endpoints et v_gf_slowest_endpoints : agregation par ROUTE
-- normalisee (normalize_route ci-dessous), pas par path brut. Le path
-- loggue est concret (/v1/restaurants/1234/menu/2026-09-09) : une table
-- permanente indexee dessus grossirait sans limite, alors que la route
-- normalisee borne la cardinalite a quelques dizaines de lignes.
CREATE TABLE IF NOT EXISTS stats_by_route(
    ROUTE TEXT PRIMARY KEY,

    -- v_gf_top_endpoints : COUNT(*), sans filtre (l'exclusion de
    -- /v1/status et /metrics se fait dans la vue, pas ici)
    TOTAL BIGINT NOT NULL DEFAULT 0,

    -- v_gf_slowest_endpoints : AVG(process_time) WHERE ratelimit_limit >= 0
    SUM_PROCESS_TIME BIGINT NOT NULL DEFAULT 0,
    COUNT_PROCESS_TIME BIGINT NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_stats_by_route_total ON stats_by_route (TOTAL DESC);


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
    UNDER_200MS_COUNT BIGINT NOT NULL DEFAULT 0,

    -- v_gf_hourly_ratelimit_limit : AVG(ratelimit_limit) WHERE ratelimit_limit != -1
    -- (filtre != -1 et non >= 0 : c'est celui du panel d'origine, conserve tel quel).
    -- NULLables : les heures figees avant l'ajout de ces colonnes restent
    -- inconnues (NULL = trou dans le graphe, plutot qu'un faux zero).
    SUM_RATELIMIT_LIMIT BIGINT,
    COUNT_RATELIMIT_LIMIT BIGINT,

    -- v_gf_hourly_ratelimit_saturation : moyenne et pic de
    -- ratelimit_used / ratelimit_limit sur l'heure, et nombre de requetes
    -- arrivees a 80 % ou plus de leur limite. NULLables comme ci-dessus.
    -- Ne pas deduire la moyenne de SUM_RATELIMIT_USED / SUM_RATELIMIT_LIMIT :
    -- la moyenne des rapports n'est la moyenne rapportee que si toutes les
    -- cles partagent le meme bucket.
    SUM_RATELIMIT_RATIO NUMERIC,
    COUNT_RATELIMIT_RATIO BIGINT,
    MAX_RATELIMIT_RATIO NUMERIC,
    NEAR_LIMIT_COUNT BIGINT
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
    STATUS_503 BIGINT NOT NULL DEFAULT 0,

    -- Colonnes ajoutees pour les panels qui scannaient encore requests_logs.
    -- Toutes NULLables : les jours figes avant leur ajout restent inconnus
    -- (NULL = trou dans le graphe, plutot qu'un faux zero).

    -- v_gf_daily_unique_ips : COUNT(DISTINCT hashed_ip) WHERE ratelimit_limit >= 0
    UNIQUE_IPS BIGINT,

    -- v_gf_daily_errors : WHERE ratelimit_limit >= 0 AND status BETWEEN 4xx / 5xx
    -- (plages completes, la ou STATUS_4xx ci-dessus ne couvre que 400/404/405/429)
    ERRORS_4XX BIGINT,
    ERRORS_5XX BIGINT,

    -- v_gf_daily_process_time : PERCENTILE_CONT(0.50 / 0.95) WHERE ratelimit_limit >= 0.
    -- Non additifs : calcules exactement une seule fois, a la cloture du jour.
    P50_PROCESS_TIME NUMERIC,
    P95_PROCESS_TIME NUMERIC
);


-- Journal d'archivage : une ligne par partition mensuelle de requests_logs
-- deplacee vers la base distante (voir archiver.py).
CREATE TABLE IF NOT EXISTS archive_log(
    PARTITION_NAME VARCHAR(50) PRIMARY KEY,
    ARCHIVED_AT TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ROW_COUNT BIGINT NOT NULL
);


-- Normalisation des routes : le path loggue est concret
-- (/v1/restaurants/1234/menu/2026-09-09), on remplace les segments variables
-- par des placeholders pour obtenir la route de l'API
-- (/v1/restaurants/<code>/menu/<date>). IMMUTABLE : utilisable dans un index
-- fonctionnel si le besoin s'en fait sentir, et evaluee une fois par ligne
-- lors de l'agregation.
CREATE OR REPLACE FUNCTION normalize_route(P TEXT) RETURNS TEXT AS $$
    SELECT regexp_replace(
               regexp_replace(
                   regexp_replace(
                       COALESCE(P, ''),
                       '/[0-9]{4}-[0-9]{2}-[0-9]{2}(?=/|$)', '/<date>', 'g'
                   ),
                   '/[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}(?=/|$)', '/<uuid>', 'g'
               ),
               '/[0-9]+(?=/|$)', '/<code>', 'g'
           );
$$ LANGUAGE SQL IMMUTABLE;


-- ================================================
-- MIGRATIONS • Colonnes ajoutees apres coup
-- ================================================
--
-- Les CREATE TABLE ci-dessus sont en IF NOT EXISTS : sur une base deja en
-- place ils ne font rien, donc les colonnes ajoutees apres coup doivent
-- l'etre explicitement ici. Idempotent (ADD COLUMN IF NOT EXISTS), donc
-- schema.sql reste rejouable tel quel.
--
-- Ces colonnes ne sont que des CONTENANTS : pour remplir l'historique deja
-- fige, lancer ensuite migrate_stats.sql.

ALTER TABLE stats_counters ADD COLUMN IF NOT EXISTS REQUESTS_WITHOUT_KEY BIGINT NOT NULL DEFAULT 0;
ALTER TABLE stats_counters ADD COLUMN IF NOT EXISTS MAX_RATELIMIT_RATIO NUMERIC NOT NULL DEFAULT 0;
ALTER TABLE stats_counters ADD COLUMN IF NOT EXISTS NEAR_LIMIT_COUNT BIGINT NOT NULL DEFAULT 0;

ALTER TABLE stats_hourly ADD COLUMN IF NOT EXISTS SUM_RATELIMIT_LIMIT BIGINT;
ALTER TABLE stats_hourly ADD COLUMN IF NOT EXISTS COUNT_RATELIMIT_LIMIT BIGINT;
ALTER TABLE stats_hourly ADD COLUMN IF NOT EXISTS SUM_RATELIMIT_RATIO NUMERIC;
ALTER TABLE stats_hourly ADD COLUMN IF NOT EXISTS COUNT_RATELIMIT_RATIO BIGINT;
ALTER TABLE stats_hourly ADD COLUMN IF NOT EXISTS MAX_RATELIMIT_RATIO NUMERIC;
ALTER TABLE stats_hourly ADD COLUMN IF NOT EXISTS NEAR_LIMIT_COUNT BIGINT;

ALTER TABLE stats_daily ADD COLUMN IF NOT EXISTS UNIQUE_IPS BIGINT;
ALTER TABLE stats_daily ADD COLUMN IF NOT EXISTS ERRORS_4XX BIGINT;
ALTER TABLE stats_daily ADD COLUMN IF NOT EXISTS ERRORS_5XX BIGINT;
ALTER TABLE stats_daily ADD COLUMN IF NOT EXISTS P50_PROCESS_TIME NUMERIC;
ALTER TABLE stats_daily ADD COLUMN IF NOT EXISTS P95_PROCESS_TIME NUMERIC;
