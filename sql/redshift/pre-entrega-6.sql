-- ============================================================================
-- Pre-entrega 6: analítica avanzada in-stream con Amazon Redshift
-- Proyecto: data_pipeline_aws  ·  Autor: Federico Valiño
--
-- Kinesis (dataplatform-dev-stream) ──RSI──► stream_staging.mv_clickstream_events
--                                              └──► analytics.mv_*  (agregaciones)
-- Glue Data Catalog (lakehouse_db, Iceberg) ──► lakehouse.user_activity
-- analytics.v_hot_users_with_history  =  stream (caliente) JOIN Iceberg (histórico)
--
-- Capas / esquemas:
--   kds_ingest      externo, mapea Kinesis   → solo el administrador (ingesta)
--   stream_staging  MV de streaming (JSON)   → solo el administrador (datos crudos)
--   lakehouse       externo, Iceberg en Glue → lectura para analítica
--   analytics       MVs y vistas de consumo  → rol analytics_reader
--
-- Ejecutar en Query Editor v2, conectado al workgroup dataplatform-dev-analytics,
-- base "analytics", con el usuario administrador (secreto de Secrets Manager).
-- IAM_ROLE default = rol del namespace creado por Terraform
-- (dataplatform-dev-redshift-streaming-role): autenticación IAM entre servicios,
-- sin claves ni contraseñas en el script.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- 1. DEFINICIÓN DE LA INGESTA (Redshift Streaming Ingestion)
-- ----------------------------------------------------------------------------

-- 1.1 Esquema externo que mapea Kinesis Data Streams como objeto de Redshift.
--     Cada stream de la cuenta/región aparece como una "tabla" de este esquema.
CREATE EXTERNAL SCHEMA IF NOT EXISTS kds_ingest
FROM KINESIS
IAM_ROLE default;

-- 1.2 Esquema donde "aterrizan" los datos del stream
CREATE SCHEMA IF NOT EXISTS stream_staging;

-- 1.3 Vista materializada de streaming: consume el stream directamente (sin S3).
--     - Lógica mínima a propósito: un error de casteo dentro de una MV de
--       streaming bloquea la ingesta, por eso acá solo se extrae texto.
--     - CAN_JSON_PARSE descarta registros que no son JSON (protección ante
--       schema drift); json_extract_path_text(..., true) devuelve NULL en vez
--       de fallar si falta un campo.
--     - El nombre del stream lleva comillas por los guiones.
CREATE MATERIALIZED VIEW stream_staging.mv_clickstream_events
AUTO REFRESH YES
AS
SELECT
    -- metadatos de Kinesis
    approximate_arrival_timestamp,                     -- llegada al stream (UTC)
    refresh_time,                                      -- ingesta en Redshift (UTC)
    partition_key,                                     -- = user_id (productor)
    shard_id,
    sequence_number,
    -- campos del JSON del evento
    json_extract_path_text(from_varbyte(kinesis_data, 'utf-8'), 'event_id',   true)::VARCHAR(64)  AS event_id,
    json_extract_path_text(from_varbyte(kinesis_data, 'utf-8'), 'user_id',    true)::VARCHAR(64)  AS user_id,
    json_extract_path_text(from_varbyte(kinesis_data, 'utf-8'), 'event_type', true)::VARCHAR(32)  AS event_type,
    json_extract_path_text(from_varbyte(kinesis_data, 'utf-8'), 'page',       true)::VARCHAR(256) AS page,
    json_extract_path_text(from_varbyte(kinesis_data, 'utf-8'), 'event_ts',   true)::VARCHAR(40)  AS event_ts_raw
FROM kds_ingest."dataplatform-dev-stream"
WHERE CAN_JSON_PARSE(kinesis_data);

-- 1.4 Primer refresco manual (los siguientes son automáticos)
REFRESH MATERIALIZED VIEW stream_staging.mv_clickstream_events;

-- 1.5 Validación: los datos fluyen y los tipos son nativos de Redshift
SELECT event_id, user_id, event_type, page, event_ts_raw,
       approximate_arrival_timestamp, refresh_time
FROM stream_staging.mv_clickstream_events
ORDER BY approximate_arrival_timestamp DESC
LIMIT 20;


-- ----------------------------------------------------------------------------
-- 2. CAPA DE AGREGACIÓN DE TIEMPO REAL (esquema de consumo)
-- ----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS analytics;

-- 2.1 Eventos por minuto y tipo (métrica operativa del dashboard)
CREATE MATERIALIZED VIEW analytics.mv_events_per_minute
AS
SELECT
    DATE_TRUNC('minute', approximate_arrival_timestamp) AS arrival_minute,
    event_type,
    COUNT(*)                                            AS events
FROM stream_staging.mv_clickstream_events
GROUP BY 1, 2;

-- 2.2 Actividad por usuario y ventana de 1 minuto (event time), con el mismo
--     grano que la tabla Iceberg lakehouse_db.user_activity escrita por Flink.
--     event_ts llega en ISO-8601 (2026-11-11T17:27:05.123+00:00): se toman los
--     primeros 19 caracteres, ya en UTC.
CREATE MATERIALIZED VIEW analytics.mv_user_activity_rt
AS
SELECT
    user_id,
    DATE_TRUNC('minute', CAST(REPLACE(LEFT(event_ts_raw, 19), 'T', ' ') AS TIMESTAMP)) AS window_start,
    COUNT(*)                                                    AS total_events,
    SUM(CASE WHEN event_type = 'page_view'   THEN 1 ELSE 0 END) AS page_views,
    SUM(CASE WHEN event_type = 'click'       THEN 1 ELSE 0 END) AS clicks,
    SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_cart,
    SUM(CASE WHEN event_type = 'checkout'    THEN 1 ELSE 0 END) AS checkouts
FROM stream_staging.mv_clickstream_events
WHERE event_ts_raw <> ''          -- descarta NULL y vacíos (campo ausente)
  AND user_id <> ''
GROUP BY 1, 2;

-- Estrategia de refresco (incremental, en dos niveles):
--   * Nivel 1 - MV de streaming (stream_staging.mv_clickstream_events):
--     AUTO REFRESH YES. Cada refresco es INCREMENTAL: lee de cada shard solo
--     desde el último sequence number consumido (Redshift guarda ese puntero),
--     nunca relee el stream completo. Latencia típica: segundos.
--   * Nivel 2 - MVs de agregación (analytics.*): refresco programado cada 60 s
--     (Query Editor v2 → Schedule, o EventBridge + Redshift Data API) con
--     REFRESH MATERIALIZED VIEW. Son agregaciones (COUNT/SUM con GROUP BY):
--     Redshift las mantiene en forma incremental, procesando solo las filas
--     nuevas de la MV de streaming.
--   * Trade-off de latencia: 30-60 s alcanza para un dashboard operativo;
--     refrescar cada 1 s saturaría el cómputo del workgroup sin beneficio.
REFRESH MATERIALIZED VIEW analytics.mv_events_per_minute;
REFRESH MATERIALIZED VIEW analytics.mv_user_activity_rt;

SELECT * FROM analytics.mv_events_per_minute
ORDER BY arrival_minute DESC, event_type
LIMIT 20;


-- ----------------------------------------------------------------------------
-- 3. INTEGRACIÓN LAKEHOUSE (Iceberg vía Glue Data Catalog)
-- ----------------------------------------------------------------------------

-- 3.1 Esquema externo sobre la base de Glue de la pre-entrega 5.
--     Redshift lee el puntero de la tabla en Glue y sigue la metadata Iceberg en S3.
CREATE EXTERNAL SCHEMA IF NOT EXISTS lakehouse
FROM DATA CATALOG
DATABASE 'lakehouse_db'
REGION 'us-east-1'
IAM_ROLE default;

-- 3.2 Histórico en Iceberg (escrito por Flink)
SELECT user_id, window_start, total_events, add_to_cart, checkouts,
       pending_cart_items, abandonment_risk, alert_triggered
FROM lakehouse.user_activity
ORDER BY window_start DESC, pending_cart_items DESC
LIMIT 20;

-- 3.3 JOIN caliente + histórico en la misma sesión:
--     usuarios activos en los últimos 15 minutos (stream) cruzados con su
--     historial de riesgo de abandono (Iceberg).
--     Late-binding view (WITH NO SCHEMA BINDING): obligatorio para referenciar
--     tablas externas desde una vista.
CREATE OR REPLACE VIEW analytics.v_hot_users_with_history AS
WITH hot AS (                                   -- datos calientes (MV de streaming)
    SELECT user_id,
           SUM(total_events)             AS events_last_15m,
           SUM(add_to_cart)              AS add_to_cart_last_15m,
           SUM(checkouts)                AS checkouts_last_15m,
           MAX(window_start)             AS last_active_minute
    FROM analytics.mv_user_activity_rt
    WHERE window_start >= DATEADD(minute, -15, GETDATE())
    GROUP BY user_id
),
history AS (                                    -- histórico (tabla Iceberg)
    SELECT user_id,
           COUNT(*)                                            AS hist_windows,
           SUM(total_events)                                   AS hist_events,
           SUM(CASE WHEN alert_triggered THEN 1 ELSE 0 END)    AS hist_abandonment_alerts,
           MAX(pending_cart_items)                             AS hist_max_pending_items
    FROM lakehouse.user_activity
    GROUP BY user_id
)
SELECT h.user_id,
       h.events_last_15m,
       h.add_to_cart_last_15m,
       h.checkouts_last_15m,
       h.last_active_minute,
       COALESCE(i.hist_windows, 0)            AS hist_windows,
       COALESCE(i.hist_events, 0)             AS hist_events,
       COALESCE(i.hist_abandonment_alerts, 0) AS hist_abandonment_alerts,
       COALESCE(i.hist_max_pending_items, 0)  AS hist_max_pending_items,
       -- regla de negocio: activo ahora, carrito abierto y antecedentes de abandono
       (h.add_to_cart_last_15m > h.checkouts_last_15m
        AND COALESCE(i.hist_abandonment_alerts, 0) > 0)  AS retarget_now
FROM hot h
LEFT JOIN history i ON i.user_id = h.user_id
WITH NO SCHEMA BINDING;

SELECT * FROM analytics.v_hot_users_with_history
ORDER BY retarget_now DESC, hist_abandonment_alerts DESC, events_last_15m DESC;

-- 3.4 Reconciliación por ventana: lo que contó Redshift desde el stream vs. lo que
--     escribió Flink en Iceberg, para el mismo usuario y minuto.
SELECT r.user_id,
       r.window_start,
       r.total_events               AS events_redshift_rsi,
       i.total_events               AS events_flink_iceberg,
       r.total_events - i.total_events AS diff
FROM analytics.mv_user_activity_rt r
JOIN lakehouse.user_activity       i
  ON i.user_id = r.user_id
 AND i.window_start = r.window_start
ORDER BY r.window_start DESC, r.user_id
LIMIT 20;


-- ----------------------------------------------------------------------------
-- 4. SEGURIDAD: acceso restringido a un rol de analítica
-- ----------------------------------------------------------------------------

-- 4.1 Nadie más que el administrador ve la ingesta ni los datos crudos
REVOKE ALL ON SCHEMA stream_staging FROM PUBLIC;
REVOKE ALL ON SCHEMA kds_ingest     FROM PUBLIC;
REVOKE CREATE ON SCHEMA public      FROM PUBLIC;

-- 4.2 Rol de analítica (RBAC): solo esquema de consumo + lectura del Lakehouse
CREATE ROLE analytics_reader;
GRANT USAGE  ON SCHEMA analytics TO ROLE analytics_reader;
GRANT SELECT ON analytics.mv_events_per_minute     TO ROLE analytics_reader;
GRANT SELECT ON analytics.mv_user_activity_rt      TO ROLE analytics_reader;
GRANT SELECT ON analytics.v_hot_users_with_history TO ROLE analytics_reader;
GRANT USAGE  ON SCHEMA lakehouse TO ROLE analytics_reader;   -- lectura de Iceberg
-- Sin CREATE en ningún esquema y sin ASSUMEROLE: el rol no puede crear esquemas
-- externos ni usar el rol IAM del namespace para leer otras fuentes.

-- 4.3 Usuario analista de prueba, sin contraseña (en producción: IAM Identity Center)
CREATE USER analista_demo PASSWORD DISABLE;
GRANT ROLE analytics_reader TO analista_demo;

-- 4.4 Prueba de permisos
SET SESSION AUTHORIZATION 'analista_demo';
SELECT current_user;
SELECT COUNT(*) FROM analytics.v_hot_users_with_history;          -- OK
SELECT COUNT(*) FROM stream_staging.mv_clickstream_events;        -- ERROR: permission denied
RESET SESSION AUTHORIZATION;


-- ----------------------------------------------------------------------------
-- 5. MONITOREO DE LA INGESTA
-- ----------------------------------------------------------------------------

-- 5.1 Lag de ingesta por minuto: llegada al stream → disponible en Redshift
SELECT DATE_TRUNC('minute', approximate_arrival_timestamp)              AS arrival_minute,
       COUNT(*)                                                         AS records,
       AVG(DATEDIFF(second, approximate_arrival_timestamp, refresh_time)) AS avg_lag_s,
       MAX(DATEDIFF(second, approximate_arrival_timestamp, refresh_time)) AS max_lag_s
FROM stream_staging.mv_clickstream_events
GROUP BY 1
ORDER BY 1 DESC
LIMIT 15;

-- 5.2 Frescura actual de la MV
SELECT MAX(approximate_arrival_timestamp)                            AS last_record_in_stream,
       MAX(refresh_time)                                            AS last_refresh,
       DATEDIFF(second, MAX(approximate_arrival_timestamp), GETDATE()) AS seconds_since_last_record
FROM stream_staging.mv_clickstream_events;

-- 5.3 Historial de refrescos y errores de la ingesta (vistas de sistema)
SELECT mv_name, status, refresh_type, start_time, end_time
FROM sys_mv_refresh_history
WHERE schema_name IN ('stream_staging', 'analytics')
ORDER BY start_time DESC
LIMIT 20;

SELECT * FROM sys_stream_scan_states LIMIT 20;   -- estado por shard
SELECT * FROM sys_stream_scan_errors LIMIT 20;   -- registros descartados
