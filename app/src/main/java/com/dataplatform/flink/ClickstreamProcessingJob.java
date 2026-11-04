package com.dataplatform.flink;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.IcebergSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;

/**
 * Pre-entrega 4 · Procesamiento en tiempo real del clickstream.
 *
 *   Kinesis (dataplatform-dev-stream, 2 shards)
 *     -> parseo y validación del JSON
 *     -> Event Time (event_ts) + watermark con 10 s de tolerancia e idleness de 1 min
 *     -> keyBy(user_id) -> ventana Tumbling de 1 minuto (Event Time)
 *          · AggregateFunction: contadores por tipo de evento (estado mínimo por ventana)
 *          · ProcessWindowFunction: Keyed State entre ventanas (productos en carrito sin checkout)
 *     -> USER_ACTIVITY y CART_ABANDONMENT_ALERT a CloudWatch Logs
 *     -> eventos tardíos (después de allowedLateness) a un Side Output: LATE_EVENT
 *     -> Lakehouse (pre-entrega 5): IcebergSink -> tabla lakehouse_db.user_activity
 *        registrada en el AWS Glue Data Catalog (upsert por user_id + window_start)
 *
 * Parámetros: grupo "FlinkApplicationProperties" definido en Terraform (modules/flink).
 * Checkpointing: en MSF lo configura Terraform (checkpoint_configuration); los checkpoints
 * se guardan en el S3 administrado por el servicio.
 */
public class ClickstreamProcessingJob {

    private static final Logger LOG = LoggerFactory.getLogger(ClickstreamProcessingJob.class);
    private static final String PROPERTY_GROUP = "FlinkApplicationProperties";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final Properties props = loadApplicationProperties(env);

        final String streamName      = props.getProperty("stream.name", "dataplatform-dev-stream");
        final String region          = props.getProperty("aws.region", "us-east-1");
        final int windowMinutes      = Integer.parseInt(props.getProperty("window.minutes", "1"));
        final int outOfOrderSeconds  = Integer.parseInt(props.getProperty("watermark.out.of.orderness.seconds", "10"));
        final int idlenessSeconds    = Integer.parseInt(props.getProperty("watermark.idleness.seconds", "60"));
        final int allowedLatenessSec = Integer.parseInt(props.getProperty("allowed.lateness.seconds", "30"));
        final String getRecordsIntervalMs = props.getProperty("kinesis.getrecords.interval.ms", "1000");
        // Lakehouse: en MSF vienen de Terraform; en el clúster local, de variables de entorno
        final String icebergWarehouse = props.getProperty("iceberg.warehouse", System.getenv("ICEBERG_WAREHOUSE"));
        final String icebergDatabase  = props.getProperty("iceberg.database",
                System.getenv().getOrDefault("ICEBERG_DATABASE", "lakehouse_db"));
        final String icebergTable     = props.getProperty("iceberg.table",
                System.getenv().getOrDefault("ICEBERG_TABLE", "user_activity"));
        final boolean lakehouseEnabled = icebergWarehouse != null && !icebergWarehouse.isBlank();

        LOG.info("Configuración: stream={} region={} ventana={}min tolerancia={}s idleness={}s allowedLateness={}s",
                streamName, region, windowMinutes, outOfOrderSeconds, idlenessSeconds, allowedLatenessSec);

        // Solo en ejecución local: en MSF el checkpointing lo define la infraestructura.
        if (env instanceof LocalStreamEnvironment) {
            env.enableCheckpointing(60_000, CheckpointingMode.EXACTLY_ONCE);
        }

        // ===== 1. Source: Kinesis Data Stream de la pre-entrega 2 =====
        // Los offsets (sequence numbers) de cada shard son OPERATOR STATE del source:
        // se guardan en cada checkpoint y permiten retomar exactamente donde quedó.
        Properties consumerConfig = new Properties();
        consumerConfig.setProperty(ConsumerConfigConstants.AWS_REGION, region);
        consumerConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");
        // El stream tiene otro consumidor (Firehose, pre-entrega 2) y Kinesis admite 5 GetRecords/s
        // y 2 MB/s de lectura por shard, COMPARTIDOS entre consumidores en modo polling.
        // Default del conector: 200 ms (5 llamadas/s) -> agotaría el cupo y provocaría throttling
        // (ReadProvisionedThroughputExceeded). Con 1000 ms: Flink 1/s + Firehose 1/s = 2 de 5.
        consumerConfig.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_INTERVAL_MILLIS, getRecordsIntervalMs);

        DataStream<String> rawEvents = env
                .addSource(new FlinkKinesisConsumer<>(streamName, new SimpleStringSchema(), consumerConfig))
                .name("kinesis-source")
                .uid("kinesis-source");

        // ===== 2. Parseo + Event Time + Watermarks =====
        // event_ts lo pone el producer al generar el evento: determinista ante lag o reprocesos.
        // 10 s de tolerancia: red móvil, reintentos y lectura en paralelo de 2 shards.
        // Idleness: un shard sin tráfico no congela el watermark de toda la aplicación.
        WatermarkStrategy<ClickEvent> watermarkStrategy = WatermarkStrategy
                .<ClickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(outOfOrderSeconds))
                .withTimestampAssigner((event, recordTimestamp) -> event.eventTimeMillis)
                .withIdleness(Duration.ofSeconds(idlenessSeconds));

        DataStream<ClickEvent> events = rawEvents
                .flatMap(new ParseAndValidate())
                .name("parse-validate")
                .uid("parse-validate")
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .name("event-time-watermarks")
                .uid("event-time-watermarks");

        // ===== 3. Ventana Tumbling de 1 minuto por usuario + Keyed State =====
        final OutputTag<ClickEvent> lateEvents = new OutputTag<ClickEvent>("late-events") {};

        SingleOutputStreamOperator<UserActivity> userActivity = events
                .keyBy(event -> event.userId)
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(windowMinutes)))
                .allowedLateness(Duration.ofSeconds(allowedLatenessSec))   // tardíos moderados: re-emite la ventana corregida
                .sideOutputLateData(lateEvents)                      // muy tardíos: no se pierden en silencio
                .aggregate(new EventCounts.Aggregator(), new CartAbandonmentFunction())
                .name("user-activity-1min-window")
                .uid("user-activity-1min-window");

        // ===== 4. Sinks: salida observable en CloudWatch Logs =====
        userActivity
                .addSink(new LoggingSink<>("USER_ACTIVITY", false))
                .name("log-user-activity")
                .uid("log-user-activity");

        userActivity
                .filter(activity -> activity.alertTriggered)
                .name("filter-abandonment-risk")
                .uid("filter-abandonment-risk")
                .addSink(new LoggingSink<>("CART_ABANDONMENT_ALERT", true))
                .name("log-abandonment-alerts")
                .uid("log-abandonment-alerts");

        userActivity.getSideOutput(lateEvents)
                .addSink(new LoggingSink<>("LATE_EVENT", true))
                .name("log-late-events")
                .uid("log-late-events");

        // ===== 5. Lakehouse: tabla Iceberg en el Glue Data Catalog =====
        // El IcebergSink escribe archivos Parquet continuamente, pero COMMITEA (nuevo snapshot +
        // puntero atómico en Glue) solo cuando Flink completa un checkpoint: sin checkpointing
        // habría archivos en S3 y la tabla en el catálogo seguiría vacía.
        // Upsert por (user_id, window_start): cuando allowedLateness re-emite una ventana
        // corregida, la fila nueva REEMPLAZA a la anterior en lugar de duplicarla.
        if (lakehouseEnabled) {
            TableIdentifier tableId = TableIdentifier.of(icebergDatabase, icebergTable);
            CatalogLoader catalogLoader = IcebergLakehouse.catalogLoader(icebergWarehouse, region);
            IcebergLakehouse.ensureTable(catalogLoader, tableId);

            DataStream<RowData> rows = userActivity
                    .map(new IcebergLakehouse.ToRowData())
                    .returns(InternalTypeInfo.of(FlinkSchemaUtil.convert(IcebergLakehouse.SCHEMA)))
                    .name("to-iceberg-rowdata")
                    .uid("to-iceberg-rowdata");

            IcebergSink.forRowData(rows)
                    .tableLoader(TableLoader.fromCatalog(catalogLoader, tableId))
                    .equalityFieldColumns(IcebergLakehouse.EQUALITY_FIELDS)
                    .upsert(true)
                    .append()
                    .name("iceberg-sink-user-activity")
                    .uid("iceberg-sink-user-activity");

            LOG.info("Lakehouse: {} en {} (catálogo Glue)", tableId, icebergWarehouse);
        } else {
            LOG.warn("Sin iceberg.warehouse / ICEBERG_WAREHOUSE: el job corre sin escribir en el Lakehouse");
        }

        env.execute("clickstream-user-activity");
    }

    /** Descarta registros mal formados en lugar de hacer fallar el job (y entrar en loop de reinicios). */
    public static class ParseAndValidate implements FlatMapFunction<String, ClickEvent> {
        private static final Logger PARSE_LOG = LoggerFactory.getLogger(ParseAndValidate.class);

        @Override
        public void flatMap(String json, Collector<ClickEvent> out) {
            ClickEvent event = ClickEvent.fromJson(json.trim());
            if (event != null) {
                out.collect(event);
            } else {
                PARSE_LOG.warn("INVALID_EVENT descartado: {}", json);
            }
        }
    }

    /** Propiedades del grupo FlinkApplicationProperties (MSF) o defaults si corre en local. */
    private static Properties loadApplicationProperties(StreamExecutionEnvironment env) {
        if (env instanceof LocalStreamEnvironment) {
            return new Properties();
        }
        try {
            Map<String, Properties> groups = KinesisAnalyticsRuntime.getApplicationProperties();
            Properties props = groups.get(PROPERTY_GROUP);
            return props != null ? props : new Properties();
        } catch (Exception e) {
            LOG.warn("No se pudieron leer las propiedades de la aplicación, se usan defaults", e);
            return new Properties();
        }
    }
}
