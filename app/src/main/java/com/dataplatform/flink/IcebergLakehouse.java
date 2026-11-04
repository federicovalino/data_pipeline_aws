package com.dataplatform.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * Definición de la tabla Iceberg del Lakehouse y del catálogo Glue.
 *
 *   Catálogo: AWS Glue Data Catalog (GlueCatalog) -> base lakehouse_db (creada por Terraform)
 *   FileIO:   S3FileIO (AWS SDK v2) sobre s3://<lakehouse-bucket>/warehouse
 *   Tabla:    lakehouse_db.user_activity, Iceberg v2, particionada por day(window_start)
 */
public final class IcebergLakehouse {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergLakehouse.class);

    static final String CATALOG_NAME = "glue_catalog";
    static final String GLUE_CATALOG_IMPL = "org.apache.iceberg.aws.glue.GlueCatalog";
    static final String S3_FILE_IO_IMPL = "org.apache.iceberg.aws.s3.S3FileIO";

    /** Columnas que identifican una fila: un resultado por usuario y ventana (clave del upsert). */
    static final List<String> EQUALITY_FIELDS = Arrays.asList("user_id", "window_start");

    /** Esquema con IDs de columna explícitos: Iceberg evoluciona el esquema por ID, no por nombre. */
    static final Schema SCHEMA = new Schema(
            Arrays.asList(
                    Types.NestedField.required(1, "user_id", Types.StringType.get()),
                    Types.NestedField.required(2, "window_start", Types.TimestampType.withoutZone()),
                    Types.NestedField.required(3, "window_end", Types.TimestampType.withoutZone()),
                    Types.NestedField.required(4, "total_events", Types.LongType.get()),
                    Types.NestedField.required(5, "page_views", Types.LongType.get()),
                    Types.NestedField.required(6, "clicks", Types.LongType.get()),
                    Types.NestedField.required(7, "add_to_cart", Types.LongType.get()),
                    Types.NestedField.required(8, "checkouts", Types.LongType.get()),
                    Types.NestedField.required(9, "pending_cart_items", Types.LongType.get()),
                    Types.NestedField.required(10, "abandonment_risk", Types.BooleanType.get()),
                    Types.NestedField.required(11, "alert_triggered", Types.BooleanType.get()),
                    Types.NestedField.required(12, "processed_at", Types.TimestampType.withoutZone())),
            new HashSet<>(Arrays.asList(1, 2)));   // identifier fields: user_id + window_start

    /**
     * Hidden partitioning por día del inicio de ventana:
     *  - cardinalidad baja y estable (1 partición por día): sin metadata overhead;
     *  - las consultas filtran por window_start y Iceberg poda particiones sin que el
     *    analista conozca la columna de partición (partition pruning);
     *  - NO se particiona por user_id: cardinalidad alta = miles de particiones minúsculas.
     */
    static final PartitionSpec PARTITION_SPEC = PartitionSpec.builderFor(SCHEMA)
            .day("window_start")
            .build();

    static Map<String, String> tableProperties() {
        Map<String, String> props = new HashMap<>();
        props.put("format-version", "2");                          // v2: upserts (equality deletes)
        props.put("write.format.default", "parquet");
        props.put("write.parquet.compression-codec", "zstd");
        props.put("write.target-file-size-bytes", "134217728");    // 128 MB: objetivo de la compactación
        props.put("write.upsert.enabled", "true");
        props.put("write.distribution-mode", "hash");               // agrupa por partición antes de escribir: menos archivos
        props.put("write.metadata.delete-after-commit.enabled", "true");
        props.put("write.metadata.previous-versions-max", "50");   // acota los metadata.json viejos
        props.put("history.expire.max-snapshot-age-ms", "604800000"); // 7 días de time travel
        props.put("commit.retry.num-retries", "10");                // reintentos ante conflicto de commit
        props.put("commit.retry.min-wait-ms", "200");
        return props;
    }

    /**
     * Catálogo Glue. El commit de Iceberg en Glue usa control de concurrencia optimista:
     * UpdateTable condicionado al VersionId de la tabla en Glue (el "lock" lo provee el
     * propio servicio Glue). Si otro escritor commiteó antes, Iceberg reintenta sobre la
     * metadata nueva en lugar de pisarla.
     */
    static CatalogLoader catalogLoader(String warehouse, String region) {
        Map<String, String> props = new HashMap<>();
        props.put("warehouse", warehouse);
        props.put("io-impl", S3_FILE_IO_IMPL);
        props.put("client.region", region);
        props.put("glue.skip-archive", "true");   // no guardar una versión de tabla en Glue por cada commit
        return CatalogLoader.custom(CATALOG_NAME, props, new Configuration(), GLUE_CATALOG_IMPL);
    }

    /** Crea la tabla si no existe (la base lakehouse_db la crea Terraform). Idempotente. */
    static void ensureTable(CatalogLoader loader, TableIdentifier tableId) {
        Catalog catalog = loader.loadCatalog();
        if (catalog.tableExists(tableId)) {
            LOG.info("Tabla Iceberg {} existente: se reutiliza", tableId);
            return;
        }
        try {
            catalog.createTable(tableId, SCHEMA, PARTITION_SPEC, tableProperties());
            LOG.info("Tabla Iceberg {} creada (particionada por day(window_start))", tableId);
        } catch (AlreadyExistsException e) {
            LOG.info("Tabla Iceberg {} creada en paralelo por otra instancia: se reutiliza", tableId);
        }
    }

    /** UserActivity -> RowData con el mismo orden y tipos que SCHEMA. */
    public static class ToRowData implements MapFunction<UserActivity, RowData> {
        @Override
        public RowData map(UserActivity a) {
            GenericRowData row = new GenericRowData(12);
            row.setField(0, StringData.fromString(a.userId));
            row.setField(1, TimestampData.fromEpochMillis(a.windowStartMillis));
            row.setField(2, TimestampData.fromEpochMillis(a.windowEndMillis));
            row.setField(3, a.totalEvents);
            row.setField(4, a.pageViews);
            row.setField(5, a.clicks);
            row.setField(6, a.addToCart);
            row.setField(7, a.checkouts);
            row.setField(8, a.pendingCartItems);
            row.setField(9, a.abandonmentRisk);
            row.setField(10, a.alertTriggered);
            row.setField(11, TimestampData.fromEpochMillis(System.currentTimeMillis()));
            return row;
        }
    }

    private IcebergLakehouse() {}
}
