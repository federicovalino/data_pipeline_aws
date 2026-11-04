package com.dataplatform.flink;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sink de salida observable: escribe cada registro en el log de la aplicación.
 * En Managed Service for Apache Flink esos logs van a CloudWatch Logs
 * (monitoring_configuration, log_level INFO), donde se consultan con Logs Insights.
 */
public class LoggingSink<T> implements SinkFunction<T> {

    private static final Logger LOG = LoggerFactory.getLogger(LoggingSink.class);

    private final String prefix;
    private final boolean warn;

    public LoggingSink(String prefix, boolean warn) {
        this.prefix = prefix;
        this.warn = warn;
    }

    @Override
    public void invoke(T value, Context context) {
        if (warn) {
            LOG.warn("{} {}", prefix, value);
        } else {
            LOG.info("{} {}", prefix, value);
        }
    }
}
