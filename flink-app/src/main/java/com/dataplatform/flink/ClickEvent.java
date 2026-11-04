package com.dataplatform.flink;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.OffsetDateTime;

/**
 * Evento de clickstream publicado por scripts/producer.py en el Kinesis Data Stream:
 * {"event_id", "user_id", "event_type", "page", "event_ts"}.
 *
 * POJO de Flink (constructor vacío + campos públicos): Flink lo serializa de forma
 * eficiente en el estado y en el shuffle del keyBy.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ClickEvent {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @JsonProperty("event_id")   public String eventId;
    @JsonProperty("user_id")    public String userId;
    @JsonProperty("event_type") public String eventType;
    @JsonProperty("page")       public String page;
    @JsonProperty("event_ts")   public String eventTs;

    /** Event Time en epoch millis, calculado una vez al parsear (lo usa la WatermarkStrategy). */
    public long eventTimeMillis;

    public ClickEvent() {}

    /** Parsea y valida el JSON. Devuelve null si el registro es inválido (se descarta aguas arriba). */
    public static ClickEvent fromJson(String json) {
        try {
            ClickEvent event = MAPPER.readValue(json, ClickEvent.class);
            if (event.userId == null || event.eventType == null || event.eventTs == null) {
                return null;
            }
            event.eventTimeMillis = OffsetDateTime.parse(event.eventTs).toInstant().toEpochMilli();
            return event;
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public String toString() {
        return "ClickEvent{" + userId + ", " + eventType + ", " + eventTs + "}";
    }
}
