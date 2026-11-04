package com.dataplatform.flink;

import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Agregación incremental de la ventana: en lugar de guardar todos los eventos del
 * minuto, Flink mantiene solo estos contadores por usuario (estado mínimo).
 */
public class EventCounts {

    public long total;
    public long pageViews;
    public long clicks;
    public long addToCart;
    public long checkouts;

    public EventCounts() {}

    public static class Aggregator implements AggregateFunction<ClickEvent, EventCounts, EventCounts> {

        @Override
        public EventCounts createAccumulator() {
            return new EventCounts();
        }

        @Override
        public EventCounts add(ClickEvent event, EventCounts acc) {
            acc.total++;
            switch (event.eventType) {
                case "page_view":   acc.pageViews++; break;
                case "click":       acc.clicks++;    break;
                case "add_to_cart": acc.addToCart++; break;
                case "checkout":    acc.checkouts++; break;
                default:            break;
            }
            return acc;
        }

        @Override
        public EventCounts getResult(EventCounts acc) {
            return acc;
        }

        @Override
        public EventCounts merge(EventCounts a, EventCounts b) {
            a.total += b.total;
            a.pageViews += b.pageViews;
            a.clicks += b.clicks;
            a.addToCart += b.addToCart;
            a.checkouts += b.checkouts;
            return a;
        }
    }
}
