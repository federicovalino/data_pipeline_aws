package com.dataplatform.flink;

/**
 * Resultado por usuario y ventana de 1 minuto (Event Time).
 * pendingCartItems y abandonmentRisk vienen del Keyed State acumulado entre ventanas.
 */
public class UserActivity {

    public String userId;
    public String windowStart;
    public String windowEnd;
    public long totalEvents;
    public long pageViews;
    public long clicks;
    public long addToCart;
    public long checkouts;
    public long pendingCartItems;
    public boolean abandonmentRisk;
    public boolean alertTriggered;

    public UserActivity() {}

    /** Formato JSON de una línea: fácil de filtrar con CloudWatch Logs Insights. */
    @Override
    public String toString() {
        return String.format(
                "{\"user_id\":\"%s\",\"window_start\":\"%s\",\"window_end\":\"%s\",\"total_events\":%d,"
                        + "\"page_views\":%d,\"clicks\":%d,\"add_to_cart\":%d,\"checkouts\":%d,"
                        + "\"pending_cart_items\":%d,\"abandonment_risk\":%b,\"alert_triggered\":%b}",
                userId, windowStart, windowEnd, totalEvents, pageViews, clicks, addToCart, checkouts,
                pendingCartItems, abandonmentRisk, alertTriggered);
    }
}
