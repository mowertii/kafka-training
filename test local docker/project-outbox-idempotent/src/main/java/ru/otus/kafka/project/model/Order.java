package ru.otus.kafka.project.model;

/**
 * Заказ (бизнес-сущность).
 */
public record Order(
    String id,
    String userId,
    int amount,
    String status
) {
    public static Order newOrder(String id, String userId, int amount) {
        return new Order(id, userId, amount, "CREATED");
    }
}
