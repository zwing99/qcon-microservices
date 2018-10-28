package io.confluent.qcon.orders.domain;

public class OrderValidation {

    String OrderId;
    OrderValidationType type;
    OrderValidationResult result;

    public OrderValidation(String orderId, OrderValidationType type, OrderValidationResult result) {
        OrderId = orderId;
        this.type = type;
        this.result = result;
    }

    @Override
    public String toString() {
        return "OrderValidation{" +
                "OrderId='" + OrderId + '\'' +
                ", type=" + type +
                ", result=" + result +
                '}';
    }
}

