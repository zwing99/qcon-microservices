package io.confluent.qcon.orders.domain;

public class Order {

    String Id;
    String customerId;
    OrderState state;
    Product product;
    int quantity;
    double price;

    public Order(String id, String customerId, OrderState state, Product product, int quantity, double price) {
        Id = id;
        this.customerId = customerId;
        this.state = state;
        this.product = product;
        this.quantity = quantity;
        this.price = price;
    }


    public OrderState getState() {
        return state;
    }

    public String getCustomerId() {
        return customerId;
    }

    public Product getProduct() {
        return product;
    }

    public int getQuantity() {
        return quantity;
    }

    public double getPrice() {
        return price;
    }

    public String getId() {
        return Id;
    }

    @Override
    public String toString() {
        return "Order{" +
                "Id='" + Id + '\'' +
                ", customerId='" + customerId + '\'' +
                ", state=" + state +
                ", product=" + product +
                ", quantity=" + quantity +
                ", price=" + price +
                '}';
    }
}
