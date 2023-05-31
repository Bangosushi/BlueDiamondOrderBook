package com.bluediamond.assignment;

import com.bluediamond.assignment.Level2View.Side;

import java.math.BigDecimal;
import java.util.Date;

/**
 * Order Class populating the OrderBook implementation.
 * Implements Comparable for easier sorting depending on Side value.
 */
public class Order implements Comparable<Order> {
    public static Order EMPTY = new Order();
    private long orderId;
    private Side side;
    private BigDecimal price;
    private long quantity;
    private Date timestamp;

    private Order() {
    }

    public Order(long orderId, Side side, BigDecimal price, long quantity) {
        this.orderId = orderId;
        this.side = side;
        this.price = price;
        this.quantity = quantity;
        this.timestamp = new Date();
    }

    /**
     * The custom ordering is done at timestamp level for equally priced orders, and at price level after that.
     *
     * @param other the object to be compared.
     * @return Negative value if this object should be sorted after the other object, positive value if this
     * object should be sorted before the other object.
     */
    public int compareTo(Order other) {
        if (this.getPrice().equals(other.getPrice())) {
            return this.getTimestamp().compareTo(other.getTimestamp());
        } else {
            if (getSide().equals(Side.BID)) {
                return -1 * this.getPrice().compareTo(other.getPrice());
            } else {
                return this.getPrice().compareTo(other.getPrice());
            }
        }
    }

    /**
     * @param quantity Quantity to be removed from the order
     */
    public void fillOrder(long quantity) {
        this.quantity -= quantity;
    }

    public long getOrderId() {
        return orderId;
    }

    public Side getSide() {
        return side;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public long getQuantity() {
        return quantity;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public Order setPrice(BigDecimal price) {
        this.price = price;
        return this;
    }

    public Order setQuantity(long quantity) {
        this.quantity = quantity;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Order order = (Order) o;

        if (getOrderId() != order.getOrderId()) return false;
        if (getQuantity() != order.getQuantity()) return false;
        if (getSide() != order.getSide()) return false;
        if (getPrice() != null ? !getPrice().equals(order.getPrice()) : order.getPrice() != null) return false;
        return getTimestamp() != null ? getTimestamp().equals(order.getTimestamp()) : order.getTimestamp() == null;
    }

    @Override
    public int hashCode() {
        int result = (int) (getOrderId() ^ (getOrderId() >>> 32));
        result = 31 * result + (getSide() != null ? getSide().hashCode() : 0);
        result = 31 * result + (getPrice() != null ? getPrice().hashCode() : 0);
        result = 31 * result + (int) (getQuantity() ^ (getQuantity() >>> 32));
        result = 31 * result + (getTimestamp() != null ? getTimestamp().hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Order{" +
                "orderId=" + orderId +
                ", side=" + side +
                ", price=" + price +
                ", quantity=" + quantity +
                ", timestamp=" + timestamp +
                '}';
    }
}
