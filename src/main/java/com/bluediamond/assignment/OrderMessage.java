package com.bluediamond.assignment;

/**
 * OrderMessage class used to send requests to the OrderBook.
 * Describes a MessageType Enum to describe the request, and contains an Order object to interact with OrderBook.
 */
public class OrderMessage {
    /**
     * Different message types handled by the OrderBook
     * New - New Order (Side, Price, Quantity, OrderId)
     * Amend - Amend Order (Price, Quantity, OrderId)
     * Cancel - Cancel Order (OrderId)
     * Trade - Trade Order (Quantity, OrderId)
     * Close - Close Book ()
     */
    public enum MessageType {
        New, Amend, Cancel, Trade, Close
    }

    private final MessageType msgType;

    private Order orderData;

    public OrderMessage(MessageType msgType, Order orderData) {
        this.msgType = msgType;
        this.orderData = orderData;
    }

    public MessageType getMsgType() {
        return msgType;
    }

    public Order getOrderData() {
        return orderData;
    }
}
