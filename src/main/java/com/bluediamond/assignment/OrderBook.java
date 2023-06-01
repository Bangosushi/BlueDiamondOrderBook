package com.bluediamond.assignment;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

/**
 * This is the main order book process which implements Level2View interfaces, as well as Runnable.
 * It uses a BlockingQueue to handle incoming message requests in a thread safe way.
 * A map of all orders on the book is stored for faster response time on certain functions.
 * Two separate ordered list of Order objects maintain the ASK and BID orders to also enable faster response time.
 */
public class OrderBook implements Level2View, Runnable {

    String id;
    List<Order> buys;
    List<Order> sells;
    BlockingQueue<OrderMessage> queue;
    Map<Long, Order> orderMap = new HashMap<>();
    private static Logger logger = LogManager.getLogger(OrderBook.class);

    public OrderBook(String id, BlockingQueue<OrderMessage> queue) {
        this.id = id;
        this.queue = queue;
        buys = new LinkedList<>();
        sells = new LinkedList<>();
    }

    /**
     * Main function for the OrderBook object which scans for incoming OrderMessages in the BlockingQueue object.
     * Will call the appropriate implemented Level2View method according to message types (New, Cancel, Amend, and
     * Trade), or will exit the process upon receiving a Close MessageType.
     */
    public void run() {
        logger.info("Order Book " + id + " started...");
        while (true) {
            try {
                OrderMessage msg = queue.take();
                Order order = msg.getOrderData();
                switch (msg.getMsgType()) {
                    case New -> onNewOrder(order.getSide(), order.getPrice(), order.getQuantity(), order.getOrderId());
                    case Cancel -> onCancelOrder(order.getOrderId());
                    case Amend -> onReplaceOrder(order.getPrice(), order.getQuantity(), order.getOrderId());
                    case Trade -> onTrade(order.getQuantity(), order.getOrderId());
                    case Close -> {
                        logger.info("Order Book " + id + " Closing...");
                        return;
                    }
                    default -> logger.error("Unexpected value: " + msg.getMsgType());
                }
            } catch (InterruptedException e) {
                logger.error("Order book was interrupted unexpectedly.", e);
            }
        }
    }

    /**
     * @param side Enum describing which side the request is for
     * @return The list of orders corresponding to the side requested
     */
    private List<Order> getOrderList(Side side) {
        return side == Side.BID ? buys : sells;
    }

    /**
     * Helper function to filter stream based on object propriety
     */
    private static <T> Predicate<T> distinctByKey(
            Function<? super T, ?> keyExtractor) {
        Map<Object, Boolean> seen = new ConcurrentHashMap<>();
        return t -> seen.putIfAbsent(keyExtractor.apply(t), Boolean.TRUE) == null;
    }

    /**
     * Will check the order request is valid. If successful, it will be added to the order book.
     *
     * @param side     Enum describing which side the new order is
     * @param price    Price of the order
     * @param quantity Quantity of the order
     * @param orderId  Unique identifier for the order
     */
    public void onNewOrder(Side side, BigDecimal price, long quantity, long orderId) {
        if (price.compareTo(BigDecimal.ZERO) <= 0) {
            logger.warn("Invalid price on new order: " + price);
            logger.debug("Order Id: " + orderId + "; Price: " + price + "; Quantity: " + quantity);
            return;
        }
        if (quantity <= 0L) {
            logger.warn("Invalid quantity on new order: " + quantity);
            logger.debug("Order Id: " + orderId + "; Price: " + price + "; Quantity: " + quantity);
            return;
        }
        if (orderMap.containsKey(orderId)) {
            logger.warn("Order already exists: " + orderId);
            logger.debug("Order Id: " + orderId + "; Price: " + price + "; Quantity: " + quantity);
            return;
        }
        Order newOrder = new Order(orderId, side, price, quantity);
        orderMap.put(newOrder.getOrderId(), newOrder);
        List<Order> orderList = getOrderList(side);
        orderList.add(newOrder);
        Collections.sort(orderList);
        logger.info("New order created: " + newOrder);
    }

    /**
     * Will check order exists on the order book, and cancel it if successful.
     *
     * @param orderId Unique identifier for the order to cancel
     */
    public void onCancelOrder(long orderId) {
        if (orderMap.containsKey(orderId)) {
            Order cancelledOrder = orderMap.remove(orderId);
            List<Order> orderList = getOrderList(cancelledOrder.getSide());
            orderList.remove(cancelledOrder);
            logger.info("Order cancelled: " + cancelledOrder);
        } else {
            logger.warn("Order not found: " + orderId);
        }
    }

    /**
     * Will check the price and/or quantity are amended correctly, and amend the order on the order book if successful.
     *
     * @param price    Price of the amended order
     * @param quantity Quantity of the amended order
     * @param orderId  Unique identifier of the amended order
     */
    public void onReplaceOrder(BigDecimal price, long quantity, long orderId) {
        if (price.compareTo(BigDecimal.ZERO) > 0 && quantity > 0L) {
            if (orderMap.containsKey(orderId)) {
                Order amendedOrder = orderMap.get(orderId).setPrice(price).setQuantity(quantity);
                List<Order> orderList = getOrderList(amendedOrder.getSide());
                for (Order order : orderList) {
                    if (order.getOrderId() == orderId) {
                        order.setPrice(price).setQuantity(quantity);
                    }
                }
                logger.info("Order amended: " + amendedOrder);
            } else {
                logger.warn("Order not found: " + orderId);
            }
        } else {
            logger.warn("Invalid price and/or quantity amendment: price(" + price + ") quantity(" + quantity + ")");
            logger.debug("Order Id: " + orderId + "; Price: " + price + "; Quantity: " + quantity);
        }
    }

    /**
     * Will check the order exists on the order book and that the quantity doesn't exceed the resting order, and trade
     * if successful. Will delete the order if the order has no volume left.
     *
     * @param quantity       Quantity traded on the order
     * @param restingOrderId Order being traded on
     */
    public void onTrade(long quantity, long restingOrderId) {
        if (orderMap.containsKey(restingOrderId)) {
            Order tradedOrder = orderMap.get(restingOrderId);
            if (tradedOrder.getQuantity() < quantity) {
                logger.warn("Not enough volume left in order " + restingOrderId + "to trade " + quantity);
                logger.debug("Traded order -> " + tradedOrder);
                return;
            }
            List<Order> orderList = getOrderList(tradedOrder.getSide());
            logger.info(quantity + " traded on order: " + tradedOrder);
            tradedOrder.fillOrder(quantity);
            if (tradedOrder.getQuantity() == 0L) {
                orderList.remove(tradedOrder);
                orderMap.remove(restingOrderId);
                logger.info("Order was fully filled, removing from depth");
            }
        } else {
            logger.warn("Order not found: " + restingOrderId);
        }
    }

    /**
     * Will check the price level is correct and that the side of the order book requested isn't empty,
     * then returns the number of orders requested.
     *
     * @param side  Side of the price level requested
     * @param price Price level of the request
     * @return The number of orders at requested side and price level
     */
    public long getSizeForPriceLevel(Side side, final BigDecimal price) {
        if (price.compareTo(BigDecimal.ZERO) <= 0) {
            logger.warn("Invalid price level value on " + side + ": " + price);
            return 0L;
        }
        List<Order> orderList = getOrderList(side);
        if (!orderList.isEmpty()) {
            return orderList.stream().filter(x -> Objects.equals(x.getPrice(), price)).count();
        } else {
            logger.debug("No " + side + " order(s) found at price level: " + price);
            return 0L;
        }
    }

    /**
     * @param side Side of the book depth request
     * @return The number of price levels on the requested side of the order book
     */
    public long getBookDepth(Side side) {
        List<Order> orderList = getOrderList(side);
        return orderList.stream()
                .filter(distinctByKey(Order::getPrice))
                .count();
    }

    /**
     * Will check the side of the order book isn't empty, and return the top price level if successful.
     *
     * @param side Side of the book to request the top price level of
     * @return The price level of the side of the order book requested.
     */
    public BigDecimal getTopOfBook(Side side) {
        List<Order> orderList = getOrderList(side);
        if (!orderList.isEmpty()) {
            return orderList.get(0).getPrice();
        } else {
            logger.debug("No orders on book for : " + side);
            return BigDecimal.ZERO;
        }
    }
}
