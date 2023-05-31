package com.bluediamond.assignment;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static com.bluediamond.assignment.Level2View.Side.ASK;
import static com.bluediamond.assignment.Level2View.Side.BID;

public class OrderBookTest {
    private final List<OrderMessage> newOrders = new ArrayList<>();

    @Before
    public void setup() {
        newOrders.add(new OrderMessage(OrderMessage.MessageType.New, new Order(1L, BID, new BigDecimal("9.40"), 10L)));
        newOrders.add(new OrderMessage(OrderMessage.MessageType.New, new Order(2L, BID, new BigDecimal("9.40"), 5L)));
        newOrders.add(new OrderMessage(OrderMessage.MessageType.New, new Order(3L, BID, new BigDecimal("9.35"), 2L)));
        newOrders.add(new OrderMessage(OrderMessage.MessageType.New, new Order(4L, ASK, new BigDecimal("9.45"), 10L)));
        newOrders.add(new OrderMessage(OrderMessage.MessageType.New, new Order(5L, ASK, new BigDecimal("9.50"), 5L)));
        newOrders.add(new OrderMessage(OrderMessage.MessageType.New, new Order(6L, ASK, new BigDecimal("9.55"), 2L)));
    }

    /**
     * This test checks if the order book can handle data requests when empty.
     */
    @Test
    public void emptyOrderBook() {
        // Setup
        BlockingQueue<OrderMessage> queue = new ArrayBlockingQueue<>(100);
        OrderBook book = new OrderBook("VOD.L", queue);
        Thread process = new Thread(book);
        process.start();

        // Process has started
        Assert.assertTrue(process.isAlive());

        // Check L2 Data doesn't fail
        Assert.assertEquals(BigDecimal.ZERO, book.getTopOfBook(BID));
        Assert.assertEquals(BigDecimal.ZERO, book.getTopOfBook(ASK));
        Assert.assertEquals(0, book.getBookDepth(BID));
        Assert.assertEquals(0, book.getBookDepth(ASK));
        Assert.assertEquals(0, book.getSizeForPriceLevel(BID, new BigDecimal("9.40")));
        Assert.assertEquals(0, book.getSizeForPriceLevel(ASK, new BigDecimal("9.40")));
        Assert.assertEquals(0, book.getSizeForPriceLevel(ASK, BigDecimal.ZERO));

        // Close order book
        queue.add(new OrderMessage(OrderMessage.MessageType.Close, Order.EMPTY));

        // Wait for message to be processed
        try {
            waitForEmptyQueue(queue);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Process is closed
        Assert.assertFalse(process.isAlive());
    }

    /**
     * This test enters 6 orders and checks that they have been entered correctly.
     * Then it checks if the BIDs and ASKs are sorted accordingly.
     */
    @Test
    public void orderBook() {
        // Setup
        BlockingQueue<OrderMessage> queue = new ArrayBlockingQueue<>(100);
        OrderBook book = new OrderBook("VOD.L", queue);
        Thread process = new Thread(book);
        process.start();

        // Process has started
        Assert.assertTrue(process.isAlive());

        // Send orders
        queue.addAll(newOrders);

        // Wait for orders to be processed
        try {
            waitForEmptyQueue(queue);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Expected 3 BID and 3 ASK orders on book
        Assert.assertEquals(2, book.getBookDepth(BID));
        Assert.assertEquals(3, book.getBookDepth(ASK));
        // Expected size of 2 at 9.40 (1L & 2L)
        Assert.assertEquals(2, book.getSizeForPriceLevel(BID, new BigDecimal("9.40")));
        // Expected size of 0 at 9.41
        Assert.assertEquals(0, book.getSizeForPriceLevel(BID, new BigDecimal("9.41")));

        // Check the order book is sorted correctly for BIDs and ASKs
        Iterator<Order> orderIterator = book.buys.iterator();
        Order current, previous = orderIterator.next();
        while (orderIterator.hasNext()) {
            current = orderIterator.next();
            if (previous.compareTo(current) > 0) {
                Assert.fail("BID not ordered");
            }
        }
        orderIterator = book.sells.iterator();
        previous = orderIterator.next();
        while (orderIterator.hasNext()) {
            current = orderIterator.next();
            if (previous.compareTo(current) > 0) {
                Assert.fail("ASK not ordered");
            }
        }

        // Expected top of book for BID (9.40) and ASK (9.45)
        Assert.assertEquals(new BigDecimal("9.40"), book.getTopOfBook(BID));
        Assert.assertEquals(new BigDecimal("9.45"), book.getTopOfBook(ASK));

        // Close order book
        queue.add(new OrderMessage(OrderMessage.MessageType.Close, Order.EMPTY));

        // Wait for message to be processed
        try {
            waitForEmptyQueue(queue);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Process is closed
        Assert.assertFalse(process.isAlive());
    }

    /**
     * This test enters 6 orders, and tries to re-enter orderId(1) which will fail.
     */
    @Test
    public void enterOrders() {
        // Setup
        BlockingQueue<OrderMessage> queue = new ArrayBlockingQueue<>(100);
        OrderBook book = new OrderBook("VOD.L", queue);
        Thread process = new Thread(book);
        process.start();

        // Process has started
        Assert.assertTrue(process.isAlive());

        // Send orders
        queue.addAll(newOrders);
        // Re-adding Order 1L (will fail)
        queue.add(new OrderMessage(OrderMessage.MessageType.New, new Order(1L, BID, new BigDecimal("9.40"), 10L)));

        // Wait for orders to be processed
        try {
            waitForEmptyQueue(queue);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Expected 3 BID and 3 ASK orders on book, only 2 at 9.40 (1L & 2L)
        Assert.assertEquals(2, book.getBookDepth(BID));
        Assert.assertEquals(3, book.getBookDepth(ASK));
        Assert.assertEquals(2, book.getSizeForPriceLevel(BID, new BigDecimal("9.40")));
        Assert.assertEquals(new BigDecimal("9.40"), book.getTopOfBook(BID));
        Assert.assertEquals(new BigDecimal("9.45"), book.getTopOfBook(ASK));


        // Close order book
        queue.add(new OrderMessage(OrderMessage.MessageType.Close, Order.EMPTY));

        // Wait for message to be processed
        try {
            waitForEmptyQueue(queue);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Process is closed
        Assert.assertFalse(process.isAlive());
    }

    /**
     * This test enters 6 orders, amends the price on OrderId(6), amends the quantity on OrderId(5),
     * amends the price and quantity on OrderId(4), then tries to amend OrderId(0) which does not exist.
     */
    @Test
    public void amendOrders() {
        // Setup
        BlockingQueue<OrderMessage> queue = new ArrayBlockingQueue<>(100);
        OrderBook book = new OrderBook("VOD.L", queue);
        Thread process = new Thread(book);
        process.start();

        // Process has started
        Assert.assertTrue(process.isAlive());

        // Send orders
        queue.addAll(newOrders);

        // Wait for orders to be processed
        try {
            waitForEmptyQueue(queue);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Amend Order 6L to price of 9.50
        queue.add(new OrderMessage(OrderMessage.MessageType.Amend, new Order(6L, ASK, new BigDecimal("9.50"), 2L)));
        // Amend Order 5L to quantity to 7L
        queue.add(new OrderMessage(OrderMessage.MessageType.Amend, new Order(5L, ASK, new BigDecimal("9.50"), 7L)));
        // Amend Order 4L price to 9.42 and quantity to 9L
        queue.add(new OrderMessage(OrderMessage.MessageType.Amend, new Order(4L, ASK, new BigDecimal("9.42"), 9L)));
        // Amend in-existing order 0L at new price level
        queue.add(new OrderMessage(OrderMessage.MessageType.Amend, new Order(0L, ASK, new BigDecimal("9.70"), 2L)));

        // Wait for orders to be processed
        try {
            waitForEmptyQueue(queue);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Expected only 9.42 (4L) and 9.50 (5L & 6L) to be the book depth for order book (0L doesn't exist)
        Assert.assertEquals(2, book.getBookDepth(ASK));
        Assert.assertEquals(0, book.getSizeForPriceLevel(ASK, new BigDecimal("9.45")));
        Assert.assertEquals(0, book.getSizeForPriceLevel(ASK, new BigDecimal("9.55")));
        Assert.assertEquals(0, book.getSizeForPriceLevel(ASK, new BigDecimal("9.70")));
        Assert.assertEquals(1, book.getSizeForPriceLevel(ASK, new BigDecimal("9.42")));
        Assert.assertEquals(2, book.getSizeForPriceLevel(ASK, new BigDecimal("9.50")));

        // Close order book
        queue.add(new OrderMessage(OrderMessage.MessageType.Close, Order.EMPTY));

        // Wait for message to be processed
        try {
            waitForEmptyQueue(queue);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Process is closed
        Assert.assertFalse(process.isAlive());
    }

    /**
     * This test enters 6 orders and cancels OrderId(4). Then tries to cancel OrderId(0), which doesn't exist.
     */
    @Test
    public void cancelOrders() {
        // Setup
        BlockingQueue<OrderMessage> queue = new ArrayBlockingQueue<>(100);
        OrderBook book = new OrderBook("VOD.L", queue);
        Thread process = new Thread(book);
        process.start();

        // Process has started
        Assert.assertTrue(process.isAlive());

        // Send orders
        queue.addAll(newOrders);

        // Wait for orders to be processed
        try {
            waitForEmptyQueue(queue);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Cancel Order 4L
        queue.add(new OrderMessage(OrderMessage.MessageType.Cancel, new Order(4L, ASK, new BigDecimal("9.45"), 10L)));
        // Cancel in-existing order 0L
        queue.add(new OrderMessage(OrderMessage.MessageType.Cancel, new Order(0L, ASK, new BigDecimal("9.50"), 5L)));

        // Wait for orders to be processed
        try {
            waitForEmptyQueue(queue);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Expected only 9.50 (5L) & 9.55 (6L) to be the book depth for order book
        Assert.assertEquals(2, book.getBookDepth(ASK));
        Assert.assertEquals(0, book.getSizeForPriceLevel(ASK, new BigDecimal("9.45")));
        Assert.assertEquals(1, book.getSizeForPriceLevel(ASK, new BigDecimal("9.50")));
        Assert.assertEquals(1, book.getSizeForPriceLevel(ASK, new BigDecimal("9.55")));

        // Close order book
        queue.add(new OrderMessage(OrderMessage.MessageType.Close, Order.EMPTY));

        // Wait for message to be processed
        try {
            waitForEmptyQueue(queue);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Process is closed
        Assert.assertFalse(process.isAlive());
    }

    /**
     * This test enters 6 orders and trades OrderId(1) 2 volume, then 8 volume, thus removing it from the depth.
     * Then, it tries to trade on an order for more than the available quantity, thus being rejected.
     */
    @Test
    public void tradeOrders() {
        // Setup
        BlockingQueue<OrderMessage> queue = new ArrayBlockingQueue<>(100);
        OrderBook book = new OrderBook("VOD.L", queue);
        Thread process = new Thread(book);
        process.start();
        // Process has started
        Assert.assertTrue(process.isAlive());

        // Send orders
        queue.addAll(newOrders);

        // Wait for orders to be processed
        try {
            waitForEmptyQueue(queue);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Trade some volume on order 1L
        queue.add(new OrderMessage(OrderMessage.MessageType.Trade, new Order(1L, BID, new BigDecimal("9.40"), 2L)));
        // Trade remaining volume on order 1L
        queue.add(new OrderMessage(OrderMessage.MessageType.Trade, new Order(1L, BID, new BigDecimal("9.40"), 8L)));
        // Trade too much volume on order 2L
        queue.add(new OrderMessage(OrderMessage.MessageType.Trade, new Order(2L, BID, new BigDecimal("9.40"), 6L)));


        // Wait for orders to be processed
        try {
            waitForEmptyQueue(queue);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Expected only 2L (9.40) & 3L (9.35) on BID order book
        Assert.assertEquals(2, book.getBookDepth(BID));
        Assert.assertEquals(1, book.getSizeForPriceLevel(BID, new BigDecimal("9.40")));
        Assert.assertEquals(1, book.getSizeForPriceLevel(BID, new BigDecimal("9.35")));

        // Close order book
        queue.add(new OrderMessage(OrderMessage.MessageType.Close, Order.EMPTY));

        // Wait for message to be processed
        try {
            waitForEmptyQueue(queue);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Process is closed
        Assert.assertFalse(process.isAlive());
    }

    public void waitForEmptyQueue(BlockingQueue<OrderMessage> queue) throws InterruptedException {
        while (!queue.isEmpty()) {
            System.out.println("Sleeping...");
            Thread.sleep(50);
        }
    }
}

