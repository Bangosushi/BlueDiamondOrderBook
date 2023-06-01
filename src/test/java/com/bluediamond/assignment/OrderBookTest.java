package com.bluediamond.assignment;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
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

    // Tests the process starts up and is running, then closes it.
    @Test
    public void orderBookStartAndClose() {
        // Setup
        BlockingQueue<OrderMessage> queue = new ArrayBlockingQueue<>(100);
        OrderBook book = new OrderBook("VOD.L", queue);
        Thread process = new Thread(book);
        process.start();

        // Process has started
        Assert.assertTrue(process.isAlive());

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

    @Test
    public void emptyOrderBook() {
        // Setup
        BlockingQueue<OrderMessage> queue = new ArrayBlockingQueue<>(100);
        OrderBook book = new OrderBook("VOD.L", queue);
        Thread process = new Thread(book);
        process.start();

        Assert.assertTrue(book.orderMap.isEmpty());
        Assert.assertTrue(book.buys.isEmpty());
        Assert.assertTrue(book.sells.isEmpty());

        // Close order book
        queue.add(new OrderMessage(OrderMessage.MessageType.Close, Order.EMPTY));
    }

    @Test
    public void ordersOnOrderBook() {
        // Setup
        BlockingQueue<OrderMessage> queue = new ArrayBlockingQueue<>(100);
        OrderBook book = new OrderBook("VOD.L", queue);
        Thread process = new Thread(book);
        process.start();

        // Send 6 orders (3 BID, 3 ASK)
        queue.addAll(newOrders);

        // Wait for orders to be processed
        try {
            waitForEmptyQueue(queue);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        Assert.assertEquals(6, book.orderMap.size());
        Assert.assertEquals(3, book.buys.size());
        Assert.assertEquals(3, book.sells.size());

        // Close order book
        queue.add(new OrderMessage(OrderMessage.MessageType.Close, Order.EMPTY));
    }

    @Test
    public void bookDepthEmpty() {
        // Setup
        BlockingQueue<OrderMessage> queue = new ArrayBlockingQueue<>(100);
        OrderBook book = new OrderBook("VOD.L", queue);
        Thread process = new Thread(book);
        process.start();

        Assert.assertEquals(0, book.getBookDepth(BID));
        Assert.assertEquals(0, book.getBookDepth(ASK));

        // Close order book
        queue.add(new OrderMessage(OrderMessage.MessageType.Close, Order.EMPTY));
    }

    @Test
    public void hasBookDepth() {
        // Setup
        BlockingQueue<OrderMessage> queue = new ArrayBlockingQueue<>(100);
        OrderBook book = new OrderBook("VOD.L", queue);
        Thread process = new Thread(book);
        process.start();

        // Send 3 BID orders with 2 different price levels, 3 ASK orders with 3 different price levels
        queue.addAll(newOrders);

        // Wait for orders to be processed
        try {
            waitForEmptyQueue(queue);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        Assert.assertEquals(2, book.getBookDepth(BID));
        Assert.assertEquals(3, book.getBookDepth(ASK));

        // Close order book
        queue.add(new OrderMessage(OrderMessage.MessageType.Close, Order.EMPTY));
    }

    @Test
    public void topOfBookEmpty() {
        // Setup
        BlockingQueue<OrderMessage> queue = new ArrayBlockingQueue<>(100);
        OrderBook book = new OrderBook("VOD.L", queue);
        Thread process = new Thread(book);
        process.start();

        Assert.assertEquals(BigDecimal.ZERO, book.getTopOfBook(BID));
        Assert.assertEquals(BigDecimal.ZERO, book.getTopOfBook(ASK));

        // Close order book
        queue.add(new OrderMessage(OrderMessage.MessageType.Close, Order.EMPTY));
    }

    @Test
    public void hasTopOfBook() {
        // Setup
        BlockingQueue<OrderMessage> queue = new ArrayBlockingQueue<>(100);
        OrderBook book = new OrderBook("VOD.L", queue);
        Thread process = new Thread(book);
        process.start();

        // Send 1 BID with price level at
        queue.add(newOrders.get(0));
        // Send 1 ASK with price level at
        queue.add(newOrders.get(4));
        // Wait for orders to be processed
        try {
            waitForEmptyQueue(queue);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        Assert.assertEquals(new BigDecimal("9.40"), book.getTopOfBook(BID));
        Assert.assertEquals(new BigDecimal("9.50"), book.getTopOfBook(ASK));

        // Close order book
        queue.add(new OrderMessage(OrderMessage.MessageType.Close, Order.EMPTY));
    }

    @Test
    public void sizeForPriceLevelEmpty() {
        // Setup
        BlockingQueue<OrderMessage> queue = new ArrayBlockingQueue<>(100);
        OrderBook book = new OrderBook("VOD.L", queue);
        Thread process = new Thread(book);
        process.start();

        Assert.assertEquals(0, book.getSizeForPriceLevel(BID, new BigDecimal("9.40")));
        Assert.assertEquals(0, book.getSizeForPriceLevel(ASK, new BigDecimal("9.40")));
        Assert.assertEquals(0, book.getSizeForPriceLevel(ASK, BigDecimal.ZERO));

        // Close order book
        queue.add(new OrderMessage(OrderMessage.MessageType.Close, Order.EMPTY));
    }

    @Test
    public void hasSizeForPriceLevel() {
        // Setup
        BlockingQueue<OrderMessage> queue = new ArrayBlockingQueue<>(100);
        OrderBook book = new OrderBook("VOD.L", queue);
        Thread process = new Thread(book);
        process.start();

        // Send 3 BID orders with 2 different price levels, 3 ASK orders with 3 different price levels
        queue.addAll(newOrders);

        // Wait for orders to be processed
        try {
            waitForEmptyQueue(queue);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        Assert.assertEquals(2, book.getSizeForPriceLevel(BID, new BigDecimal("9.40")));
        Assert.assertEquals(1, book.getSizeForPriceLevel(ASK, new BigDecimal("9.50")));

        // Close order book
        queue.add(new OrderMessage(OrderMessage.MessageType.Close, Order.EMPTY));
    }

    @Test
    public void orderBookIsSorted() {
        // Setup
        BlockingQueue<OrderMessage> queue = new ArrayBlockingQueue<>(100);
        OrderBook book = new OrderBook("VOD.L", queue);
        Thread process = new Thread(book);
        process.start();

        // Send orders
        queue.addAll(newOrders);

        // Wait for orders to be processed
        try {
            waitForEmptyQueue(queue);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

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

        // Close order book
        queue.add(new OrderMessage(OrderMessage.MessageType.Close, Order.EMPTY));
    }

    @Test
    public void enterSameOrders() {
        // Setup
        BlockingQueue<OrderMessage> queue = new ArrayBlockingQueue<>(100);
        OrderBook book = new OrderBook("VOD.L", queue);
        Thread process = new Thread(book);
        process.start();

        Order order1L = new Order(1L, BID, new BigDecimal("9.40"), 10L);

        // Send Order 1L
        queue.add(new OrderMessage(OrderMessage.MessageType.New, order1L));
        // Wait for orders to be processed
        try {
            waitForEmptyQueue(queue);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        order1L.setQuantity(20L);
        Assert.assertEquals(order1L.getOrderId(), book.orderMap.get(1L).getOrderId());

        // Send Order 1L again
        queue.add(new OrderMessage(OrderMessage.MessageType.New, order1L));
        // Wait for orders to be processed
        try {
            waitForEmptyQueue(queue);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        Assert.assertEquals(1, book.orderMap.size());
        Assert.assertEquals(10L, book.orderMap.get(1L).getQuantity());

        // Close order book
        queue.add(new OrderMessage(OrderMessage.MessageType.Close, Order.EMPTY));
    }

    @Test
    public void amendOrderQuantity() {
        // Setup
        BlockingQueue<OrderMessage> queue = new ArrayBlockingQueue<>(100);
        OrderBook book = new OrderBook("VOD.L", queue);
        Thread process = new Thread(book);
        process.start();

        // Send order 5L
        queue.add(newOrders.get(4));

        // Wait for orders to be processed
        try {
            waitForEmptyQueue(queue);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Order before amendment
        Assert.assertEquals(5L, book.orderMap.get(5L).getQuantity());

        // Amend Order 5L to quantity to 7L
        queue.add(new OrderMessage(OrderMessage.MessageType.Amend, new Order(5L, ASK, new BigDecimal("9.50"), 7L)));

        // Wait for orders to be processed
        try {
            waitForEmptyQueue(queue);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Order after amendment
        Assert.assertEquals(7L, book.orderMap.get(5L).getQuantity());

        // Close order book
        queue.add(new OrderMessage(OrderMessage.MessageType.Close, Order.EMPTY));
    }

    @Test
    public void amendOrderPrice() {
        // Setup
        BlockingQueue<OrderMessage> queue = new ArrayBlockingQueue<>(100);
        OrderBook book = new OrderBook("VOD.L", queue);
        Thread process = new Thread(book);
        process.start();

        // Send order 6L
        queue.add(newOrders.get(5));

        // Wait for orders to be processed
        try {
            waitForEmptyQueue(queue);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Order before amendment
        Assert.assertEquals(new BigDecimal("9.55"), book.orderMap.get(6L).getPrice());

        // Amend Order 6L to price of 9.50
        queue.add(new OrderMessage(OrderMessage.MessageType.Amend, new Order(6L, ASK, new BigDecimal("9.50"), 2L)));

        // Wait for orders to be processed
        try {
            waitForEmptyQueue(queue);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Order after amendment
        Assert.assertEquals(new BigDecimal("9.50"), book.orderMap.get(6L).getPrice());

        // Close order book
        queue.add(new OrderMessage(OrderMessage.MessageType.Close, Order.EMPTY));
    }

    @Test
    public void amendOrderQuantityAndPrice() {
        // Setup
        BlockingQueue<OrderMessage> queue = new ArrayBlockingQueue<>(100);
        OrderBook book = new OrderBook("VOD.L", queue);
        Thread process = new Thread(book);
        process.start();

        // Send order 4L
        queue.add(newOrders.get(3));

        // Wait for orders to be processed
        try {
            waitForEmptyQueue(queue);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Orders before amendments
        Assert.assertEquals(new BigDecimal("9.45"), book.orderMap.get(4L).getPrice());
        Assert.assertEquals(10L, book.orderMap.get(4L).getQuantity());

        // Amend order 4L to 9.42 price and 9 quantity
        queue.add(new OrderMessage(OrderMessage.MessageType.Amend, new Order(4L, ASK, new BigDecimal("9.42"), 9L)));

        // Wait for orders to be processed
        try {
            waitForEmptyQueue(queue);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Orders after amendments
        Assert.assertEquals(new BigDecimal("9.42"), book.orderMap.get(4L).getPrice());
        Assert.assertEquals(9L, book.orderMap.get(4L).getQuantity());

        // Close order book
        queue.add(new OrderMessage(OrderMessage.MessageType.Close, Order.EMPTY));
    }

    @Test
    public void invalidAmendPrice() {
        // Setup
        BlockingQueue<OrderMessage> queue = new ArrayBlockingQueue<>(100);
        OrderBook book = new OrderBook("VOD.L", queue);
        Thread process = new Thread(book);
        process.start();

        // Send orders 4L & 6L
        queue.addAll(Arrays.asList(newOrders.get(3), newOrders.get(5)));

        // Wait for orders to be processed
        try {
            waitForEmptyQueue(queue);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Order before amendments
        Assert.assertEquals(new BigDecimal("9.55"), book.orderMap.get(6L).getPrice());
        Assert.assertEquals(new BigDecimal("9.45"), book.orderMap.get(4L).getPrice());

        // Amend Order 6L to price of 0
        queue.add(new OrderMessage(OrderMessage.MessageType.Amend, new Order(6L, ASK, new BigDecimal("0"), 2L)));
        // Amend Order 4L price to -9.60
        queue.add(new OrderMessage(OrderMessage.MessageType.Amend, new Order(4L, ASK, new BigDecimal("-9.60"), 10L)));


        // Wait for orders to be processed
        try {
            waitForEmptyQueue(queue);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Order amendments have not taken place
        Assert.assertEquals(new BigDecimal("9.55"), book.orderMap.get(6L).getPrice());
        Assert.assertEquals(new BigDecimal("9.45"), book.orderMap.get(4L).getPrice());

        // Close order book
        queue.add(new OrderMessage(OrderMessage.MessageType.Close, Order.EMPTY));
    }

    @Test
    public void invalidAmendQuantity() {
        // Setup
        BlockingQueue<OrderMessage> queue = new ArrayBlockingQueue<>(100);
        OrderBook book = new OrderBook("VOD.L", queue);
        Thread process = new Thread(book);
        process.start();

        // Send orders 3L & 5L
        queue.addAll(Arrays.asList(newOrders.get(2), newOrders.get(4)));

        // Wait for orders to be processed
        try {
            waitForEmptyQueue(queue);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Order before amendment message
        Assert.assertEquals(5L, book.orderMap.get(5L).getQuantity());
        Assert.assertEquals(2L, book.orderMap.get(3L).getQuantity());

        // Amend Order 5L to quantity to 0
        queue.add(new OrderMessage(OrderMessage.MessageType.Amend, new Order(5L, ASK, new BigDecimal("9.50"), 0L)));
        // Amend Order 3L quantity to -10
        queue.add(new OrderMessage(OrderMessage.MessageType.Amend, new Order(3L, BID, new BigDecimal("9.35"), -10L)));

        // Wait for orders to be processed
        try {
            waitForEmptyQueue(queue);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Order amendments have not taken place
        Assert.assertEquals(5L, book.orderMap.get(5L).getQuantity());
        Assert.assertEquals(2L, book.orderMap.get(3L).getQuantity());

        // Close order book
        queue.add(new OrderMessage(OrderMessage.MessageType.Close, Order.EMPTY));
    }

    @Test
    public void amendNonExistentOrder() {
        // Setup
        BlockingQueue<OrderMessage> queue = new ArrayBlockingQueue<>(100);
        OrderBook book = new OrderBook("VOD.L", queue);
        Thread process = new Thread(book);
        process.start();

        // Order book is still empty
        Assert.assertTrue(book.orderMap.isEmpty());

        // Amend in-existing order 0L at new price level
        queue.add(new OrderMessage(OrderMessage.MessageType.Amend, new Order(0L, ASK, new BigDecimal("9.70"), 2L)));

        // Wait for orders to be processed
        try {
            waitForEmptyQueue(queue);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Order book is still empty
        Assert.assertTrue(book.orderMap.isEmpty());

        // Close order book
        queue.add(new OrderMessage(OrderMessage.MessageType.Close, Order.EMPTY));
    }

    @Test
    public void cancelOrder() {
        // Setup
        BlockingQueue<OrderMessage> queue = new ArrayBlockingQueue<>(100);
        OrderBook book = new OrderBook("VOD.L", queue);
        Thread process = new Thread(book);
        process.start();

        // Send order 4L
        queue.add(newOrders.get(3));

        // Wait for orders to be processed
        try {
            waitForEmptyQueue(queue);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Order book is not empty
        Assert.assertFalse(book.orderMap.isEmpty());

        // Cancel Order 4L
        queue.add(new OrderMessage(OrderMessage.MessageType.Cancel, new Order(4L, ASK, new BigDecimal("9.45"), 10L)));

        // Wait for orders to be processed
        try {
            waitForEmptyQueue(queue);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Order book is now empty
        Assert.assertTrue(book.orderMap.isEmpty());

        // Close order book
        queue.add(new OrderMessage(OrderMessage.MessageType.Close, Order.EMPTY));
    }

    @Test
    public void cancelNonExistentOrder() {
        // Setup
        BlockingQueue<OrderMessage> queue = new ArrayBlockingQueue<>(100);
        OrderBook book = new OrderBook("VOD.L", queue);
        Thread process = new Thread(book);
        process.start();

        // Order book is empty
        Assert.assertTrue(book.orderMap.isEmpty());

        // Cancel non-existent order 0L
        queue.add(new OrderMessage(OrderMessage.MessageType.Cancel, new Order(0L, ASK, new BigDecimal("9.50"), 5L)));

        // Wait for orders to be processed
        try {
            waitForEmptyQueue(queue);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Order book is still empty and up
        Assert.assertTrue(book.orderMap.isEmpty());

        // Close order book
        queue.add(new OrderMessage(OrderMessage.MessageType.Close, Order.EMPTY));
    }

    @Test
    public void tradeSomeQuantity() {
        // Setup
        BlockingQueue<OrderMessage> queue = new ArrayBlockingQueue<>(100);
        OrderBook book = new OrderBook("VOD.L", queue);
        Thread process = new Thread(book);
        process.start();

        // Send order 1L
        queue.add(newOrders.get(0));

        // Wait for orders to be processed
        try {
            waitForEmptyQueue(queue);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        Assert.assertEquals(10L, book.orderMap.get(1L).getQuantity());

        // Trade some volume on order 1L
        queue.add(new OrderMessage(OrderMessage.MessageType.Trade, new Order(1L, BID, new BigDecimal("9.40"), 2L)));

        // Wait for orders to be processed
        try {
            waitForEmptyQueue(queue);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Order has 8 quantity left after trading 2
        Assert.assertEquals(8L, book.orderMap.get(1L).getQuantity());

        // Close order book
        queue.add(new OrderMessage(OrderMessage.MessageType.Close, Order.EMPTY));
    }

    @Test
    public void multiTradeOrder() {
        // Setup
        BlockingQueue<OrderMessage> queue = new ArrayBlockingQueue<>(100);
        OrderBook book = new OrderBook("VOD.L", queue);
        Thread process = new Thread(book);
        process.start();

        // Send order 1L
        queue.add(newOrders.get(0));

        // Wait for orders to be processed
        try {
            waitForEmptyQueue(queue);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        Assert.assertEquals(10L, book.orderMap.get(1L).getQuantity());

        // Trade some volume on order 1L
        queue.add(new OrderMessage(OrderMessage.MessageType.Trade, new Order(1L, BID, new BigDecimal("9.40"), 2L)));

        // Wait for orders to be processed
        try {
            waitForEmptyQueue(queue);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Order has 8 quantity left after trading 2
        Assert.assertEquals(8L, book.orderMap.get(1L).getQuantity());

        // Trade some volume on order 1L
        queue.add(new OrderMessage(OrderMessage.MessageType.Trade, new Order(1L, BID, new BigDecimal("9.40"), 4L)));

        // Wait for orders to be processed
        try {
            waitForEmptyQueue(queue);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Order has 4 quantity left after trading 4
        Assert.assertEquals(4L, book.orderMap.get(1L).getQuantity());

        // Close order book
        queue.add(new OrderMessage(OrderMessage.MessageType.Close, Order.EMPTY));
    }

    @Test
    public void tradeAllQuantity() {
        // Setup
        BlockingQueue<OrderMessage> queue = new ArrayBlockingQueue<>(100);
        OrderBook book = new OrderBook("VOD.L", queue);
        Thread process = new Thread(book);
        process.start();

        // Send order 1L
        queue.add(newOrders.get(0));

        // Wait for orders to be processed
        try {
            waitForEmptyQueue(queue);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        Assert.assertEquals(10L, book.orderMap.get(1L).getQuantity());

        // Trade all volume on order 1L
        queue.add(new OrderMessage(OrderMessage.MessageType.Trade, new Order(1L, BID, new BigDecimal("9.40"), 10L)));

        // Wait for orders to be processed
        try {
            waitForEmptyQueue(queue);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Order was removed from book aftre being fully filled
        Assert.assertTrue(book.orderMap.isEmpty());

        // Close order book
        queue.add(new OrderMessage(OrderMessage.MessageType.Close, Order.EMPTY));
    }

    private void waitForEmptyQueue(BlockingQueue<OrderMessage> queue) throws InterruptedException {
        while (!queue.isEmpty()) {
            System.out.println("Sleeping...");
            Thread.sleep(50);
        }
    }
}

