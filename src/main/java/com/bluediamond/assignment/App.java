package com.bluediamond.assignment;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Blue Diamond Assignment App
 */
public class App {
    public static void main(String[] args) {
        System.out.println("Blue Diamond Assignment");
        BlockingQueue<OrderMessage> queue = new ArrayBlockingQueue<>(100);
        OrderBook book = new OrderBook("VOD.L", queue);
        new Thread(book).start();
    }
}
