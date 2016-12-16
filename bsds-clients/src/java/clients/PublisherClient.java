/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package clients;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

/**
 *
 * @author nirav
 */
public class PublisherClient {
        
    static CyclicBarrier barrier;
    public static void main(String[] args) {
        int numThreads = 50;

        int numMessages = 100;

        String topics[] = {
            "Sports",
            "News",
            "Technology",
            "DistributedSystems",
            "TheNextJavascriptLibrary",
            "DonaldTrumpsTweets",
            "SeattleStartups",
            "HackerNews",
            "Weather",
            "Recipes"};

        barrier = new CyclicBarrier(numThreads + 1);

        try {
            Thread[] publisherThreads = new Thread[numThreads];

            long startTime = System.currentTimeMillis();

            for (int i = 0; i < numThreads; i++) {
                publisherThreads[i] = new Thread(new PublisherClientThread(topics[i % topics.length], barrier, numMessages));
                publisherThreads[i].start();
            }

            System.out.println("Publisher Client waiting at barrier");

            barrier.await();

            long endTime = System.currentTimeMillis();

            long publishTime = endTime - startTime;
            System.out.println("Took " + publishTime + "ms to complete publishing.");
        } catch (InterruptedException e) {
            System.err.println("Client exception: " + e.toString());
        } catch (BrokenBarrierException e) {
            System.err.println("Client exception: " + e.toString());
        }

    }
}
