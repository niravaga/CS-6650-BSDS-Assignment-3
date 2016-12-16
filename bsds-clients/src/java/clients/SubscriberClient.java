/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package clients;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author nirav
 */
public class SubscriberClient {

    static CyclicBarrier barrier;

    public static void main(String[] args) {
        int numThreads = 30;

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
            Thread[] subscriberThreads = new Thread[numThreads];

            long startTime = System.currentTimeMillis();
            for (int i = 0; i < numThreads; i++) {
                subscriberThreads[i] = new Thread(new SubscriberClientThread(topics[i % topics.length], barrier));
                subscriberThreads[i].start();
            }

            barrier.await();

            long endTime = System.currentTimeMillis();
            long timeTaken = endTime - startTime;

            System.out.println("Subscribers took " + timeTaken + "ms to complete.");

        } catch (InterruptedException ex) {
            Logger.getLogger(SubscriberClient.class.getName()).log(Level.SEVERE, null, ex);
        } catch (BrokenBarrierException ex) {
            Logger.getLogger(SubscriberClient.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

}
