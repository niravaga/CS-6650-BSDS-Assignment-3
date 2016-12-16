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
public class TopNClient {
    static CyclicBarrier barrier;
    
    public static void main(String[] args) {
        try {
            int numThreads = 20;
            
            barrier = new CyclicBarrier(numThreads + 1);
            
            Thread[] topNThreads = new Thread[numThreads];
            for (int i = 0; i < numThreads; i++) {
                topNThreads[i] = new Thread(new TopNClientThread(barrier));
                topNThreads[i].start();
            }
            
            barrier.await();
        } catch (InterruptedException ex) {
            Logger.getLogger(TopNClient.class.getName()).log(Level.SEVERE, null, ex);
        } catch (BrokenBarrierException ex) {
            Logger.getLogger(TopNClient.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
