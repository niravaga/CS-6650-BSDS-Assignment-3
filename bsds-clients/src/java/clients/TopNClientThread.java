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
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

/**
 *
 * @author nirav
 */
public class TopNClientThread implements Runnable {

    private CyclicBarrier synk;

    public TopNClientThread(CyclicBarrier synk) {
        this.synk = synk;
    }

    @Override
    public void run() {
        try {
            Client client = ClientBuilder.newClient();

            int count = 0;
            while (true) {

                int n = (int) (Math.random() * 100);
                System.out.println("Getting top " + n + "  results.");
                WebTarget myResource = client.target("http://" + ClientConstants.HOST + ":8080/bsds-Assignment-2/resources/word/top/" + n);
                String response = myResource.request(MediaType.TEXT_PLAIN).get(String.class);

                Thread.sleep(1000);

                count++;

                if (count > 1000) {
                    break;
                }
            }

            synk.await();
        } catch (InterruptedException ex) {
            Logger.getLogger(TopNClientThread.class.getName()).log(Level.SEVERE, null, ex);
        } catch (BrokenBarrierException ex) {
            Logger.getLogger(TopNClientThread.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

}
