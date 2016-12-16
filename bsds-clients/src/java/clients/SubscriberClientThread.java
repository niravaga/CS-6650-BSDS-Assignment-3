/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package clients;

import java.util.concurrent.CyclicBarrier;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

/**
 *
 * @author nirav
 */
public class SubscriberClientThread implements Runnable {

    String topic;
    CyclicBarrier synk;

    public SubscriberClientThread(String topic, CyclicBarrier cb) {
        this.topic = topic;
        this.synk = cb;
    }

    public void run() {
        try {
            Client client = ClientBuilder.newClient();
            WebTarget myResource = client.target("http://" + ClientConstants.HOST + ":8080/KafkaEnterpriseApp-web/resources/subscriber");

            String response = myResource.request(MediaType.TEXT_PLAIN).post(Entity.entity(topic, MediaType.TEXT_PLAIN), String.class);

            int sleepTime = 100;
            int numMessages = 0;
            while (true) {
                WebTarget messageResource = client.target("http://" + ClientConstants.HOST + ":8080/KafkaEnterpriseApp-web/resources/message");
                messageResource = messageResource.queryParam("subscriberId", response);

                String message = messageResource.request(MediaType.TEXT_PLAIN).get(String.class);

                if (message == null || message.isEmpty()) {
                    Thread.sleep(sleepTime);
                    sleepTime *= 2;
                } else {
                    sleepTime = 100;
                    numMessages++;
//                    System.out.println(numMessages);
                }

                if (sleepTime > 10000) {
                    break;
                }

            }

            System.out.println("Subscriber Id " + response + " received " + numMessages + " messages on topic " + topic);
            synk.await();
        } catch (Exception e) {
            System.err.println("Client exception: " + e.toString());
        }

    }

}
