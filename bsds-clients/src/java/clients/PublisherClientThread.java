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
public class PublisherClientThread implements Runnable {

    private final String topic;
    private final CyclicBarrier synk;
    private final int numMessages;

    String messages[] = {
        "Steelers quarterback Ben Roethlisberger has a torn meniscus in left knee and will have surgery Monday morning. No timetable for his return.",
        "Animals representing Hillary Clinton and Dems in North Carolina just firebombed our office in Orange County because we are winning @NCGOP",
        "#Haima could become a super typhoon in the western Pacific before threatening the Philippines: http://wxch.nl/2dG1NWF",
        "Julian Assange's internet link has been intentionally severed by a state party. We have activated the appropriate contingency plans.",
        "The classically trained African-American folk singer Odetta was an enormous influence on the young Bob Dylan. http://nyer.cm/lqo61Du"
    };

    public PublisherClientThread(String topic, CyclicBarrier cb, int numMessages) {
        this.topic = topic;
        this.synk = cb;
        this.numMessages = numMessages;
    }

    public void run() {
        try {
            Client client = ClientBuilder.newClient();
            WebTarget myResource = client.target("http://" + ClientConstants.HOST + ":8080/KafkaEnterpriseApp-web/resources/publisher");

            String response = myResource.request(MediaType.TEXT_PLAIN).post(Entity.entity(topic, MediaType.TEXT_PLAIN), String.class);

            System.out.println(response);

            for (int i = 0; i < numMessages; i++) {
                WebTarget messageResource = client.target("http://" + ClientConstants.HOST + ":8080/KafkaEnterpriseApp-web/resources/message");
                messageResource = messageResource.queryParam("publisherId", response);

                String messageRes = messageResource.request(MediaType.TEXT_PLAIN).post(Entity.entity(messages[i % messages.length], MediaType.TEXT_PLAIN), String.class);
//                Thread.sleep(100);
            }

            synk.await();
        } catch (InterruptedException ex) {
            Logger.getLogger(PublisherClientThread.class.getName()).log(Level.SEVERE, null, ex);
        } catch (BrokenBarrierException ex) {
            Logger.getLogger(PublisherClientThread.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
