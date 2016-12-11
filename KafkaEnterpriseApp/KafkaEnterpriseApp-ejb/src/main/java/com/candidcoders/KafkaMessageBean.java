/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.candidcoders;

import java.util.Properties;
import javax.ejb.Stateless;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 *
 * @author nirav
 */
@Stateless
public class KafkaMessageBean implements KafkaMessageBeanRemote {

    // Add business logic below. (Right-click in editor and choose
    // "Insert Code > Add Business Method")
    @Override
    public void sendMessage() {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        System.out.println("Sending bean messages");
        try (Producer<String, String> producer = new KafkaProducer<String, String>(props)) {
            for (int i = 0; i < 100; i++) {
                producer.send(new ProducerRecord<>("Sports", Integer.toString(i), Integer.toString(i)));
            }
        }
    }

}
