package restfulAPI;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import javax.ejb.Stateless;

import javax.ws.rs.Path;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Produces;
import javax.ws.rs.Consumes;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import static restfulAPI.KafkaSubscriberResource.dynamoDB;

@Stateless
@Path("/message")
public class KafkaMessageResource {

    private AmazonDynamoDBClient dynamoDB = null;
    static Properties producerProperties = null;
    static Producer<String, String> producer = null;

    /**
     * Retrieves representation of an instance of helloworld.PublisherResource
     *
     * @param publisherId
     * @param message
     * @return an instance of java.lang.String
     */
    @POST
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.TEXT_PLAIN)
    public String addMessage(@QueryParam("publisherId") String publisherId, String message) {

        if (producerProperties == null || producer == null) {
            initProducerProperties();
        }

        String topic = getPublisherTopic(publisherId);

        if (topic != null && !topic.isEmpty()) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);

            producer.send(record);
        } else {
            System.err.println("No Topic found for given publisher ID");
        }

        return "Success";
    }

    private String getPublisherTopic(String publisherId) {
        
        if (dynamoDB == null) {
            initializeDynamoDBCredentials();
        }

        String tableName = "publisher";
        try {
            DynamoDB db = new DynamoDB(dynamoDB);
            Table table = db.getTable(tableName);
            Item item = table.getItem("id", publisherId);

            if (item != null) {
                String topic = item.asMap().get("topic").toString();
//                System.out.println("Topic: " + topic);
                return topic;
            }
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to AWS, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with AWS, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }

        return null;
    }

    @GET
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.TEXT_PLAIN)
    public String getMessage(@QueryParam("subscriberId") String subscriberId) {
        Map<String, Object> subscriberDetails = getSubscriberDetails(subscriberId);

        String subscribedTopic = (String) subscriberDetails.get("topic");

        long offset = Long.parseLong((String) subscriberDetails.get("offset"));

        Properties props = new Properties();
        props.put("bootstrap.servers", BSDSConstants.KAFKA_SERVER);
        props.put("group.id", subscriberId);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("max.poll.records", "1");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        TopicPartition partition = new TopicPartition(subscribedTopic, 0);
        TopicPartition partition1 = new TopicPartition(subscribedTopic, 1);
        TopicPartition partition2 = new TopicPartition(subscribedTopic, 2);
        consumer.assign(Arrays.asList(partition, partition1, partition2));

        if (offset == 0) {
            consumer.seekToBeginning(Arrays.asList(partition, partition1, partition2));
        }

        ConsumerRecords<String, String> records = consumer.poll(1000);

        String message = "";

//        System.out.println("NumRecords: " + records.count());
        for (ConsumerRecord<String, String> record : records) {
            message += record.value();
        }

        if (offset == 0) {
            updateSuscriberDetails(subscriberId, subscribedTopic, "1");
        }
        consumer.commitSync();
        consumer.close();
        return message;
    }

    private Map<String, Object> getSubscriberDetails(String subscriberId) {

        if (dynamoDB == null) {
            initializeDynamoDBCredentials();
        }

        String tableName = "subscriber";
        try {
            DynamoDB db = new DynamoDB(dynamoDB);
            Table table = db.getTable(tableName);
            Item item = table.getItem("id", subscriberId);

            if (item != null) {
                return item.asMap();
            }
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to AWS, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with AWS, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }

        return null;
    }

    private void updateSuscriberDetails(String subscriberId, String topic, String offset) {

        String tableName = "subscriber";
        try {
//            Map<String, AttributeValue> key = new HashMap<>();
            Map<String, AttributeValue> item = newItem(subscriberId, topic, offset);
            PutItemRequest putItemRequest = new PutItemRequest(tableName, item);
            PutItemResult putItemResult = dynamoDB.putItem(putItemRequest);

        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to AWS, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with AWS, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    }

    private Map<String, AttributeValue> newItem(String id, String topic, String offset) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", new AttributeValue(id));
        item.put("topic", new AttributeValue(topic));
        item.put("offset", new AttributeValue(offset));
        return item;
    }
    
    private void initProducerProperties() {
        producerProperties = new Properties();
        producerProperties.put("bootstrap.servers", BSDSConstants.KAFKA_SERVER);
        producerProperties.put("acks", "all");
        producerProperties.put("retries", 0);
        producerProperties.put("batch.size", 16384);
        producerProperties.put("linger.ms", 1);
        producerProperties.put("buffer.memory", 33554432);
        producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(producerProperties);
    }
    
    private void initializeDynamoDBCredentials() {
        AWSCredentials credentials = null;

        try {
            credentials = new ProfileCredentialsProvider("default").getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. "
                    + "Please make sure that your credentials file is at the correct "
                    + "location (C:\\Users\\nirav\\.aws\\credentials), and is in valid format.",
                    e);
        }
        dynamoDB = new AmazonDynamoDBClient(credentials);
        Region usWest2 = Region.getRegion(Regions.US_WEST_2);
        dynamoDB.setRegion(usWest2);
    }
}
