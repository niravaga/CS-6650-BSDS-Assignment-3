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
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import javax.ejb.EJB;
import javax.ejb.Stateless;

import javax.ws.rs.Path;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Produces;
import javax.ws.rs.Consumes;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import static restfulAPI.KafkaPublisherResource.dynamoDB;

@Stateless
@Path("/message")
public class KafkaMessageResource {

    static AmazonDynamoDBClient dynamoDB;
//    @EJB
//    private com.candidcoders.KafkaMessageBeanRemote kafkaMessageBean;

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
        System.out.println("Sending from web");

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
//            for (int i = 0; i < 100; i++) {
////                producer.send(new ProducerRecord<>("Sports", Integer.toString(i), Integer.toString(i)));
//            }

            String topic = getPublisherTopic(publisherId);

            if (topic != null && !topic.isEmpty()) {
                ProducerRecord<String, String> record = new ProducerRecord<>(getPublisherTopic(publisherId), message);

                producer.send(record);
            } else {
                System.err.println("No Topic found for given publisher ID");
            }
        }

        return "Success";
    }

    private String getPublisherTopic(String publisherId) {
        System.out.println("Getting publisher topic for id: " + publisherId);

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

        String tableName = "publisher";
        try {
//            Map<String, AttributeValue> key = new HashMap<>();
            DynamoDB db = new DynamoDB(dynamoDB);
            Table table = db.getTable(tableName);
            Item item = table.getItem("id", publisherId);
            System.out.println("Item " + item);
            String topic = item.asMap().get("topic").toString();
            System.out.println("Topic: " + topic);
            return topic;

//            key.put("id", new AttributeValue(Integer.toString(publisherId)));
//            GetItemResult getItemResult = dynamoDB.getItem(tableName, key);
//            System.err.println(getItemResult.getItem().get("topic"));
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

}
