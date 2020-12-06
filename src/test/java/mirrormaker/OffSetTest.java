package mirrormaker;

import java.time.Duration;
import java.util.*;
import java.util.regex.Pattern;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.junit.Assert;
import org.junit.Test;

import static java.util.Arrays.asList;

public class OffSetTest {

    @Test
    public void testCreateTopic() throws InterruptedException {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");

        AdminClient admin = AdminClient.create(config);

        Map<String, String> configs = new HashMap<>();
        int partitions = 1;
        short replication = 1;

        admin.createTopics(Collections.singletonList(new NewTopic("rmirrored", partitions, replication).configs(configs)));
    }

    @Test
    public void testFailOver() throws InterruptedException {
        //Assign topicName to string variable
        //UUID uuid = UUID.randomUUID();
        String groupName = UUID.randomUUID().toString();
        groupName = "3770225a-85f0-475f-ad45-93c485812bfc";
        //String topicName = "topic-" + uuid;
        String topicName = "topic-7";
        System.out.println("Group:" + groupName);
        System.out.println("Topic:" + topicName);
        Producer<String, String> producer = getProducer("localhost:29093",false);
        for(int i = 0; i < 5; i++)
            producer.send(new ProducerRecord<String, String>(topicName,
                    Integer.toString(i), Integer.toString(i)));
        System.out.println("Message sent successfully");
        producer.close();

        //Sleep for a while to ensure new topic is replicated


        KafkaConsumer<String, String> consumer = getConsumer("localhost:29093",groupName);
        //consumer.subscribe(Pattern.compile(".*"+uuid));
        consumer.subscribe(Pattern.compile(".*"+topicName));

        int i = 0;
        int sum = 0;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                i++;
                sum += Integer.parseInt(record.value());
            }
            if(i >= 5 && records.isEmpty())
                break;
        }
        System.out.println("Consumed " + i +" messages");
        consumer.close();

        //Assert.assertEquals(10,sum);

        //Thread.sleep(10000);

        producer = getProducer("localhost:29093",true);

        producer.initTransactions();
        producer.beginTransaction();
        producer.send(new ProducerRecord<String, String>(topicName,"100","100"));
        producer.abortTransaction();
        producer.close();

        producer = getProducer("localhost:29093",false);
        for(i = 5; i < 10; i++)
            producer.send(new ProducerRecord<String, String>(topicName,
                    Integer.toString(i), Integer.toString(i)));
        System.out.println("Message sent successfully");
        producer.close();

        Thread.sleep(20000);

        KafkaConsumer<String, String> targetConsumer = getConsumer("localhost:29092",groupName);
        //targetConsumer.subscribe(Pattern.compile(".*"+uuid));
        targetConsumer.subscribe(Pattern.compile(".*"+topicName));


        i = 0;
        sum = 0;
        while (i<5) {
            ConsumerRecords<String, String> records = targetConsumer.poll(Duration.ofMillis(100));
            if(records.isEmpty())
                System.out.println("Empty Poll");
            for (ConsumerRecord<String, String> record : records) {
                i++;
                sum += Integer.parseInt(record.value());
                System.out.println(sum);
            }
        }
        System.out.println("Consumed 5 messages");
        targetConsumer.close();

        Assert.assertEquals(35,sum);
    }

    @Test
    public void testFailBack() throws InterruptedException {
        //Assign topicName to string variable
        //UUID uuid = UUID.randomUUID();
        String groupName = UUID.randomUUID().toString();
        groupName = "3770225a-85f0-475f-ad45-93c485812bfc";
        //String topicName = "topic-" + uuid;
        String topicName = "topic-6";
        System.out.println("Group:" + groupName);
        System.out.println("Topic:" + topicName);
        Producer<String, String> producer = getProducer("localhost:29093",false);
        for(int i = 0; i < 4; i++)
            producer.send(new ProducerRecord<String, String>(topicName,
                    Integer.toString(i), Integer.toString(i)));
        System.out.println("Message sent successfully");
        producer.close();

        //Sleep for a while to ensure new topic is replicated


        KafkaConsumer<String, String> consumer = getConsumer("localhost:29092",groupName);
        //consumer.subscribe(Pattern.compile(".*"+uuid));
        consumer.subscribe(Pattern.compile(".*"+topicName));

        int i = 0;
        int sum = 0;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            if(records.isEmpty())
                System.out.println("Empty Poll " + i);
            for (ConsumerRecord<String, String> record : records) {
                i++;
                sum += Integer.parseInt(record.value());
            }
            if(i >= 4 && records.isEmpty())
                break;
        }
        System.out.println("Consumed " + i +" messages");
        consumer.close();

        //Assert.assertEquals(10,sum);

        //Thread.sleep(10000);

        producer = getProducer("localhost:29093",true);

        producer.initTransactions();
        producer.beginTransaction();
        producer.send(new ProducerRecord<String, String>(topicName,"100","100"));
        producer.abortTransaction();
        producer.close();

        producer = getProducer("localhost:29093",false);
        for(i = 5; i < 11; i++)
            producer.send(new ProducerRecord<String, String>(topicName,
                    Integer.toString(i), Integer.toString(i)));
        System.out.println("Message sent successfully");
        producer.close();

        Thread.sleep(20000);

        KafkaConsumer<String, String> targetConsumer = getConsumer("localhost:29093",groupName);
        //targetConsumer.subscribe(Pattern.compile(".*"+uuid));
        targetConsumer.subscribe(Pattern.compile(".*"+topicName));


        i = 0;
        sum = 0;
        while (true) {
            ConsumerRecords<String, String> records = targetConsumer.poll(Duration.ofMillis(100));
            if(records.isEmpty())
                System.out.println("Empty Poll");
            for (ConsumerRecord<String, String> record : records) {
                i++;
                sum += Integer.parseInt(record.value());
                System.out.println(sum);
            }
            if(i >= 6 && records.isEmpty())
                break;
        }
        System.out.println("Consumed "+ i +" messages");
        targetConsumer.close();

        Assert.assertEquals(45,sum);
    }


    @Test
    public void testConsume(){
        //Kafka consumer configuration settings
        String topicName = ".*rmirrored4";
        KafkaConsumer<String, String> consumer = getConsumer("localhost:29093","8550e4ce-0fea-4066-a013-2cce72756ed7");

        //Kafka Consumer subscribes list of topics here.
        consumer.subscribe(Pattern.compile(topicName));

        //print the topic name
        System.out.println("Subscribed to topic " + topicName);
        int i = 0;

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records)

                // print the offset,key and value for the consumer records.
                System.out.printf("offset = %d, key = %s, value = %s\n",
                        record.offset(), record.key(), record.value());
        }
    }

    private Producer<String, String> getProducer(String server,Boolean trans){
        Properties props = new Properties();
        props.put("bootstrap.servers", server);
        props.put("acks", "all");
        props.put("retries", 1);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        if(trans)
            props.put("transactional.id", "tran");


        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer
                <String, String>(props);
    }

    private KafkaConsumer<String, String> getConsumer(String server, String consumerGroup) {
        Properties props = new Properties();

        props.put("bootstrap.servers", server);
        props.put("group.id", consumerGroup);
        props.put("enable.auto.commit", "true");
        props.put("auto.offset.reset", "earliest");

        props.put("auto.commit.interval.ms", "500");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        return new KafkaConsumer
                <String, String>(props);
    }
}
