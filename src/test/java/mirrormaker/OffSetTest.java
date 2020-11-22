package mirrormaker;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.junit.Assert;
import org.junit.Test;

public class OffSetTest {


    @Test
    public void testActivePassive() throws InterruptedException {
        //Assign topicName to string variable
        UUID uuid = UUID.randomUUID();
        String topicName = "topic-" + uuid;
        System.out.println("Topic:" + topicName);
        Producer<String, String> producer = getProducer(false);
        for(int i = 0; i < 5; i++)
            producer.send(new ProducerRecord<String, String>(topicName,
                    Integer.toString(i), Integer.toString(i)));
        System.out.println("Message sent successfully");
        producer.close();

        //Sleep for a while to ensure new topic is replicated


        KafkaConsumer<String, String> consumer = getConsumer("localhost:29092");
        consumer.subscribe(Pattern.compile(".*"+uuid));

        int i = 0;
        int sum = 0;
        while (i<5) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                i++;
                sum += Integer.parseInt(record.value());
            }
        }
        System.out.println("Consumed 5 messages");
        consumer.close();

        Assert.assertEquals(10,sum);

        Thread.sleep(60000);

        producer = getProducer(true);

        producer.initTransactions();
        producer.beginTransaction();
        producer.send(new ProducerRecord<String, String>(topicName,"100","100"));
        producer.abortTransaction();
        producer.close();

        producer = getProducer(false);
        for(i = 5; i < 10; i++)
            producer.send(new ProducerRecord<String, String>(topicName,
                    Integer.toString(i), Integer.toString(i)));
        System.out.println("Message sent successfully");
        producer.close();

        Thread.sleep(10000);

        KafkaConsumer<String, String> targetConsumer = getConsumer("localhost:29093");
        targetConsumer.subscribe(Pattern.compile(".*"+uuid));

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
    public void testConsume(){
        //Kafka consumer configuration settings
        String topicName = ".*topic-18e39925-4004-447b-bf68-02e2898918a6";
        KafkaConsumer<String, String> consumer = getConsumer("localhost:29092");

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

    private Producer<String, String> getProducer(Boolean trans){
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:29092");
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

    private KafkaConsumer<String, String> getConsumer(String server) {
        Properties props = new Properties();

        props.put("bootstrap.servers", server);
        props.put("group.id", "test");
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
