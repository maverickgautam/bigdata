package com.big.data.kafka.unit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;



public class KafkaUnitTest {

    public static final Logger log = LoggerFactory.getLogger(KafkaUnitTest.class);

    public static final String TOPIC = "TOPIC";
    private KafkaUnitAdmin admin;

    @ClassRule
    public static KakaUnit cluster = new KakaUnit(1);
    // KakaUnit(1)  number of Broker in the cluster


    @Before
    public void testKafkaUnit() throws Throwable {
        admin = new KafkaUnitAdmin(cluster);
        admin.createTopic(TOPIC, 1, 1, new Properties());
    }

    public void producerTest() {
        long events = 10;
        Properties props = new Properties();
        log.info("Broker list is : " + cluster.getConfig().getKafkaBrokerString());

        props.put("bootstrap.servers", cluster.getConfig().getKafkaBrokerString());
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 2);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        for (long nEvents = 0; nEvents < events; nEvents++) {
            String ip = "192.168.2." + nEvents;
            String msg = "www.example.com," + ip;
            ProducerRecord<String, String> data = new ProducerRecord<>(TOPIC, ip, msg);
            producer.send(data);
            producer.flush();
        }
        producer.close();
    }

    @Test
    public void consumerTest() {
        producerTest();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException ie) {
        }

        kafkaGenericConsumer consumer = new kafkaGenericConsumer(cluster.getConfig().getZkString(), "1", TOPIC);
        consumer.run(1);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ie) {
        }
        Assert.assertEquals(consumer.getResultQueue().size(), 10);
        Assert.assertEquals(consumer.getResultQueue().contains("www.example.com,192.168.2.0"), true);
        Assert.assertEquals(consumer.getResultQueue().contains("www.example.com,192.168.2.9"), true);
        Assert.assertEquals(consumer.getResultQueue().contains("www.example.com,192.168.2.5"), true);
        consumer.shutdown();
    }

    //@Test
    public void genericConsumerTest() {
        producerTest();

        Properties props = new Properties();
        props.put("bootstrap.servers", cluster.getConfig().getKafkaBrokerString());
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Arrays.asList(TOPIC));
        while (true) {
            producerTest();
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("OUTPUT = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
        }

//        Assert.assertEquals(consumer.r.size(), 10);
//        Assert.assertEquals(consumer.resultQueue().contains("www.example.com,192.168.2.0"), true);
//        Assert.assertEquals(consumer.resultQueue().contains("www.example.com,192.168.2.9"), true);
//        Assert.assertEquals(consumer.resultQueue().contains("www.example.com,192.168.2.5"), true);
//        consumer.close();
    }

    @After
    public void deleteTopic() {
        admin.deleteTopic(TOPIC);
    }

}
