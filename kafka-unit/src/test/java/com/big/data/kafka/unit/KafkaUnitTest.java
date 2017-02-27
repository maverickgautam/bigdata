package com.big.data.kafka.unit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
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
    public static KafkaUnit cluster = new KafkaUnit(1);
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

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getConfig().getKafkaBrokerString());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 2);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");


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

        KafkaGenericConsumer consumer = new KafkaGenericConsumer(cluster.getConfig().getZkString(), "1", TOPIC);
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
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getConfig().getKafkaBrokerString());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
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
