package com.big.data.flink;

import com.big.data.kafka.unit.flink.KafkaUnitAdmin;
import com.big.data.kafka.unit.flink.KafkaUnit;
import org.apache.commons.io.FileUtils;
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

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;


public class ReadFromKafkaTest {

    public static final Logger log = LoggerFactory.getLogger(ReadFromKafkaTest.class);

    private static final String LOCAL_FILEURI_PREFIX = "file://";
    private static final String NEW_LINE_DELIMETER = "\n";
    public static final String TOPIC = "TOPIC";
    private static KafkaUnitAdmin admin;
    private static String baseDir;
    private static String outputDir;

    @ClassRule
    public static KafkaUnit cluster = new KafkaUnit(1);
    // KakaUnit(1)  number of Broker in the cluster

    public class MyThread extends Thread {

        public void run() {
            System.out.println("MyThread running");
            //--topic test --bootstrap.servers localhost:9092 --zookeeper.connect localhost:2181 --group.id myGroup
            String[] args = new String[]{"--topic", TOPIC, "--bootstrap.servers", cluster.getConfig().getKafkaBrokerString(), "--zookeeper.connect",
                    cluster.getConfig().getZkString(), "--group.id", "myGroup", "--auto.offset.reset", "earliest", "--" + ReadFromKafka.OUTPUT_PATH,
                    LOCAL_FILEURI_PREFIX + outputDir, "--" + WordCount.PARALLELISM, "1"};
            try {
                ReadFromKafka.main(args);
            } catch (Exception e) {
                log.info("Execption occured while launching Flink Kafka consumer");
            }

        }
    }

    @Before
    public void startup() throws Exception {

        //  create topic in embedded Kafka Cluster
        admin = new KafkaUnitAdmin(cluster);
        admin.createTopic(TOPIC, 1, 1, new Properties());

        //Input Directory
        baseDir = "/tmp/mapreduce/wordcount/" + UUID.randomUUID().toString();

        //OutPutDirectory
        outputDir = baseDir + "/output";

        // produce data in Embedded Kafka (Kafka Unit)
        producerDatainKafka();
    }

    public static void producerDatainKafka() {
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
            String msg = nEvents + "," + ip;
            ProducerRecord<String, String> data = new ProducerRecord<>(TOPIC, ip, msg);
            producer.send(data);
            producer.flush();
        }
        producer.close();
    }

    @Test
    public void readFromKafkaStoreInHdfs() throws Exception {

        log.info("OutPutDir is {} ", outputDir);

        // Starting Flink from a different Thread , elese it will block as it will keep waiting for messaged in Kafka
        MyThread flinkThread = new MyThread();
        flinkThread.start();

        // Sleeping for 10 seconds so that Flink can comsume message and write in outputDir
        Thread.sleep(10000);

        StringBuilder fileToString = new StringBuilder();

        //Read the data from the outputfile folder , OutPutFIle Folder has multiple output files named as 1 2 3 4
        Iterator<File> files = FileUtils.iterateFiles(new File(outputDir), null, true);
        while (files.hasNext()) {
            fileToString.append(FileUtils.readFileToString(files.next(), "UTF-8"));

        }

        Map<String, String> wordToCount = new HashMap<>();

        //4 lines in output file, with one word per line
        Arrays.stream(fileToString.toString().split(NEW_LINE_DELIMETER)).forEach(e -> {
            String[] wordCount = e.split(",");
            wordToCount.put(wordCount[0], wordCount[1]);
            log.info("Event number {}   => ip {}", wordCount[0], wordCount[1]);
        });

        //10 meesaged to consume from kafka
        Assert.assertEquals(10, wordToCount.size());
        Assert.assertTrue(wordToCount.containsKey("0"));
        Assert.assertTrue(wordToCount.get("1").equals("192.168.2.1"));
        Assert.assertTrue(wordToCount.get("0").equals("192.168.2.0"));
        Assert.assertTrue(wordToCount.get("2").equals("192.168.2.2"));
        Assert.assertTrue(wordToCount.get("3").equals("192.168.2.3"));
        Assert.assertTrue(wordToCount.get("4").equals("192.168.2.4"));
        Assert.assertTrue(wordToCount.get("5").equals("192.168.2.5"));

    }

    @After
    public void deleteTopic() {
        admin.deleteTopic(TOPIC);
    }

}
