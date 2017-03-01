package com.big.data.flink;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.unit.impls.AerospikeRunTimeConfig;
import com.aerospike.unit.impls.AerospikeSingleNodeCluster;
import com.aerospike.unit.utils.AerospikeUtils;
import com.big.data.kafka.unit.flink.KafkaUnit;
import com.big.data.kafka.unit.flink.KafkaUnitAdmin;
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

/**
 * Created by kunalgautam on 01.03.17.
 */
public class ReadFromKafkaWriteToAerospikeTest {

    public static final Logger log = LoggerFactory.getLogger(ReadFromKafkaTest.class);

    private static final String LOCAL_FILEURI_PREFIX = "file://";
    private static final String NEW_LINE_DELIMETER = "\n";
    public static final String TOPIC = "TOPIC";
    public static final String BIN_NAME = "ip";
    private static KafkaUnitAdmin admin;

    private static String baseDir;
    private static String outputDir;

    private AerospikeSingleNodeCluster aerospikeCluster;
    private AerospikeRunTimeConfig runtimConfig;
    private AerospikeClient client;
    private WritePolicy policy;

    //Aerospike unit related Params
    private String memorySize = "64M";
    private String setName = "BIGTABLE";
    private String binName = "BIGBIN";
    private String nameSpace = "RandomNameSpace";

    @ClassRule
    public static KafkaUnit kafkaUnitCluster = new KafkaUnit(1);
    // KakaUnit(1)  number of Broker in the kafkaUnitCluster

    public class MyThread extends Thread {

        public void run() {
            System.out.println("MyThread running");
            //--topic test --bootstrap.servers localhost:9092 --zookeeper.connect localhost:2181 --group.id myGroup
            String[] args = new String[]{"--topic", TOPIC,
                    "--bootstrap.servers", kafkaUnitCluster.getConfig().getKafkaBrokerString(),
                    "--zookeeper.connect", kafkaUnitCluster.getConfig().getZkString(),
                    "--group.id", "myGroup",
                    "--auto.offset.reset", "earliest",
                    "--" + ReadFromKafka.OUTPUT_PATH, LOCAL_FILEURI_PREFIX + outputDir,
                    "--" + WordCount.PARALLELISM, "1",
                    "--" + ReadFromKafkaWriteToAerospike.AEROSPIKE_NAMESPACE, runtimConfig.getNameSpaceName(),
                    "--" + ReadFromKafkaWriteToAerospike.AEROSPIKE_HOSTNAME, "127.0.0.1",
                    "--" + ReadFromKafkaWriteToAerospike.AEROSPIKE_PORT, String.valueOf(runtimConfig.getServicePort()),
                    "--" + ReadFromKafkaWriteToAerospike.AEROSPIKE_SETNAME, setName,
                    "--" + ReadFromKafkaWriteToAerospike.VALUE_BIN_NAME, BIN_NAME};

            try {
                ReadFromKafkaWriteToAerospike.main(args);
            } catch (Exception e) {
                log.info("Execption occured while launching Flink Kafka consumer");
            }

        }
    }

    @Before
    public void startup() throws Exception {

        //  create topic in embedded Kafka Cluster
        admin = new KafkaUnitAdmin(kafkaUnitCluster);
        admin.createTopic(TOPIC, 1, 1, new Properties());

        //Input Directory
        baseDir = "/tmp/mapreduce/wordcount/" + UUID.randomUUID().toString();

        //OutPutDirectory
        outputDir = baseDir + "/output";

        // Start Aerospike MiniCluster
        // Instatiate the cluster with NameSpaceName , memory Size.
        // One can use the default constructor and retieve nameSpace,Memory info from cluster.getRunTimeConfiguration();
        aerospikeCluster = new AerospikeSingleNodeCluster(nameSpace, memorySize);
        aerospikeCluster.start();
        // Get the runTime configuration of the cluster
        runtimConfig = aerospikeCluster.getRunTimeConfiguration();
        client = new AerospikeClient("127.0.0.1", runtimConfig.getServicePort());
        AerospikeUtils.printNodes(client);
        policy = new WritePolicy();

        // produce data in Embedded Kafka (Kafka Unit)
        producerDatainKafka();
    }

    public static void producerDatainKafka() {
        long events = 10;
        Properties props = new Properties();
        log.info("Broker list is : " + kafkaUnitCluster.getConfig().getKafkaBrokerString());

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUnitCluster.getConfig().getKafkaBrokerString());
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

    private String getValueFromAerospike(String key) {
        //Fetch both the keys from Aerospike
        Key key1 = new Key(runtimConfig.getNameSpaceName(), setName, key);
        Record result1 = client.get(policy, key1);
        return result1.getValue(BIN_NAME).toString();
    }

    @Test
    public void ReadFromKafkaWriteToAerospike() throws Exception {

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

        //Asserts from Aerospike
        Assert.assertEquals("192.168.2.0", getValueFromAerospike("0"));
        Assert.assertEquals("192.168.2.1", getValueFromAerospike("1"));
        Assert.assertEquals("192.168.2.2", getValueFromAerospike("2"));
        Assert.assertEquals("192.168.2.3", getValueFromAerospike("3"));
        Assert.assertEquals("192.168.2.4", getValueFromAerospike("4"));
        Assert.assertEquals("192.168.2.5", getValueFromAerospike("5"));

    }

    @After
    public void deleteTopic() {
        admin.deleteTopic(TOPIC);
    }

}
