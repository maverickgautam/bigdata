package com.big.data.flink;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;
import org.apache.commons.io.IOUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

/**
 * Simple example on how to read with a Kafka consumer
 * <p>
 * Note that the Kafka source is expecting the following parameters to be set
 * - "bootstrap.servers" (comma separated list of kafka brokers)
 * - "zookeeper.connect" (comma separated list of zookeeper servers)
 * - "group.id" the id of the consumer group
 * - "topic" the name of the topic to read data from.
 * <p>
 * You can pass these required parameters using "--bootstrap.servers host:port,host1:port1 --zookeeper.connect host:port --topic testTopic"
 * <p>
 * This is a valid input example:
 * --topic test --bootstrap.servers localhost:9092 --zookeeper.connect localhost:2181 --group.id myGroup
 * <p>
 * Read from Kafka And write to HDFS.
 * <p>
 * Version info
 * https://ci.apache.org/projects/flink/flink-docs-release-1.2/dev/connectors/kafka.html
 */

public class ReadFromKafkaWriteToAerospike {

    public static final String OUTPUT_PATH = "output.path";
    public static final String PARALLELISM = "parallelism";
    public static final String TOPIC_NAME = "topic";

    //Aerospike related properties
    public static final String AEROSPIKE_HOSTNAME = "aerospike.host.name";
    public static final String AEROSPIKE_PORT = "aerospike.port";
    public static final String AEROSPIKE_NAMESPACE = "aerospike.name.space";
    public static final String AEROSPIKE_SETNAME = "aerospike.set.name";
    public static final String VALUE_BIN_NAME = "value.bin.name";

    // Do remember all the lambda function are instantiated on driver, serialized and sent to driver.
    // No need to initialize the Service(Aerospike , Hbase on driver ) hence making it transiet
    // In the map , for each record insert into Aerospike , this can be coverted into batch too
    public static class InsertIntoAerospike implements MapFunction<String, String> {

        // not making it static , as it will not be serialized and sent to executors
        private final String aerospikeHostName;
        private final int aerospikePortNo;
        private final String aerospikeNamespace;
        private final String aerospikeSetName;
        private final String valueColumnName;

        // The Aerospike client is not serializable and neither there is a need to instatiate on driver
        private transient AerospikeClient client;
        private transient WritePolicy policy;

        public InsertIntoAerospike(String hostName, int portNo, String nameSpace, String setName, String valueColumnName) {
            this.aerospikeHostName = hostName;
            this.aerospikePortNo = portNo;
            this.aerospikeNamespace = nameSpace;
            this.aerospikeSetName = setName;
            this.valueColumnName = valueColumnName;

            //Add Shutdown hook to close the client gracefully
            //This is the place where u can gracefully clean your Service resources as there is no cleanup() function in Spark Map
            JVMShutdownHook jvmShutdownHook = new JVMShutdownHook();
            Runtime.getRuntime().addShutdownHook(jvmShutdownHook);

        }

        @Override
        public String map(String value) throws Exception {
            // Intitialize on the first call
            if (client == null) {
                policy = new WritePolicy();
                // how to close the client gracefully ?
                client = new AerospikeClient(aerospikeHostName, aerospikePortNo);
            }

            String[] keyValue = value.split(",");
            // As rows have schema with fieldName and Values being part of the Row
            Key key = new Key(aerospikeNamespace, aerospikeSetName, keyValue[0]);
            Bin bin = new Bin(valueColumnName, keyValue[1]);
            client.put(policy, key, bin);

            return value;
        }

        //When JVM is going down close the client
        private class JVMShutdownHook extends Thread {
            @Override
            public void run() {
                System.out.println("JVM Shutdown Hook: Thread initiated , shutting down service gracefully");
                IOUtils.closeQuietly(client);
            }
        }
    }

    public static void main(String[] args) throws Exception {

        // create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Using the parser provided by Flink
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        String aerospikeHostname = parameterTool.getRequired(AEROSPIKE_HOSTNAME);
        int aerospikePort = Integer.parseInt(parameterTool.getRequired(AEROSPIKE_PORT));
        String namespace = parameterTool.getRequired(AEROSPIKE_NAMESPACE);
        String setName = parameterTool.getRequired(AEROSPIKE_SETNAME);

        String valueBinName = parameterTool.getRequired(VALUE_BIN_NAME);

        //parameterTool.getProperties() returns back props with all key=value field set
        DataStream<String> messageStream = env.addSource(new FlinkKafkaConsumer09<String>(parameterTool.getRequired(TOPIC_NAME), new
                SimpleStringSchema(), parameterTool.getProperties()));

        // the rebelance call is causing a repartitioning of the data so that all machines
        // see the messages (for example in cases when "num kafka partitions" < "num flink operators"
        messageStream.rebalance().map(new InsertIntoAerospike(aerospikeHostname, aerospikePort, namespace, setName, valueBinName))
                     .setParallelism(parameterTool.getInt(PARALLELISM))
                     //Write to hdfs
                     .writeAsText(parameterTool.get(OUTPUT_PATH));

        env.execute();

    }

}
