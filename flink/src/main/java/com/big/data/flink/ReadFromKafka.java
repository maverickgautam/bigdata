package com.big.data.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

/**
 * Simple example on how to read with a Kafka consumer
 *
 * Note that the Kafka source is expecting the following parameters to be set
 *  - "bootstrap.servers" (comma separated list of kafka brokers)
 *  - "zookeeper.connect" (comma separated list of zookeeper servers)
 *  - "group.id" the id of the consumer group
 *  - "topic" the name of the topic to read data from.
 *
 * You can pass these required parameters using "--bootstrap.servers host:port,host1:port1 --zookeeper.connect host:port --topic testTopic"
 *
 * This is a valid input example:
 * 		--topic test --bootstrap.servers localhost:9092 --zookeeper.connect localhost:2181 --group.id myGroup
 *
 * Read from Kafka And write to HDFS.
 *
 * Version info
 * https://ci.apache.org/projects/flink/flink-docs-release-1.2/dev/connectors/kafka.html
 */
public class ReadFromKafka {
    public static final String OUTPUT_PATH = "output.path";
    public static final String PARALLELISM = "parallelism";
    public static final String TOPIC_NAME  = "topic";



    public static void main(String[] args) throws Exception {

        // create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        // Using the parser provided by Flink
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        //parameterTool.getProperties() returns back props with all key=value field set
        DataStream<String> messageStream = env.addSource(new FlinkKafkaConsumer09<String>(parameterTool.getRequired(TOPIC_NAME), new SimpleStringSchema(), parameterTool.getProperties()));

        // the rebelance call is causing a repartitioning of the data so that all machines
        // see the messages (for example in cases when "num kafka partitions" < "num flink operators"
        messageStream.rebalance().map(new MapFunction<String, String>() {
            private static final long serialVersionUID = -6867736771747690202L;

            @Override
            public String map(String value) throws Exception {
                return value;
            }
        })
                     .setParallelism(parameterTool.getInt(PARALLELISM))
                      //Write to hdfs
                     .writeAsText(parameterTool.get(OUTPUT_PATH));

        env.execute();

    }
}