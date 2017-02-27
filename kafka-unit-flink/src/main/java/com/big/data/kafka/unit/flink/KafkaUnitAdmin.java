package com.big.data.kafka.unit.flink;

import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.serialize.ZkSerializer;

import java.util.Properties;

/**
 * Created by kunalgautam on 17.02.17.
 */
public class KafkaUnitAdmin {

    public static final int TICK_TIME = 5000;
    public static final int SESSION_TIMEOUT = 60000;
    public static final int WAIT_TIME = 5000;
    private ZkClient zkClient;
    private ZkUtils zkUtils ;

    private static final ZkSerializer zkSerializer = new ZkSerializer() {

        @Override
        public byte[] serialize(Object data)  {
            return ZKStringSerializer.serialize(data);
        }

        @Override
        public Object deserialize(byte[] bytes) {
            return ZKStringSerializer.deserialize(bytes);
        }
    };




    public KafkaUnitAdmin(KafkaUnit unit) throws Exception {
        zkClient = new ZkClient(unit.getConfig().getZkString(), SESSION_TIMEOUT, WAIT_TIME, zkSerializer);
        zkUtils = new ZkUtils(zkClient, new ZkConnection(unit.getConfig().getZkString()), false);

//        ZooKeeper zooKeeper = new ZooKeeper(unit.getConfig().getZkString(), 10000, null);
//       if(zooKeeper.getChildren("/brokers/ids", false).size() != 1){
           // get clustersize
//           System.exit(0);
//       }


    }

    public void createTopic(String topicName,int noOfPartitions, int noOfReplication, Properties topicConfiguration  ){
        AdminUtils.createTopic(zkUtils,topicName, noOfPartitions, noOfReplication, topicConfiguration);
    }

    public void deleteTopic(String topicName) {
        AdminUtils.deleteTopic(zkUtils,topicName);
    }



}
