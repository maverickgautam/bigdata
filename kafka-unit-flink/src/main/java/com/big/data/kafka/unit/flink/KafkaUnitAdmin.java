package com.big.data.kafka.unit.flink;

import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

import java.util.Properties;

/**
 * Created by kunalgautam on 17.02.17.
 */
public class KafkaUnitAdmin {

    public static final int tickTime = 5000;
    public static final int sessionTimeout = 60000;
    public static final int waitTime = 5000;
    private ZkClient zkClient;
    private ZkUtils zkUtils ;

    private static final ZkSerializer zkSerializer = new ZkSerializer() {

        public byte[] serialize(Object data) throws ZkMarshallingError {
            return ZKStringSerializer.serialize(data);
        }

        public Object deserialize(byte[] bytes) throws ZkMarshallingError {
            return ZKStringSerializer.deserialize(bytes);
        }
    };




    public KafkaUnitAdmin(KakaUnit unit) throws Exception {
        zkClient = new ZkClient(unit.getConfig().getZkString(), sessionTimeout, waitTime, zkSerializer);
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
