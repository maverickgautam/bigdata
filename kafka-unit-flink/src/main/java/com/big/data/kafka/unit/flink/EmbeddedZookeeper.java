package com.big.data.kafka.unit.flink;

import org.apache.curator.test.TestingServer;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.UUID;

public class EmbeddedZookeeper extends ExternalResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedZookeeper.class);
    private TestingServer zkServer;
    private EmbeddedZookeeperConfig config;
    private String zookeperPath;

    @Override
    protected void before() throws Throwable {
        config = new EmbeddedZookeeperConfig();
        // Random free port being used

        zookeperPath = "/tmp/EmbeddedZookeeper/"+UUID.randomUUID().toString();
        File file = new File(zookeperPath);
        file.deleteOnExit();
        zkServer = new TestingServer(config.zookeperPort, file);
    }

    @Override
    protected void after() {
        if (null != zkServer) {
            try {
                zkServer.stop();
            } catch (IOException e) {
                LOGGER.info("Error while Zookeeper Shutdown ", e);
            }
        }

    }

    public EmbeddedZookeeperConfig getConfig() {
        return config;
    }

    /**
     * configuration for zookeper.
     */
    public class EmbeddedZookeeperConfig {

        private int zookeperPort;
        private String zkstring;

        public EmbeddedZookeeperConfig() {
            zookeperPort = FreeRandomPort.generateRandomPort();
            zkstring = InetAddress.getLoopbackAddress().getHostAddress() + ":" + zookeperPort;
        }

        public int getZookeperPort() {
            return zookeperPort;
        }

        public String getZkstring() {
            return zkstring;
        }

    }

}
