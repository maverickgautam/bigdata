package com.big.data.mapreduce.hbaseio;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

public class HbaseFetchTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(HbaseFetchMapper.class);

    //Embedded Hbase + hdfs
    private static HBaseTestingUtility hbaseEmbeddedClusterHandler;
    private static FileSystem fs;
    private static final String BASEDIR = "/tmp/embeddedHbase/";
    private static final String HDFS_INPUT_FOLDER = BASEDIR + "/input/" + System.currentTimeMillis() + "/";
    private static final String HDFS_OUTPUT_FOLDER = BASEDIR + "/output/"+System.currentTimeMillis() + "/";
    private static String tableName;
    private static String columnFamilyName;
    private static Configuration hbaseConf;

    @BeforeClass
    public static void setUp() throws Exception {

        Properties prop = new Properties();
        try (InputStream input = ClassLoader.getSystemResourceAsStream(Constant.CONFIG_FILE_NAME)) {
            prop.load(input);
        }

        tableName = prop.getProperty(Constant.HBASE_TABLE_NAME);
        columnFamilyName = prop.getProperty(Constant.HBASE_TABLE_CF_NAME);
        // init embedded hbase

        //Instantiate hbase minicluster
        hbaseEmbeddedClusterHandler = new HBaseTestingUtility();

        // Start Hbase minicluster with Number of region server = 1
        hbaseEmbeddedClusterHandler.startMiniCluster(1);

        fs = hbaseEmbeddedClusterHandler.getTestFileSystem();
        hbaseConf = hbaseEmbeddedClusterHandler.getConfiguration();

        // create HDFS_TEST_INPUT_FOLDER
        fs.mkdirs(new Path(HDFS_INPUT_FOLDER));
    }

    /**
     * To Create table in Hbase
     *
     * @throws IOException
     */
    @Before
    public void createTable() throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(hbaseConf);
             Admin admin = connection.getAdmin()) {

            HTableDescriptor table =
                    new HTableDescriptor(TableName.valueOf(tableName));
            table.addFamily(new HColumnDescriptor(columnFamilyName));

            if (!admin.tableExists(table.getTableName())) {
                LOGGER.info("Creating Table {} ", tableName);
                admin.createTable(table);
            }
        }

    }

    /**
     * To delete Table in Hbase
     *
     * @throws IOException
     */
    @After
    public void deleteTable() throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(hbaseConf); Admin admin = connection.getAdmin()) {

            HTableDescriptor table =
                    new HTableDescriptor(TableName.valueOf(tableName));
            table.addFamily(new HColumnDescriptor(columnFamilyName));

            if (!admin.tableExists(table.getTableName())) {
                LOGGER.info("Disabling And Deleting Table {} ", tableName);
                admin.disableTable(table.getTableName());
                admin.deleteTable(table.getTableName());
            }
        }

    }

    private void putIntoHbase(Table table, String columnFamily, String columnName, String key, String value) throws
            IOException {
        Put put = new Put(key.getBytes());
        put.addColumn(columnFamily.getBytes(), columnName.getBytes(), value.getBytes());
        table.put(put);
    }

    private void createTestDataInHbaseAndHdfs() throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(hbaseConf); Table table = connection.getTable(TableName.valueOf(tableName))) {

            //Only col1, col2, col3 will be fetched as provided in hbase.config.properties in test/resources
            //Batch size is 3  hence inserting 4 keys
            //Key k1 with one good  values
            putIntoHbase(table, columnFamilyName, "col1", "k1", "k1v1");

            //col4 key not to be considered for fetching
            putIntoHbase(table, columnFamilyName, "col4", "k1", "k1v2");

            //Key k2 with two values
            putIntoHbase(table, columnFamilyName, "col1", "k2", "k2v1");
            putIntoHbase(table, columnFamilyName, "col2", "k2", "k2v2");

            //key k3 with three values
            putIntoHbase(table, columnFamilyName, "col1", "k3", "k3v1");
            putIntoHbase(table, columnFamilyName, "col2", "k3", "k3v2");
            putIntoHbase(table, columnFamilyName, "col3", "k3", "k3v3");

            //col4 key not to be considered for fetching
            putIntoHbase(table, columnFamilyName, "col4", "k3", "k3v4");

            //Key k4 with unknown columnName
            putIntoHbase(table, columnFamilyName, "unknown", "k4", "v2");

            //Writing all the keys in Hdfs , input to the Job
            FSDataOutputStream out = fs.create(new Path(HDFS_INPUT_FOLDER, "part000"));
            out.write("k1\n".getBytes());
            out.write("k2\n".getBytes());
            out.write("k3\n".getBytes());
            out.write("k4\n".getBytes());
            //not exsistent key
            out.write("k5\n".getBytes());
            out.close();
        }
    }

    @Test
    public void testWithValidFiles() throws Exception {
        createTestDataInHbaseAndHdfs();

        Configuration conf = new Configuration();

        //mandatory param to access Hdfs which is being retrieved from the minicluster started
        conf.set("fs.defaultFS", hbaseConf.get("fs.defaultFS"));
        //mandatory parameters to access hbase , which is being retrieved from minicluster started
        conf.set("hbase.zookeeper.quorum", hbaseConf.get("hbase.zookeeper.quorum"));
        conf.set("hbase.zookeeper.property.clientPort", hbaseConf.get("hbase.zookeeper.property.clientPort"));
        conf.set(Constant.JOB_NAME, "MY_JOB");
        conf.set(Constant.INPUT_PATH, HDFS_INPUT_FOLDER);
        conf.set(Constant.OUTPUT_PATH, HDFS_OUTPUT_FOLDER);
        HbaseFetchDriver test = new HbaseFetchDriver();
        test.setConf(conf);
        test.run(null);

        Multimap<String, Object> myMultimap = ArrayListMultimap.create();
        //Read data from outPut path in HDFS
        BufferedReader bfr = new BufferedReader(new InputStreamReader(fs.open(new Path(HDFS_OUTPUT_FOLDER, "part-m-00000")
        )));
        String str = null;
        while ((str = bfr.readLine()) != null) {
            String[] keyValye = str.split("\t");
            myMultimap.put(keyValye[0], keyValye[1]);

        }
        bfr.close();

        Assert.assertEquals(1, myMultimap.get("k1").size());
        Assert.assertEquals(true, myMultimap.get("k1").contains("k1v1"));
        Assert.assertEquals(false, myMultimap.get("k1").contains("k1v2"));

        Assert.assertEquals(2, myMultimap.get("k2").size());
        Assert.assertEquals(true, myMultimap.get("k2").contains("k2v1"));
        Assert.assertEquals(true, myMultimap.get("k2").contains("k2v2"));

        Assert.assertEquals(3, myMultimap.get("k3").size());
        Assert.assertEquals(true, myMultimap.get("k3").contains("k3v1"));
        Assert.assertEquals(true, myMultimap.get("k3").contains("k3v2"));
        Assert.assertEquals(true, myMultimap.get("k3").contains("k3v3"));
        Assert.assertEquals(false, myMultimap.get("k3").contains("k3v4"));

        Assert.assertEquals(true, myMultimap.get("k4").isEmpty());
        Assert.assertEquals(true, myMultimap.get("k5").isEmpty());

    }

    @AfterClass
    public static void shutdown() throws Exception {
        if (hbaseEmbeddedClusterHandler != null) {
            hbaseEmbeddedClusterHandler.shutdownMiniCluster();
        }
        FileUtils.deleteQuietly(new File(BASEDIR));
    }
}
