package com.big.data.spark;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.unit.impls.AerospikeRunTimeConfig;
import com.aerospike.unit.impls.AerospikeSingleNodeCluster;
import com.aerospike.unit.utils.AerospikeUtils;
import com.big.data.avro.AvroUtils;
import com.big.data.avro.schema.Employee;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parquet.avro.AvroParquetReader;
import parquet.hadoop.ParquetReader;

import java.io.File;

/**
 * Created by kunalgautam on 01.03.17.
 */
public class ReadKeysFromHdfsgetValuesfromAerospikeSparkTest {

    private static final Logger LOG = LoggerFactory.getLogger(ReadWriteAvroParquetFilesTest.class);
    private static final String BASEDIR = "/tmp/ReadKeysFromHdfsgetValuesfromAerospikeSparkTest/avroparquetInputFile/" + System.currentTimeMillis()
            + "/";
    private String input;
    private String output;

    private AerospikeSingleNodeCluster cluster;
    private AerospikeRunTimeConfig runtimConfig;
    private AerospikeClient client;

    //Aerospike unit related Params
    private String memorySize = "64M";
    private String setName = "BIGTABLE";
    private String binName = "BIGBIN";
    private String nameSpace = "RandomNameSpace";

    @Before
    public void setUp() throws Exception {

        input = BASEDIR + "input/";
        output = BASEDIR + "output/";


        Configuration conf = new Configuration();
        conf.set("fs.default.fs","file:///");
        FileSystem fs =  FileSystem.get(conf);



        //Writing all the keys in Hdfs , input to the Job
        FSDataOutputStream out = fs.create(new Path(input, "part000"));
        out.write("1\n".getBytes());
        out.write("2\n".getBytes());
        //not exsistent key
        out.close();


        // Start Aerospike MiniCluster
        // Instatiate the cluster with NameSpaceName , memory Size.
        // One can use the default constructor and retieve nameSpace,Memory info from cluster.getRunTimeConfiguration();
        cluster = new AerospikeSingleNodeCluster(nameSpace, memorySize);
        cluster.start();
        // Get the runTime configuration of the cluster
        runtimConfig = cluster.getRunTimeConfiguration();
        client = new AerospikeClient("127.0.0.1", runtimConfig.getServicePort());
        AerospikeUtils.printNodes(client);


        // Insert Values in Aerospike for the appropriate keys

        //Fetch both the keys from Aerospike
        WritePolicy policy = new WritePolicy();
        Key key1 = new Key(runtimConfig.getNameSpaceName(), setName, 1);
        Bin bin1 = new Bin("emp_name", "Maverick1");
        Bin bin2 = new Bin(ReadKeysFromHdfsgetValuesfromAerospikeSpark.EMPLOYEE_COUNTRY_KEY_NAME, "DE");
        client.put(policy, key1,bin1,bin2);

        Key key2 = new Key(runtimConfig.getNameSpaceName(), setName, 2);
        Bin bin3 = new Bin("emp_name", "Maverick2");
        Bin bin4 = new Bin(ReadKeysFromHdfsgetValuesfromAerospikeSpark.EMPLOYEE_COUNTRY_KEY_NAME, "IND");

        client.put(policy, key2,bin3,bin4);


    }

    @Test
    public void testSuccess() throws Exception {

        String[] args = new String[]{"-D" + ReadWriteAvroParquetFiles.INPUT_PATH + "=" + input,
                "-D" + ReadKeysFromHdfsgetValuesfromAerospikeSpark.OUTPUT_PATH + "=" + output,
                "-D" + ReadKeysFromHdfsgetValuesfromAerospikeSpark.IS_RUN_LOCALLY + "=true",
                "-D" + ReadKeysFromHdfsgetValuesfromAerospikeSpark.DEFAULT_FS + "=file:///",
                "-D" + ReadKeysFromHdfsgetValuesfromAerospikeSpark.NUM_PARTITIONS + "=1",
                "-D" + ReadKeysFromHdfsgetValuesfromAerospikeSpark.AEROSPIKE_NAMESPACE + "=" + runtimConfig.getNameSpaceName(),
                "-D" + ReadKeysFromHdfsgetValuesfromAerospikeSpark.AEROSPIKE_HOSTNAME + "=127.0.0.1",
                "-D" + ReadKeysFromHdfsgetValuesfromAerospikeSpark.AEROSPIKE_PORT + "=" + runtimConfig.getServicePort(),
                "-D" + ReadKeysFromHdfsgetValuesfromAerospikeSpark.AEROSPIKE_SETNAME + "=" + setName,
                "-D" + ReadKeysFromHdfsgetValuesfromAerospikeSpark.KEY_NAME + "=emp_id",
                "-D" + ReadKeysFromHdfsgetValuesfromAerospikeSpark.VALUE_NAME + "=emp_name"};

        ReadKeysFromHdfsgetValuesfromAerospikeSpark.main(args);


        // Read from HDFS that the key and values are available
        ParquetReader<GenericRecord> reader = AvroParquetReader.builder(new Path(output)).build();
        //Use .withConf(FS.getConf()) for reading from a diferent HDFS and not local , by default the fs is local
        GenericData.Record event = null;
        while ((event = (GenericData.Record) reader.read()) != null) {
            Employee outputEvent = AvroUtils.convertByteArraytoAvroPojo(AvroUtils.convertAvroPOJOtoByteArray(event, Employee.getClassSchema
                    ()), Employee.getClassSchema());
            LOG.info("Data read from Sparkoutput is {}", outputEvent.toString());
            Assert.assertTrue(outputEvent.getEmpId().equals(1) || outputEvent.getEmpId().equals(2));
            Assert.assertTrue(outputEvent.getEmpName().equals("Maverick1") || outputEvent.getEmpName().equals("Maverick2"));
        }
        reader.close();

    }

    @After
    public void cleanup() throws Exception {
        FileUtils.deleteDirectory(new File(BASEDIR));

        if (cluster != null) {
            // Stop the cluster
            cluster.stop(true);
        }
        if (client != null) {
            client.close();
        }
    }
}
