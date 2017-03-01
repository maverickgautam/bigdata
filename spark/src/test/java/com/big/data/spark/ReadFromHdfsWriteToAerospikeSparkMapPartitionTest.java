package com.big.data.spark;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.unit.impls.AerospikeRunTimeConfig;
import com.aerospike.unit.impls.AerospikeSingleNodeCluster;
import com.aerospike.unit.utils.AerospikeUtils;
import com.big.data.avro.AvroUtils;
import com.big.data.avro.schema.Employee;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parquet.avro.AvroParquetReader;
import parquet.avro.AvroParquetWriter;
import parquet.hadoop.ParquetReader;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;

import java.io.File;

/**
 * Created by kunalgautam on 01.03.17.
 */
public class ReadFromHdfsWriteToAerospikeSparkMapPartitionTest {
    private static final Logger LOG = LoggerFactory.getLogger(ReadWriteAvroParquetFilesTest.class);
    private static final String BASEDIR = "/tmp/ReadFromHdfsWriteToAerospikeSparkMapPartitionTest/avroparquetInputFile/" + System.currentTimeMillis() + "/";
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

        Employee employee = new Employee();
        employee.setEmpId(1);
        employee.setEmpName("Maverick1");
        employee.setEmpCountry("DE");

        Employee employee1 = new Employee();
        employee1.setEmpId(2);
        employee1.setEmpName("Maverick2");
        employee1.setEmpCountry("DE");

        //Write parquet file2 with GZIP compression
        ParquetWriter<Object> writer = AvroParquetWriter.builder(new Path(input + "1.gz.parquet")).withCompressionCodec
                (CompressionCodecName.GZIP).withSchema(Employee.getClassSchema()).build();
        writer.write(employee);
        writer.write(employee1);
        writer.close();

        // Start Aerospike MiniCluster
        // Instatiate the cluster with NameSpaceName , memory Size.
        // One can use the default constructor and retieve nameSpace,Memory info from cluster.getRunTimeConfiguration();
        cluster = new AerospikeSingleNodeCluster(nameSpace, memorySize);
        cluster.start();
        // Get the runTime configuration of the cluster
        runtimConfig = cluster.getRunTimeConfiguration();
        client = new AerospikeClient("127.0.0.1", runtimConfig.getServicePort());
        AerospikeUtils.printNodes(client);

    }

    @Test
    public void testSuccess() throws Exception {

        String[] args = new String[]{"-D" + ReadWriteAvroParquetFiles.INPUT_PATH + "=" + input,
                "-D" + ReadFromHdfsWriteToAerospikeSparkMapPartition.OUTPUT_PATH + "=" + output,
                "-D" + ReadFromHdfsWriteToAerospikeSparkMapPartition.IS_RUN_LOCALLY + "=true",
                "-D" + ReadFromHdfsWriteToAerospikeSparkMapPartition.DEFAULT_FS + "=file:///",
                "-D" + ReadFromHdfsWriteToAerospikeSparkMapPartition.NUM_PARTITIONS + "=1",
                "-D" + ReadFromHdfsWriteToAerospikeSparkMapPartition.AEROSPIKE_NAMESPACE + "=" + runtimConfig.getNameSpaceName(),
                "-D" + ReadFromHdfsWriteToAerospikeSparkMapPartition.AEROSPIKE_HOSTNAME + "=127.0.0.1",
                "-D" + ReadFromHdfsWriteToAerospikeSparkMapPartition.AEROSPIKE_PORT + "=" + runtimConfig.getServicePort(),
                "-D" + ReadFromHdfsWriteToAerospikeSparkMapPartition.AEROSPIKE_SETNAME + "=" + setName,
                "-D" + ReadFromHdfsWriteToAerospikeSparkMapPartition.KEY_NAME + "=emp_id",
                "-D" + ReadFromHdfsWriteToAerospikeSparkMapPartition.VALUE_NAME + "=emp_name"};

        ReadFromHdfsWriteToAerospikeSparkMapPartition.main(args);

        ParquetReader<GenericRecord> reader = AvroParquetReader.builder(new Path(output)).build();
        //Use .withConf(FS.getConf()) for reading from a diferent HDFS and not local , by default the fs is local
        GenericData.Record event = null;
        while ((event = (GenericData.Record) reader.read()) != null) {
            Employee outputEvent = AvroUtils.convertByteArraytoAvroPojo(AvroUtils.convertAvroPOJOtoByteArray(event, Employee.getClassSchema
                    ()), Employee.getClassSchema());
            LOG.info("Data read from Sparkoutput is {}", outputEvent.toString());
            Assert.assertTrue(outputEvent.getEmpId().equals(1) || outputEvent.getEmpId().equals(2));
        }
        reader.close();

        //Fetch both the keys from Aerospike
        WritePolicy policy = new WritePolicy();
        Key key1 = new Key(runtimConfig.getNameSpaceName(), setName, 1);
        Record result1 = client.get(policy, key1);
        Assert.assertNotNull(result1);
        Assert.assertEquals(result1.getValue("emp_name").toString(), "Maverick1");

        Key key2 = new Key(runtimConfig.getNameSpaceName(), setName, 2);
        Record result2 = client.get(policy, key2);
        Assert.assertNotNull(result2);
        Assert.assertEquals(result2.getValue("emp_name").toString(), "Maverick2");

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
