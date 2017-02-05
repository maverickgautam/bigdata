package com.big.data.spark;

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
import java.io.IOException;

/**
 * Created by kunalgautam on 05.02.17.
 */
public class ReadWriteAvroParquetFilesTest {

    private static final Logger LOG = LoggerFactory.getLogger(ReadWriteAvroParquetFilesTest.class);
    private static final String BASEDIR = "/tmp/buidprofile/avroparquetInputFile/" + System.currentTimeMillis() + "/";
    private String input;
    private String output;

    private Employee employee;

    @Before
    public void setUp() throws IOException {

        input = BASEDIR + "input/";
        output = BASEDIR + "output/";

        employee = new Employee();
        employee.setEmpId(1);
        employee.setEmpName("Maverick");
        employee.setEmpCountry("DE");

        //Write parquet file with GZIP compression
        ParquetWriter<Object> writer = AvroParquetWriter.builder(new Path(input + "1.gz.parquet")).withCompressionCodec
                (CompressionCodecName.GZIP).withSchema(Employee.getClassSchema()).build();
        writer.write(employee);
        writer.close();

    }

    @Test
    public void testSuccess() throws Exception {

        String[] args = new String[]{"-D" + WordCount.INPUT_PATH + "=" + input,
                "-D" + WordCount.OUTPUT_PATH + "=" + output,
                "-D" + WordCount.IS_RUN_LOCALLY + "=true",
                "-D" + WordCount.DEFAULT_FS + "=file:///",
                "-D" + WordCount.NUM_PARTITIONS + "=1"};

        ReadWriteAvroParquetFiles.main(args);

        ParquetReader<GenericRecord> reader = AvroParquetReader.builder(new Path(output)).build();
        //Use .withConf(FS.getConf()) for reading from a diferent HDFS and not local , by default the fs is local

        GenericData.Record event = (GenericData.Record) reader.read();
        Employee outputEvent = AvroUtils.convertByteArraytoAvroPojo(AvroUtils.convertAvroPOJOtoByteArray(event, Employee.getClassSchema
                ()), Employee.getClassSchema());
        reader.close();
        LOG.info("Data read from Sparkoutput is {}", outputEvent.toString());
        Assert.assertEquals(employee.getEmpId(), outputEvent.getEmpId());
    }

    @After
    public void cleanup() throws IOException {
        FileUtils.deleteDirectory(new File(BASEDIR));
    }

}
