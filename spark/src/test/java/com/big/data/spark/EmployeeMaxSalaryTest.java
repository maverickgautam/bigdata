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
 * Created by kunalgautam on 14.02.17.
 */
public class EmployeeMaxSalaryTest {

    private static final Logger LOG = LoggerFactory.getLogger(EmployeeMaxSalaryTest.class);
    private static final String BASEDIR = "/tmp/EmployeeMaxSalaryTest/avroparquetInputFile/" + System.currentTimeMillis() + "/";
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
        employee.setBonus(100L);

        Employee employee2 = new Employee();
        employee2.setEmpId(1);
        employee2.setEmpName("Maverick");
        employee2.setEmpCountry("DE");
        employee2.setBonus(90L);

        //Write parquet file with GZIP compression
        ParquetWriter<Object> writer = AvroParquetWriter.builder(new Path(input + "1.gz.parquet")).withCompressionCodec
                (CompressionCodecName.GZIP).withSchema(Employee.getClassSchema()).build();
        writer.write(employee);
        writer.write(employee2);
        writer.close();
    }

    @Test
    public void testSuccess() throws Exception {

        String[] args = new String[]{"-D" + EmployeeMaxSalary.INPUT_PATH + "=" + input,
                "-D" + EmployeeMaxSalary.OUTPUT_PATH + "=" + output,
                "-D" + EmployeeMaxSalary.IS_RUN_LOCALLY + "=true",
                "-D" + EmployeeMaxSalary.DEFAULT_FS + "=file:///",
                "-D" + EmployeeMaxSalary.NUM_PARTITIONS + "=1"};

        EmployeeMaxSalary.main(args);

        ParquetReader<GenericRecord> reader = AvroParquetReader.builder(new Path(output)).build();
        //Use .withConf(FS.getConf()) for reading from a diferent HDFS and not local , by default the fs is local

        GenericData.Record event = (GenericData.Record) reader.read();
        Employee outputEvent = AvroUtils.convertByteArraytoAvroPojo(AvroUtils.convertAvroPOJOtoByteArray(event, Employee.getClassSchema
                ()), Employee.getClassSchema());
        reader.close();
        LOG.info("Data read from Sparkoutput is {}", outputEvent.toString());
        Assert.assertEquals(employee.getEmpId(), outputEvent.getEmpId());
        Assert.assertEquals(100L, outputEvent.getBonus().longValue());
    }

    @After
    public void cleanup() throws IOException {
        FileUtils.deleteDirectory(new File(BASEDIR));
    }

}
