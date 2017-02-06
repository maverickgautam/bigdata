package com.big.data.spark;

import com.big.data.avro.schema.Employee;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parquet.avro.AvroParquetWriter;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;

import java.io.File;
import java.io.IOException;

/**
 * Created by kunalgautam on 06.02.17.
 */
public class RDDtoCSVTest {
    private static final Logger LOG = LoggerFactory.getLogger(ReadWriteAvroParquetFilesTest.class);
    private static final String BASEDIR = "/tmp/RDDtoCSVTest/avroparquetInputFile/" + System.currentTimeMillis() + "/";
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

        String[] args = new String[]{"-D" + RDDtoCSV.INPUT_PATH + "=" + input,
                "-D" + RDDtoCSV.OUTPUT_PATH + "=" + output,
                "-D" + RDDtoCSV.IS_RUN_LOCALLY + "=true",
                "-D" + RDDtoCSV.DEFAULT_FS + "=file:///",
                "-D" + RDDtoCSV.NUM_PARTITIONS + "=1"};

        RDDtoCSV.main(args);

        File outputFile = new File(output + "part-00000");
        String fileToString = FileUtils.readFileToString(outputFile, "UTF-8");
        System.out.println(fileToString);

        //header + 1 line
        Assert.assertEquals(2, fileToString.split(RDDtoCSV.NEW_LINE_DELIMETER).length);
    }

    @After
    public void cleanup() throws IOException {
        FileUtils.deleteDirectory(new File(BASEDIR));
    }

}
