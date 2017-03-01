package com.big.data.spark;

import com.big.data.avro.AvroUtils;
import com.big.data.avro.schema.Employee;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.json.simple.JSONObject;
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
import java.util.HashMap;
import java.util.Map;

/**
 * Created by kunalgautam on 06.02.17.
 */
public class DataFrameBroadcastJoinTest {

    private static final Logger LOG = LoggerFactory.getLogger(ReadWriteAvroParquetFilesTest.class);
    private static final String BASEDIR = "/tmp/DataFrameBroadcastJoinTest/avroparquetInputFile/" + System.currentTimeMillis() + "/";
    private String parquetInput;
    private String jsonInput;
    private String output;

    private Employee employee;

    @Before
    public void inputDataPreperation() throws IOException {

        parquetInput = BASEDIR + "parquetInput/";
        jsonInput = BASEDIR + "jsonInput";
        output = BASEDIR + "output/";


        // WRITE parquet file
        employee = new Employee();
        employee.setEmpId(1);
        employee.setEmpName("Maverick");
        employee.setEmpCountry("DE");

        //Write parquet file with GZIP compression
        ParquetWriter<Object> writer = AvroParquetWriter.builder(new Path(parquetInput + "1.gz.parquet")).
                withCompressionCodec(CompressionCodecName.GZIP).withSchema(Employee.getClassSchema()).build();
        writer.write(employee);
        writer.close();


        //Write the data into the local filesystem  for Left input
        File tempFileleft = new File(jsonInput + "/input.txt");
        Map<String,Object> jsonMap = new HashMap<>();
        jsonMap.put("country","DE");
        jsonMap.put("lang","german");
        JSONObject obj = new JSONObject(jsonMap);

        FileUtils.writeStringToFile(tempFileleft, obj.toJSONString(), "UTF-8");
        //FileUtils.writeStringToFile(tempFileleft, DataFrameJoin.NEW_LINE_DELIMETER, "UTF-8", true);
    }

    @Test
    public void testSuccess() throws Exception {

        String[] args = new String[]{"-D" + DataFrameJoin.PARQUET_INPUT_PATH + "=" + parquetInput,
                "-D" + DataFrameJoin.JSON_INPUT_PATH + "=" + jsonInput,
                "-D" + DataFrameJoin.OUTPUT_PATH + "=" + output,
                "-D" + DataFrameJoin.IS_RUN_LOCALLY + "=true",
                "-D" + DataFrameJoin.DEFAULT_FS + "=file:///",
                "-D" + DataFrameJoin.NUM_PARTITIONS + "=1"};

        DataFrameJoin.main(args);

        ParquetReader<GenericRecord> reader = AvroParquetReader.builder(new Path(output)).build();
        //Use .withConf(FS.getConf()) for reading from a diferent HDFS and not local , by default the fs is local

        GenericData.Record event = (GenericData.Record) reader.read();
        Employee outputEvent = AvroUtils.convertByteArraytoAvroPojo(AvroUtils.convertAvroPOJOtoByteArray(event, Employee.getClassSchema()), Employee.getClassSchema());
        reader.close();
        LOG.info("Data read from Sparkoutput is {}", outputEvent.toString());
        Assert.assertEquals(employee.getEmpId(), outputEvent.getEmpId());
    }

    @After
    public void cleanup() throws IOException {
        FileUtils.deleteDirectory(new File(BASEDIR));
    }
}
