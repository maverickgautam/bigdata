package com.big.data.spark.converter;

import com.big.data.avro.schema.Employee;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by kunalgautam on 15.02.17.
 */
public class AvroToRowConverterTest {

    private static final Schema AVRO_SCHEMA = SchemaBuilder
            .record("customRecord").namespace("com.big.data.spark.converter.avro")
            .fields()
            .name("int_field").type().intType().noDefault()
            .name("str_field").type().stringType().noDefault()
            .name("arr_str_field").type().array().items().stringType().noDefault()
            .name("map_str_field").type().map().values().stringType().noDefault()
            .endRecord();

    @Test
    public void avroToRowTest() throws Exception {
        // Avro Objects Extends GenericRecord
        Employee employee = new Employee();
        Row row =  AvroToRowConverter.avroToRowConverter(employee);
        Assert.assertEquals(row.get(0),employee.get(0));

    }

    @Test
    public void testEmptyRowMapping() throws Exception {
        final Row row = AvroToRowConverter.avroToRowConverter(new GenericData.Record(AVRO_SCHEMA));
        Assert.assertTrue(row.size() == AVRO_SCHEMA.getFields().size());
    }

    @Test
    public void testArrayRowMapping() throws Exception {
        final GenericRecord record = new GenericData.Record(AVRO_SCHEMA);
        final String[] testStrArr = {"test_val1", "test_val2"};
        record.put("arr_str_field", testStrArr);
        final Row row = AvroToRowConverter.avroToRowConverter(record);
        Assert.assertTrue(row.size() == AVRO_SCHEMA.getFields().size());
    }



}
