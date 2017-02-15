package com.big.data.spark.converter;

import com.databricks.spark.avro.SchemaConverters;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;

/**
 * Created by kunalgautam on 15.02.17.
 */
public class AvroToRowConverter {

    private AvroToRowConverter() {
        // do nothing
    }

    public static <A extends GenericRecord> Row avroToRowConverter(A avrorecord) {

        if (null == avrorecord) {
            return null;
        }

        Object[] objectArray = new Object[avrorecord.getSchema().getFields().size()];
        StructType structType = (StructType) SchemaConverters.toSqlType(avrorecord.getSchema()).dataType();

        for (Schema.Field field : avrorecord.getSchema().getFields()) {
            objectArray[field.pos()] = avrorecord.get(field.pos());
        }

        return new GenericRowWithSchema(objectArray, structType);
    }
}


