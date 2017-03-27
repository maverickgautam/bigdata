package com.big.data.avro;

import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.junit.Assert;
import org.junit.Test;
import java.io.File;


public class AvroSchemaCompatibilityTest {


    @Test
    public void testCompatibility() throws Exception {


        File avroSchemaPath = new File("src/main/avro/Employee.avsc").getAbsoluteFile();

        // Parse the avsc file and form the Schema Object
        Schema newSchema = new Schema.Parser().parse(avroSchemaPath);

        // Old schema dosent have department array
        Schema oldSchema = new Schema.Parser().parse("{\n" +
                                                             "    \"name\": \"com.big.data.avro.schema.Employee\",\n" +
                                                             "    \"type\": \"record\",\n" +
                                                             "    \"fields\": [{\n" +
                                                             "            \"name\": \"emp_id\",\n" +
                                                             "            \"type\": \"int\",\n" +
                                                             "            \"doc\": \"employee Id of the employee\"\n" +
                                                             "        },\n" +
                                                             "        {\n" +
                                                             "             \"name\": \"emp_name\",\n" +
                                                             "             \"type\": \"string\",\n" +
                                                             "             \"doc\": \"employee name of the employee\"\n" +
                                                             "        },\n" +
                                                             "        {\n" +
                                                             "              \"name\": \"emp_country\",\n" +
                                                             "              \"type\": \"string\",\n" +
                                                             "              \"doc\": \"country of residence\"\n" +
                                                             "        },\n" +
                                                             "        {      \"name\": \"bonus\",\n" +
                                                             "               \"type\": [\"null\", \"long\"],\n" +
                                                             "               \"default\": null,\n" +
                                                             "               \"doc\": \"bonus received on a yearly basis\"\n" +
                                                             "        },\n" +
                                                             "       {\n" +
                                                             "               \"name\": \"subordinates\",\n" +
                                                             "               \"type\": [\"null\", {\"type\": \"map\", \"values\": \"string\"}],\n" +
                                                             "               \"default\": null,\n" +
                                                             "               \"doc\": \"map of subordinates Name and Designation\"\n" +
                                                             "        }\n" +
                                                             "        ]\n" +
                                                             " }");

        // Test compatibility of newSchema as the reader schema, old schema as the writer schema
        SchemaCompatibility.SchemaPairCompatibility compatResult = SchemaCompatibility.checkReaderWriterCompatibility(newSchema, oldSchema);
        Assert.assertTrue(SchemaCompatibility.schemaNameEquals(newSchema,oldSchema));
        Assert.assertNotNull(compatResult);
        Assert.assertEquals(SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE, compatResult.getType());
    }


}
