package com.big.data.avro.serde;

import com.big.data.avro.schema.Employee;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;

import java.util.HashMap;
import java.util.Map;

public class AvroSchemaToObjectConverter {

    private static String employeeSchemaString = "{\n" +
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
            "        },\n" +
            "        {\n" +
            "               \"name\": \"departments\",\n" +
            "               \"type\":[\"null\", {\"type\":\"array\", \"items\":\"string\" }],\n" +
            "               \"default\":null,\n" +
            "               \"doc\": \"Departments under the employee\"\n" +
            "         }\n" +
            "        ]\n" +
            " }";

    public void convertSchemaToClass() {

        Map<String, String> subordinateMap;

        subordinateMap = new HashMap<>();
        subordinateMap.put("maverick", "Junior SE");

        Schema employeeSchema = new org.apache.avro.Schema.Parser().parse(employeeSchemaString);

        // Form Generic Record.
        GenericRecord employeeRecord = new GenericData.Record(employeeSchema);

        //Insert Data into Generic Record
        employeeRecord.put("emp_id", 1L);
        employeeRecord.put("emp_name", "Mavrick");
        employeeRecord.put("emp_country", "NETHERLAND");
        employeeRecord.put("subordinates", subordinateMap);
        System.out.println("Generic Employee Record is : " + employeeRecord);
    }

    public void convertClassToSchema() {
        Class<Employee> clazz = SpecificData.get().getClass(Employee.getClassSchema());
        System.out.println("Class extracted from the schema is " + clazz);
    }

    public static void main(String[] args) {
        AvroSchemaToObjectConverter converter = new AvroSchemaToObjectConverter();
        converter.convertClassToSchema();
        converter.convertSchemaToClass();
    }
}
