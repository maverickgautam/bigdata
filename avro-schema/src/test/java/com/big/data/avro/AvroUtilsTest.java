package com.big.data.avro;

import com.big.data.avro.schema.Employee;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class AvroUtilsTest {

    private Employee employee;

    @Before
    public void formPojo() {
        employee = new Employee();
        employee.setEmpId(1);
        employee.setEmpName("Maverick");
        employee.setEmpCountry("DE");
    }

    @Test
    public void serDeAvroToFileTest() throws Exception {

        String path = System.currentTimeMillis() + "Eventest.avro";
        File inputPath = new File(path);
        inputPath.deleteOnExit();
        List<Employee> employeeList = new ArrayList<>();
        employeeList.add(employee);
        AvroUtils.writeAvroFile(inputPath, Employee.class, employeeList);
        List<GenericRecord> outputList = AvroUtils.readAvroFile(inputPath, Employee.getClassSchema());
        Assert.assertNotNull(outputList.get(0));
        Assert.assertEquals(employee.toString(), outputList.get(0).toString());

    }

    @Test
    public void serDeAvroPOJOtoByteArrayTest() throws Exception {

        byte[] serialized = AvroUtils.convertAvroPOJOtoByteArray(employee, Employee.getClassSchema());
        Assert.assertNotNull(serialized);
        Employee deseriazedPojo = AvroUtils.convertByteArraytoAvroPojo(serialized, Employee.getClassSchema());
        Assert.assertEquals(employee.getEmpId(), deseriazedPojo.getEmpId());
        Assert.assertEquals(employee.getEmpName(), deseriazedPojo.getEmpName());
        Assert.assertEquals(employee.getEmpCountry(), deseriazedPojo.getEmpCountry());
        Assert.assertTrue(employee.equals(deseriazedPojo));
    }

    @Test
    public void serDeAvroPOJOtoJsonTest() throws Exception {

        byte[] serialized = AvroUtils.jsonToAvro(employee.toString(), Employee.getClassSchema());
        Assert.assertNotNull(serialized);
        Employee deseriazedPojo = AvroUtils.convertByteArraytoAvroPojo(serialized, Employee.getClassSchema());
        Assert.assertEquals(employee.getEmpId(), deseriazedPojo.getEmpId());
        Assert.assertEquals(employee.getEmpName(), deseriazedPojo.getEmpName());
        Assert.assertEquals(employee.getEmpCountry(), deseriazedPojo.getEmpCountry());

        String eventPojoJson = AvroUtils.avroToJson(serialized, Employee.getClassSchema());
        Employee deseriazedEventPojo = AvroUtils.convertByteArraytoAvroPojo(AvroUtils.jsonToAvro(eventPojoJson, Employee.getClassSchema()), Employee
                .getClassSchema
                        ());
        Assert.assertNotNull(deseriazedEventPojo);
        Assert.assertTrue(employee.equals(deseriazedEventPojo));

    }
}
