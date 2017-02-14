package com.big.data.spark;

import com.big.data.avro.schema.Employee;
import com.databricks.spark.avro.SchemaConverters;
import org.apache.avro.AvroRuntimeException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

/**
 * Created by kunalgautam on 14.02.17.
 */
public class EmployeeMaxSalary extends Configured implements Tool, Closeable, Serializable {

    public static final String INPUT_PATH = "spark.input.path";
    public static final String OUTPUT_PATH = "spark.output.path";
    public static final String IS_RUN_LOCALLY = "spark.is.run.local";
    public static final String DEFAULT_FS = "spark.default.fs";
    public static final String NUM_PARTITIONS = "spark.num.partitions";

    // Just check because of a function use , the outer class is forced to be serialized
    // Example which throws light of serialization of Lambda function .
    private transient SQLContext sqlContext;
    private transient JavaSparkContext javaSparkContext;

    protected <T> JavaSparkContext getJavaSparkContext(final boolean isRunLocal,
                                                       final String defaultFs,
                                                       final Class<T> tClass) {
        final SparkConf sparkConf = new SparkConf()
                //Set spark conf here , after one gets spark context you can set hadoop configuration for InputFormats
                .setAppName(tClass.getSimpleName())
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        if (isRunLocal) {
            sparkConf.setMaster("local[*]");
        }

        final JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        if (defaultFs != null) {
            sparkContext.hadoopConfiguration().set("fs.defaultFS", defaultFs);
        }

        return sparkContext;
    }

    // Convert Row to Avro POJO (Employee)
    public Employee convert(Row row) {
        try {
            // Employee Schema => ParquetRow Schema =>Row Schema

            Employee avroInstance = new Employee();

            for (StructField field : row.schema().fields()) {

                //row.fieldIndex => pos of the field Name in the schema
                avroInstance.put(field.name(), row.get(row.fieldIndex(field.name())));

            }

            return avroInstance;

        } catch (Exception e) {
            throw new AvroRuntimeException("Avro POJO  building failed ", e);
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        //The arguments passed has been split into Key value by ToolRunner
        Configuration conf = getConf();
        String inputPath = conf.get(INPUT_PATH);

        String outputPath = conf.get(OUTPUT_PATH);

        //Get spark context, This is the central context , which can be wrapped in Any Other context
        javaSparkContext = getJavaSparkContext(conf.getBoolean(IS_RUN_LOCALLY, Boolean.FALSE), conf.get(DEFAULT_FS), MapSideJoin.class);
        sqlContext = new SQLContext(javaSparkContext);

        // No input path has been read, no job has not been started yet .
        //To set any configuration use javaSparkContext.hadoopConfiguration().set(Key,value);
        // To set any custom inputformat use javaSparkContext.newAPIHadoopFile() and get a RDD

        // Avro schema to StructType conversion
        final StructType outPutSchemaStructType = (StructType) SchemaConverters.toSqlType(Employee.getClassSchema()).dataType();

        // read data from parquetfile, the schema of the data is taken from the avro schema
        DataFrame inputDf = sqlContext.read().format(Employee.class.getCanonicalName()).parquet(inputPath);

        // convert DataFrame into JavaRDD
        // the rows read from the parquetfile is converted into a Row object . Row has same schema as that of the parquet file row
        JavaRDD<Row> rowJavaRDD = inputDf.javaRDD();

        //Row has same schema as that of Parquet row , Parquet Row has same schema as that of Avro Object

        rowJavaRDD
                // convert each Row to Employee Object

                // if i use a method call e -> convert(e) instead of static class, i will need to serialize the Outer class
                // Lambda Functions internall needs to be serialized and is causing this issue

                .map(e -> convert(e))

                // Key by empid so that we can collect all the object on Reducer
                .keyBy(Employee::getEmpId)

                .combineByKey(new CreateCombiner(), new MergeValue(), new MergeCombiner())

                .map(new MapSpark());

        DataFrame outputDf = sqlContext.createDataFrame(rowJavaRDD, outPutSchemaStructType);

        // Convert JavaRDD to dataframe and save into parquet file
        outputDf
                .write()
                .format(Employee.class.getCanonicalName())
                .parquet(outputPath);

        return 0;
    }

    public static class MapSpark implements Function<Tuple2<Integer, Object>, Object> {

        // Do Realize all the classes , LambdaFuncation used inside the Transformation are instantiated on Driver .
        // The Serialized object is sent to the executor
        // Making a filed transient helps in not serializing it
        private transient TreeSet<Long> employeeBonusSet;

        // Please do not declare any field with static , as Multiple task can spawn inside same JVM(Executor) is Spark

        @Override
        public Object call(Tuple2<Integer, Object> v1) throws Exception {

            if (employeeBonusSet == null) {
                employeeBonusSet = new TreeSet<>();
            }

            // Object is being reused and not created on every call
            employeeBonusSet.clear();

            EmployeeAgregator aggregatedEvents = (EmployeeAgregator) v1._2();

            aggregatedEvents.getEmployeeList().stream().forEach(o -> employeeBonusSet.add(((Employee) o).getBonus()));

            // select one object from the Employee List
            Object output = aggregatedEvents.getEmployeeList().get(0);

            ((Employee) output).setBonus(employeeBonusSet.last());

            return output;
        }
    }

    public static class EmployeeAgregator {

        private List<Object> employeeList;

        public EmployeeAgregator() {
            employeeList = new ArrayList<>();
        }

        public List<Object> getEmployeeList() {
            return employeeList;
        }

        public void addEmployee(Employee emp) {
            employeeList.add(emp);
        }

        public void addEmployeeAgregator(EmployeeAgregator aggregator) {
            employeeList.addAll(aggregator.getEmployeeList());
        }

    }

    // This class would be  instantiated on MapTask for Every Employee Group(for a group and not individual input Row).
    // Only for first Row in the Group it would be instantiated
    public static class CreateCombiner implements Function<Employee, Object> {

        @Override
        public Object call(Employee v1) throws Exception {
            EmployeeAgregator aggregator = new EmployeeAgregator();
            aggregator.addEmployee(v1);

            return aggregator;
        }
    }

    // Any Subsequent input With same employeeId will be added into the EmployeeAgregator here
    // This is like combiner from MapReduce
    public static class MergeValue implements Function2<Object, Employee, Object> {

        @Override
        public Object call(Object v1, Employee v2) throws Exception {

            // Type of v1 is EmployeeAgregator  , Whereas Type of v2 is Employee
            ((EmployeeAgregator) v1).addEmployee(v2);
            return v1;
        }
    }

    // This will Be executed on Reduce Task .  All EmployeeAgregator will be coming from Mapper and just merged on Reducer
    public static class MergeCombiner implements Function2<Object, Object, Object> {

        @Override
        public Object call(Object v1, Object v2) throws Exception {

            //Type of both v1 and v2 are  EmployeeAgregator
            ((EmployeeAgregator) v1).addEmployeeAgregator((EmployeeAgregator) v2);

            return v1;
        }
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(javaSparkContext);
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new EmployeeMaxSalary(), args);
    }

}
