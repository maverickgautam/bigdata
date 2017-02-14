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
import org.apache.spark.api.java.function.FlatMapFunction;
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
import java.util.Iterator;
import java.util.TreeSet;

/**
 * Created by kunalgautam on 14.02.17.
 */
public class EmployeeMaxBonusMapPartition extends Configured implements Tool, Closeable, Serializable {

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

                .groupByKey()

                //.combineByKey(new EmployeeMaxSalary.CreateCombiner(), new EmployeeMaxSalary.MergeValue(), new EmployeeMaxSalary.MergeCombiner())

                .mapPartitions(new MapPartitionSpark());

        DataFrame outputDf = sqlContext.createDataFrame(rowJavaRDD, outPutSchemaStructType);

        // Convert JavaRDD to dataframe and save into parquet file
        outputDf
                .write()
                .format(Employee.class.getCanonicalName())
                .parquet(outputPath);

        return 0;
    }

    public static class MapPartitionSpark implements FlatMapFunction<Iterator<Tuple2<Integer, Iterable<Employee>>>, Object> {

         // Do Realize all the classes , LambdaFuncation used inside the Transformation are instantiated on Driver .
        // The Serialized object is sent to the executor

        // Making a filed transient helps in not serializing it
        private transient TreeSet<Long> employeeBonusSet;

        // Please do not declare any field with static , as Multiple task can spawn inside same JVM(Executor) is Spark, leading to Thread unsafe code


        // the call() method is called only once for each partition( partition from Map or Reduce Task)
        @Override
        public Iterable<Object> call(Iterator<Tuple2<Integer, Iterable<Employee>>> tuple2Iterator) throws Exception {

            // tuple2Iterator   points to the whole partition (all the records in the partition) , not a single record.
            // Tuple2<Integer, Iterable<Employee>> for a given key( id => Integer) , the group of values (Iterable<Employee>) is being pointed to
            // The partition can be of Map Task or Reduce Tasl

            // All the code before iterating over  tuple2Iterator will be executed only once as the call() is called only once for each partition

            employeeBonusSet = new TreeSet<>();

            // Any service like Hbase clientSetup, Aerospike setup can be instantiate here

            ArrayList<Object> outputOfMapPartition = new ArrayList<>();

            // the while loop points to the loop over all the keys in the partiton
            while (tuple2Iterator.hasNext()) {

                // this points to one of the key in the partiotn and the values associated  with it
                Tuple2<Integer, Iterable<Employee>> integerIterableTuple2 = tuple2Iterator.next();

                // Object is being reused and not created on every call
                employeeBonusSet.clear();

                for (Employee employee : integerIterableTuple2._2()) {

                    employeeBonusSet.add(employee.getBonus());

                }

                // Just select one employee and add the Max Bonus to it
                Employee output = integerIterableTuple2._2().iterator().next();
                output.setBonus(employeeBonusSet.last());
                outputOfMapPartition.add(output);

            }

            // the service can be shutdown here in the code.
            // You return only once from the MapPartion when the  data for the whole partition has been processed
            // Unlike map function you dont return once per row , rather you return once per partition
            return outputOfMapPartition;
        }
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(javaSparkContext);
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new EmployeeMaxBonusMapPartition(), args);
    }

}
