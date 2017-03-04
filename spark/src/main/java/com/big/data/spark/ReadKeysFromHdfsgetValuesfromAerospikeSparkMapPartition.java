package com.big.data.spark;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.WritePolicy;
import com.big.data.avro.schema.Employee;
import com.databricks.spark.avro.SchemaConverters;
import org.apache.avro.Schema;
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
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Created by kunalgautam on 01.03.17.
 */
public class ReadKeysFromHdfsgetValuesfromAerospikeSparkMapPartition extends Configured implements Tool, Closeable {

    public static final String INPUT_PATH = "spark.input.path";
    public static final String OUTPUT_PATH = "spark.output.path";
    public static final String IS_RUN_LOCALLY = "spark.is.run.local";
    public static final String DEFAULT_FS = "spark.default.fs";
    public static final String NUM_PARTITIONS = "spark.num.partitions";

    //Aerospike related properties
    public static final String AEROSPIKE_HOSTNAME = "aerospike.host.name";
    public static final String AEROSPIKE_PORT = "aerospike.port";
    public static final String AEROSPIKE_NAMESPACE = "aerospike.name.space";
    public static final String AEROSPIKE_SETNAME = "aerospike.set.name";

    // For Dem key is emp_id and value is emp_name
    public static final String KEY_NAME = "avro.key.name";
    public static final String VALUE_NAME = "avro.value.name";

    public static final String EMPLOYEE_COUNTRY_KEY_NAME = "emp_country";


    private SQLContext sqlContext;
    private JavaSparkContext javaSparkContext;

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

    @Override
    public int run(String[] args) throws Exception {

        //The arguments passed has been split into Key value by ToolRunner
        Configuration conf = getConf();
        String inputPath = conf.get(INPUT_PATH);
        String outputPath = conf.get(OUTPUT_PATH);
        String aerospikeHostname = conf.get(AEROSPIKE_HOSTNAME);
        int aerospikePort = conf.getInt(AEROSPIKE_PORT, 3000);
        String namespace = conf.get(AEROSPIKE_NAMESPACE);
        String setName = conf.get(AEROSPIKE_SETNAME);

        String valueName = conf.get(VALUE_NAME);

        //Get spark context, This is the central context , which can be wrapped in Any Other context
        javaSparkContext = getJavaSparkContext(conf.getBoolean(IS_RUN_LOCALLY, Boolean.FALSE), conf.get(DEFAULT_FS), ReadKeysFromHdfsgetValuesfromAerospikeSparkMapPartition.class);
        sqlContext = new SQLContext(javaSparkContext);

        // No input path has been read, no job has not been started yet .
        //To set any configuration use javaSparkContext.hadoopConfiguration().set(Key,value);
        // To set any custom inputformat use javaSparkContext.newAPIHadoopFile() and get a RDD

        // Avro schema to StructType conversion
        final StructType outPutSchemaStructType = (StructType) SchemaConverters.toSqlType(Employee.getClassSchema()).dataType();

        final StructType inputSchema = new StructType(new StructField[]{new StructField("emp_id", StringType, false, Metadata
                .empty())});

        // read data from parquetfile, the schema of the data is taken from the avro schema
        DataFrame inputDf = sqlContext.read().schema(inputSchema).text(inputPath);

        // convert DataFrame into JavaRDD
        // the rows read from the parquetfile is converted into a Row object . Row has same schema as that of the parquet file roe
        JavaRDD<Row> rowJavaRDD = inputDf.javaRDD();

        // Data read from parquet has same schema as that of avro (Empoyee Avro). Key is employeeId and value is EmployeeName
        // In the map there is no special function to initialize or shutdown the Aerospike client.
        JavaRDD<Row> returnedRowJavaRDD = rowJavaRDD.mapPartitions(new InsertIntoAerospike(aerospikeHostname, aerospikePort, namespace, setName,
                                                                                           "emp_id", valueName));

        //Map is just a transformation in Spark hence a action like write(), collect() is needed to carry out the action.
        // Remeber spark does Lazy evaluation, without a action transformations will not execute.

        DataFrame outputDf = sqlContext.createDataFrame(returnedRowJavaRDD, outPutSchemaStructType);

        // Convert JavaRDD to dataframe and save into parquet file
        outputDf
                .write()
                .format(Employee.class.getCanonicalName())
                .parquet(outputPath);

        return 0;
    }

    // Do remember all the lambda function are instantiated on driver, serialized and sent to driver.
    // No need to initialize the Service(Aerospike , Hbase on driver ) hence making it transiet
    // In the call() , for each record insert into Aerospike , this can be coverted into batch too
    public static class InsertIntoAerospike implements FlatMapFunction<Iterator<Row>, Row> {

        // not making it static , as it will not be serialized and sent to executors
        private final String aerospikeHostName;
        private final int aerospikePortNo;
        private final String aerospikeNamespace;
        private final String aerospikeSetName;
        private final String keyColumnName;
        private final String valueColumnName;

        // The Aerospike client is not serializable and neither there is a need to instatiate on driver
        private transient AerospikeClient client;
        private transient WritePolicy policy;

        public InsertIntoAerospike(String hostName, int portNo, String nameSpace, String setName, String keyColumnName, String valueColumnName) {
            this.aerospikeHostName = hostName;
            this.aerospikePortNo = portNo;
            this.aerospikeNamespace = nameSpace;
            this.aerospikeSetName = setName;
            this.keyColumnName = keyColumnName;
            this.valueColumnName = valueColumnName;
        }

        @Override
        public Iterable<Row> call(Iterator<Row> rowIterator) throws Exception {

            // rowIterator   points to the whole partition (all the records in the partition) , not a single record.
            // The partition can be of Map Task or Reduce Task
            // This is where you initialize your service , mapPartition resemble setup , map , cleanup of MapReduce,
            // Drawback of mapPartition is it returns after processing the whole chunk not after every row.

            // This will be held in memory till whole partition is evaluated
            List<Row> rowList = new ArrayList<>();

            //This is the place where you initialise your service
            policy = new WritePolicy();
            client = new AerospikeClient(aerospikeHostName, aerospikePortNo);

            // All the code before iterating over  rowIterator will be executed only once as the call() is called only once for each partition

            while (rowIterator.hasNext()) {

                Row row = rowIterator.next();

                String empID = (String) row.get(row.fieldIndex(keyColumnName));

                // As rows have schema with fieldName and Values being part of the Row
                Key key = new Key(aerospikeNamespace, aerospikeSetName, Integer.parseInt(empID));

                //Get from Aerospike  for a given employee id the employeeName
                Record result1 = client.get(policy, key);
                Employee employee = new Employee();
                employee.setEmpId(Integer.valueOf(empID));
                employee.setEmpName(result1.getValue(valueColumnName).toString());

                // This by default is available for Every key present in Aerospike
                employee.setEmpCountry(result1.getValue(EMPLOYEE_COUNTRY_KEY_NAME).toString());

                // creation of Employee object could have been skipped its just to make things clear
                // Convert Employee Avro To Object[]
                Object[] outputArray = new Object[Employee.getClassSchema().getFields().size()];
                for (Schema.Field field : Employee.getClassSchema().getFields()) {
                    outputArray[field.pos()] = employee.get(field.pos());

                }

                rowList.add(RowFactory.create(outputArray));

            }

            //Shutdown the service gracefully as this is called only once for the whole partition
            IOUtils.closeQuietly(client);

            return rowList;
        }
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(javaSparkContext);
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new ReadKeysFromHdfsgetValuesfromAerospikeSparkMapPartition(), args);
    }

}
